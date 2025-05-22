#include "source/extensions/filters/network/redis_proxy/conn_pool_impl.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "envoy/config/core/v3/base.pb.h"
#include "envoy/config/core/v3/health_check.pb.h"
#include "envoy/config/endpoint/v3/endpoint_components.pb.h"
#include "envoy/extensions/filters/network/redis_proxy/v3/redis_proxy.pb.h"
#include "envoy/extensions/filters/network/redis_proxy/v3/redis_proxy.pb.validate.h"

#include "source/common/common/assert.h"
#include "source/common/common/logger.h"
#include "source/common/stats/utility.h"
#include "source/extensions/filters/network/redis_proxy/config.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RedisProxy {
namespace ConnPool {
namespace {
// null_pool_callbacks is used for requests that must be filtered and not redirected such as
// "asking".
Common::Redis::Client::DoNothingPoolCallbacks null_client_callbacks;

const Common::Redis::RespValue& getRequest(const RespVariant& request) {
  if (request.index() == 0) {
    return absl::get<const Common::Redis::RespValue>(request);
  } else {
    return *(absl::get<Common::Redis::RespValueConstSharedPtr>(request));
  }
}

static uint16_t default_port = 6379;

} // namespace

InstanceImpl::InstanceImpl(
    const std::string& cluster_name, Upstream::ClusterManager& cm,
    Common::Redis::Client::ClientFactory& client_factory, ThreadLocal::SlotAllocator& tls,
    const envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings&
        config,
    Api::Api& api, Stats::ScopeSharedPtr&& stats_scope,
    const Common::Redis::RedisCommandStatsSharedPtr& redis_command_stats,
    Extensions::Common::Redis::ClusterRefreshManagerSharedPtr refresh_manager,
    const Extensions::Common::DynamicForwardProxy::DnsCacheSharedPtr& dns_cache)
    : cluster_name_(cluster_name), cm_(cm), client_factory_(client_factory),
      tls_(tls.allocateSlot()), config_(new Common::Redis::Client::ConfigImpl(config)), api_(api),
      stats_scope_(std::move(stats_scope)),
      redis_command_stats_(redis_command_stats), redis_cluster_stats_{REDIS_CLUSTER_STATS(
                                                     POOL_COUNTER(*stats_scope_))},
      refresh_manager_(std::move(refresh_manager)), dns_cache_(dns_cache) {

  const auto& cfg = MessageUtil::downcastAndValidate<
      const envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings&>(
      config->configProto(), البروتوكولاتValidationVisitor());

  if (cfg.has_iam_auth()) {
    iam_auth_enabled_ = true;
    redis_user_ = cfg.iam_auth().redis_user();
    cache_name_ = cfg.iam_auth().cache_name(); // This might be host or a logical name.
    ENVOY_LOG(info, "Redis IAM Auth enabled for user '{}' on cache '{}'", redis_user_, cache_name_);

    Aws::InitAPI(aws_sdk_options_);
    aws_credentials_provider_ =
        Aws::Auth::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>("redis-iam-auth");
    aws_client_config_ = Aws::MakeShared<Aws::Client::ClientConfiguration>("redis-iam-auth");

    const char* env_region = std::getenv("AWS_REGION");
    if (env_region) {
      aws_client_config_->region = env_region;
    } else {
      // Basic parsing for region from FQDN like *.region.cache.amazonaws.com
      // This is a simplification. A more robust FQDN parser might be needed.
      // Example: mycluster.xxxxxx.us-east-1.cache.amazonaws.com
      // Example: my-primary.xxxxxx.clustercfg.usw2.cache.amazonaws.com
      std::vector<std::string> parts = absl::StrSplit(cache_name_, '.');
      bool parsed_region = false;
      if (parts.size() >= 4) {
        if (parts[parts.size()-3] == "cache" && parts[parts.size()-2] == "amazonaws" && parts[parts.size()-1] == "com") {
           // Check for *.region.cache.amazonaws.com
           if (parts.size() >= 5) { // mycluster.nodeid.region.cache.amazonaws.com
             aws_client_config_->region = parts[parts.size()-4];
             parsed_region = true;
           }
        } else if (parts[parts.size()-4] == "clustercfg" && parts.size() >= 6) {
            // my-primary.xxxxxx.clustercfg.usw2.cache.amazonaws.com -> usw2
            aws_client_config_->region = parts[parts.size()-3];
            parsed_region = true;
        }
      }

      if (!parsed_region) {
        ENVOY_LOG(warn, "AWS region not found in environment or could not parse from cache_name FQDN ('{}') for IAM auth. Defaulting to us-east-1. Please configure AWS_REGION or ensure cache_name is a regional FQDN.", cache_name_);
        aws_client_config_->region = "us-east-1"; // Default if not found
      }
    }
    ENVOY_LOG(info, "Using AWS region: {} for IAM Authentication", aws_client_config_->region);
  }
}

InstanceImpl::~InstanceImpl() {
  if (iam_auth_enabled_) {
    Aws::ShutdownAPI(aws_sdk_options_);
  }
}

void InstanceImpl::init() {
  // Note: `this` and `cluster_name` have a a lifetime of the filter.
  // That may be shorter than the tls callback if the listener is torn down shortly after it is
  // created. We use a weak pointer to make sure this object outlives the tls callbacks.
  std::weak_ptr<InstanceImpl> this_weak_ptr = this->shared_from_this();
  tls_->set([this_weak_ptr](
                Event::Dispatcher& dispatcher) -> ThreadLocal::ThreadLocalObjectSharedPtr {
    if (auto this_shared_ptr = this_weak_ptr.lock()) {
      return std::make_shared<ThreadLocalPool>(
          this_shared_ptr, dispatcher, this_shared_ptr->cluster_name_, this_shared_ptr->dns_cache_);
    }
    return nullptr;
  });
}

uint16_t InstanceImpl::shardSize() { return tls_->getTyped<ThreadLocalPool>().shardSize(); }

// This method is always called from a InstanceSharedPtr we don't have to worry about tls_->getTyped
// failing due to InstanceImpl going away.
Common::Redis::Client::PoolRequest*
InstanceImpl::makeRequest(const std::string& key, RespVariant&& request, PoolCallbacks& callbacks,
                          Common::Redis::Client::Transaction& transaction) {
  return tls_->getTyped<ThreadLocalPool>().makeRequest(key, std::move(request), callbacks,
                                                       transaction);
}

// This method is always called from a InstanceSharedPtr we don't have to worry about tls_->getTyped
// failing due to InstanceImpl going away.
Common::Redis::Client::PoolRequest*
InstanceImpl::makeRequestToHost(const std::string& host_address,
                                const Common::Redis::RespValue& request,
                                Common::Redis::Client::ClientCallbacks& callbacks) {
  return tls_->getTyped<ThreadLocalPool>().makeRequestToHost(host_address, request, callbacks);
}

// This method is always called from a InstanceSharedPtr we don't have to worry about tls_->getTyped
// failing due to InstanceImpl going away.
Common::Redis::Client::PoolRequest*
InstanceImpl::makeRequestToShard(uint16_t shard_index, RespVariant&& request,
                                 PoolCallbacks& callbacks,
                                 Common::Redis::Client::Transaction& transaction) {
  return tls_->getTyped<ThreadLocalPool>().makeRequestToShard(shard_index, std::move(request),
                                                              callbacks, transaction);
}

std::string InstanceImpl::generateIAMAuthToken(const std::string& redis_user_param,
                                               const std::string& hostname_param,
                                               const std::string& region_param) {
  if (!iam_auth_enabled_ || !aws_credentials_provider_ || !aws_client_config_) {
    ENVOY_LOG(error, "IAM auth not properly initialized for token generation. IAM_enabled: {}, cred_provider: {}, client_config: {}",
              iam_auth_enabled_, aws_credentials_provider_ != nullptr, aws_client_config_ != nullptr);
    return "";
  }

  // ElastiCache IAM auth requires the hostname (cluster endpoint) without the port for signing.
  std::string service_host = hostname_param;
  size_t colon_pos = hostname_param.rfind(':');
  if (colon_pos != std::string::npos) {
    service_host = hostname_param.substr(0, colon_pos);
  }

  // The URI for presigning is typically like:
  // https://<service_host>/?Action=connect&User=<redis_user_param>
  // The AWS SDK's signer will handle the construction of the canonical request.
  Aws::Http::URI uri;
  uri.SetScheme(Aws::Http::Scheme::HTTPS); // SigV4 typically assumes HTTPS for the signing endpoint
  uri.SetAuthority(service_host);
  uri.SetPath("/"); // Path is usually just "/"
  uri.AddQueryStringParameter("Action", "connect");
  uri.AddQueryStringParameter("User", redis_user_param);
  
  ENVOY_LOG(debug, "Generating IAM token for URI: {}", uri.GetURIString());

  // Using a GET request for signing purposes, as is common for presigned URL generation.
  auto http_request = Aws::Http::CreateHttpRequest(uri, Aws::Http::HttpMethod::HTTP_GET, Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
  // The Host header must match the authority used in the URI for signing.
  http_request->SetHeaderValue(Aws::Http::HOST_HEADER, service_host);

  auto signer = Aws::MakeShared<Aws::Auth::AWSAuthV4Signer>(
      "redis-iam-signer", // Alloc tag
      aws_credentials_provider_,
      "redis", // Service name for ElastiCache IAM
      region_param.empty() ? aws_client_config_->region.c_str() : region_param.c_str(), // Region
      Aws::Auth::AWSSigningAlgorithm::SIGV4,
      false // uriEscapePath - false because Redis expects path as is.
  );
  
  // Sign the request to generate the presigned URL parameters in the query string
  if (signer->PresignRequest(*http_request, region_param.empty() ? aws_client_config_->region.c_str() : region_param.c_str(), "redis", 60*15 /*expiresInSeconds = 15 minutes, which is max for IAM user creds*/)) {
    // The token for Redis AUTH is the full query string from the presigned URL, without the leading '?'
    std::string query_string = http_request->GetURI().GetQueryString();
    if (!query_string.empty() && query_string[0] == '?') {
      ENVOY_LOG(debug, "Successfully generated IAM auth token for user {}", redis_user_param);
      return query_string.substr(1);
    }
    ENVOY_LOG(debug, "Generated IAM auth token (raw query string, no leading '?'): {} for user {}", query_string, redis_user_param);
    return query_string; // Should start with Action=...
  } else {
    ENVOY_LOG(error, "Failed to sign IAM auth request for user {}. Host: {}, Region: {}", redis_user_param, service_host, region_param.empty() ? aws_client_config_->region : region_param);
    return "";
  }
}

InstanceImpl::ThreadLocalPool::ThreadLocalPool(
    std::shared_ptr<InstanceImpl> parent, Event::Dispatcher& dispatcher, std::string cluster_name,
    const Extensions::Common::DynamicForwardProxy::DnsCacheSharedPtr& dns_cache)
    : parent_(parent), dispatcher_(dispatcher), cluster_name_(std::move(cluster_name)),
      dns_cache_(dns_cache),
      drain_timer_(dispatcher.createTimer([this]() -> void { drainClients(); })),
      client_factory_(parent->client_factory_), config_(parent->config_),
      stats_scope_(parent->stats_scope_), redis_command_stats_(parent->redis_command_stats_),
      redis_cluster_stats_(parent->redis_cluster_stats_),
      refresh_manager_(parent->refresh_manager_) {

  auto locked_parent = parent_.lock();
  if (locked_parent) {
      iam_auth_enabled_ = locked_parent->iam_auth_enabled_;
      if (iam_auth_enabled_) {
          redis_user_ = locked_parent->redis_user_;
          // cache_name_ is already part of InstanceImpl, TLP uses parent's cache_name_ if needed via parent_.lock()->cache_name_
          ENVOY_LOG(debug, "ThreadLocalPool IAM Auth enabled for user '{}'", redis_user_);
      }
  }

  cluster_update_handle_ = parent->cm_.addThreadLocalClusterUpdateCallbacks(*this);
  Upstream::ThreadLocalCluster* cluster = parent->cm_.getThreadLocalCluster(cluster_name_);
  if (cluster != nullptr) {
    Upstream::ThreadLocalClusterCommand command = [&cluster]() -> Upstream::ThreadLocalCluster& {
      return *cluster;
    };
    onClusterAddOrUpdateNonVirtual(cluster->info()->name(), command);
  }
}

InstanceImpl::ThreadLocalPool::~ThreadLocalPool() {
  while (!pending_requests_.empty()) {
    pending_requests_.pop_front();
  }
  while (!client_map_.empty()) {
    client_map_.begin()->second->redis_client_->close();
  }
  while (!clients_to_drain_.empty()) {
    (*clients_to_drain_.begin())->redis_client_->close();
  }
}

void InstanceImpl::ThreadLocalPool::onClusterAddOrUpdateNonVirtual(
    absl::string_view cluster_name, Upstream::ThreadLocalClusterCommand& get_cluster) {
  if (cluster_name != cluster_name_) {
    return;
  }
  // Ensure the filter is not deleted in the main thread during this method.
  auto shared_parent = parent_.lock();
  if (!shared_parent) {
    return;
  }

  if (cluster_ != nullptr) {
    // Treat an update as a removal followed by an add.
    ThreadLocalPool::onClusterRemoval(cluster_name_);
  }

  ASSERT(cluster_ == nullptr);
  auto& cluster = get_cluster();
  cluster_ = &cluster;
  // Update username and password when cluster updates.
  auth_username_ = ProtocolOptionsConfigImpl::authUsername(cluster_->info(), shared_parent->api_);
  auth_password_ = ProtocolOptionsConfigImpl::authPassword(cluster_->info(), shared_parent->api_);
  ASSERT(host_set_member_update_cb_handle_ == nullptr);
  host_set_member_update_cb_handle_ = cluster_->prioritySet().addMemberUpdateCb(
      [this](const std::vector<Upstream::HostSharedPtr>& hosts_added,
             const std::vector<Upstream::HostSharedPtr>& hosts_removed) -> absl::Status {
        onHostsAdded(hosts_added);
        onHostsRemoved(hosts_removed);
        return absl::OkStatus();
      });

  ASSERT(host_address_map_.empty());
  for (const auto& i : cluster_->prioritySet().hostSetsPerPriority()) {
    for (auto& host : i->hosts()) {
      host_address_map_[host->address()->asString()] = host;
    }
  }

  // Figure out if the cluster associated with this ConnPool is a Redis cluster
  // with its own hash slot sharding scheme and ability to dynamically discover
  // its members. This is done once to minimize overhead in the data path, makeRequest() in
  // particular.
  Upstream::ClusterInfoConstSharedPtr info = cluster_->info();
  OptRef<const envoy::config::cluster::v3::Cluster::CustomClusterType> cluster_type =
      info->clusterType();
  is_redis_cluster_ = cluster_type.has_value() && cluster_type->name() == "envoy.clusters.redis";
}

void InstanceImpl::ThreadLocalPool::onClusterRemoval(const std::string& cluster_name) {
  if (cluster_name != cluster_name_) {
    return;
  }

  // Treat cluster removal as a removal of all hosts. Close all connections and fail all pending
  // requests.
  host_set_member_update_cb_handle_ = nullptr;
  while (!client_map_.empty()) {
    client_map_.begin()->second->redis_client_->close();
  }
  while (!clients_to_drain_.empty()) {
    (*clients_to_drain_.begin())->redis_client_->close();
  }

  cluster_ = nullptr;
  host_address_map_.clear();
  cx_rate_limiter_map_.clear();
}

void InstanceImpl::ThreadLocalPool::onHostsAdded(
    const std::vector<Upstream::HostSharedPtr>& hosts_added) {
  for (const auto& host : hosts_added) {
    std::string host_address = host->address()->asString();
    // Insert new host into address map, possibly overwriting a previous host's entry.
    host_address_map_[host_address] = host;
    for (const auto& created_host : created_via_redirect_hosts_) {
      if (created_host->address()->asString() == host_address) {
        // Remove our "temporary" host created in makeRequestToHost().
        onHostsRemoved({created_host});
        created_via_redirect_hosts_.remove(created_host);
        break;
      }
    }
  }
}

void InstanceImpl::ThreadLocalPool::onHostsRemoved(
    const std::vector<Upstream::HostSharedPtr>& hosts_removed) {
  for (const auto& host : hosts_removed) {
    auto token_bucket = cx_rate_limiter_map_.find(host);
    if (token_bucket != cx_rate_limiter_map_.end()) {
      cx_rate_limiter_map_.erase(token_bucket);
    }
    auto it = client_map_.find(host);
    if (it != client_map_.end()) {
      if (it->second->redis_client_->active()) {
        // Put the ThreadLocalActiveClient to the side to drain.
        clients_to_drain_.push_back(std::move(it->second));
        client_map_.erase(it);
        if (!drain_timer_->enabled()) {
          drain_timer_->enableTimer(std::chrono::seconds(1));
        }
      } else {
        // There are no pending requests so close the connection.
        it->second->redis_client_->close();
      }
    }
    // There is the possibility that multiple hosts with the same address
    // are registered in host_address_map_ given that hosts may be created
    // upon redirection or supplied as part of the cluster's definition.
    auto it2 = host_address_map_.find(host->address()->asString());
    if ((it2 != host_address_map_.end()) && (it2->second == host)) {
      host_address_map_.erase(it2);
    }
  }
}

void InstanceImpl::ThreadLocalPool::drainClients() {
  while (!clients_to_drain_.empty() && !(*clients_to_drain_.begin())->redis_client_->active()) {
    (*clients_to_drain_.begin())->redis_client_->close();
  }
  if (!clients_to_drain_.empty()) {
    drain_timer_->enableTimer(std::chrono::seconds(1));
  }
}

InstanceImpl::ThreadLocalActiveClientPtr&
InstanceImpl::ThreadLocalPool::threadLocalActiveClient(Upstream::HostConstSharedPtr host) {
  TokenBucketPtr& rate_limiter = cx_rate_limiter_map_[host];
  if (config_->connectionRateLimitEnabled() && !rate_limiter) {
    rate_limiter = std::make_unique<TokenBucketImpl>(config_->connectionRateLimitPerSec(),
                                                     dispatcher_.timeSource(),
                                                     config_->connectionRateLimitPerSec());
  }
  ThreadLocalActiveClientPtr& client = client_map_[host];
  if (!client) {
    if (config_->connectionRateLimitEnabled() && rate_limiter->consume(1, false) == 0) {
      redis_cluster_stats_.connection_rate_limited_.inc();
      // Make sure to return a null client if rate limited, even if it was already allocated
      // This path should ideally not be hit if client is already non-null, but as a safeguard.
      if (client && !client->redis_client_) { // If client exists but is not valid
         client_map_.erase(host); // remove bad entry
      }
      // Return the existing client (which might be nullptr if erased or never created)
      // This will effectively return nullptr if rate limited on first creation attempt.
      return client_map_.count(host) ? client_map_.at(host) : client_map_[host];
    } else {
      client = std::make_unique<ThreadLocalActiveClient>(*this);
      client->host_ = host;
      // Do not pass auth_username_ and auth_password_ here if IAM is enabled,
      // as IAM auth will handle it.
      std::string client_auth_user = iam_auth_enabled_ ? "" : auth_username_;
      std::string client_auth_pass = iam_auth_enabled_ ? "" : auth_password_;

      client->redis_client_ = client_factory_.create(
          host, dispatcher_, config_, redis_command_stats_, *(stats_scope_), client_auth_user,
          client_auth_pass, false);
      client->redis_client_->addConnectionCallbacks(*client);
    }
  }
  return client;
}

uint16_t InstanceImpl::ThreadLocalPool::shardSize() {
  if (cluster_ == nullptr) {
    ASSERT(client_map_.empty());
    ASSERT(host_set_member_update_cb_handle_ == nullptr);
    return 0;
  }

  Common::Redis::RespValue request;
  for (uint16_t size = 0;; size++) {
    Clusters::Redis::RedisSpecifyShardContextImpl lb_context(
        size, request, Common::Redis::Client::ReadPolicy::Primary);
    Upstream::HostConstSharedPtr host = Upstream::LoadBalancer::onlyAllowSynchronousHostSelection(
        cluster_->loadBalancer().chooseHost(&lb_context));
    if (!host) {
      return size;
    }
  }
  return 0;
}

Common::Redis::Client::PoolRequest*
InstanceImpl::ThreadLocalPool::makeRequest(const std::string& key, RespVariant&& request,
                                           PoolCallbacks& callbacks,
                                           Common::Redis::Client::Transaction& transaction) {
  if (cluster_ == nullptr) {
    ASSERT(client_map_.empty());
    ASSERT(host_set_member_update_cb_handle_ == nullptr);
    return nullptr;
  }

  Clusters::Redis::RedisLoadBalancerContextImpl lb_context(
      key, config_->enableHashtagging(), is_redis_cluster_, getRequest(request),
      transaction.active_ ? Common::Redis::Client::ReadPolicy::Primary : config_->readPolicy());

  Upstream::HostConstSharedPtr host = Upstream::LoadBalancer::onlyAllowSynchronousHostSelection(
      cluster_->loadBalancer().chooseHost(&lb_context));
  if (!host) {
    ENVOY_LOG(debug, "host not found: '{}'", key);
    return nullptr;
  }

  return makeRequestToHost(host, std::move(request), callbacks, transaction);
}

Common::Redis::Client::PoolRequest*
InstanceImpl::ThreadLocalPool::makeRequestToShard(uint16_t shard_index, RespVariant&& request,
                                                  PoolCallbacks& callbacks,
                                                  Common::Redis::Client::Transaction& transaction) {
  if (cluster_ == nullptr) {
    ASSERT(client_map_.empty());
    ASSERT(host_set_member_update_cb_handle_ == nullptr);
    return nullptr;
  }

  Clusters::Redis::RedisSpecifyShardContextImpl lb_context(
      shard_index, getRequest(request),
      transaction.active_ ? Common::Redis::Client::ReadPolicy::Primary : config_->readPolicy());

  Upstream::HostConstSharedPtr host = Upstream::LoadBalancer::onlyAllowSynchronousHostSelection(
      cluster_->loadBalancer().chooseHost(&lb_context));
  if (!host) {
    ENVOY_LOG(debug, "host not found: '{}'", shard_index);
    return nullptr;
  }
  return makeRequestToHost(host, std::move(request), callbacks, transaction);
}

Common::Redis::Client::PoolRequest* InstanceImpl::ThreadLocalPool::makeRequestToHost(
    const std::string& host_address, const Common::Redis::RespValue& request,
    Common::Redis::Client::ClientCallbacks& callbacks) {
  if (cluster_ == nullptr) {
    ASSERT(client_map_.empty());
    ASSERT(host_set_member_update_cb_handle_ == nullptr);
    return nullptr;
  }

  auto colon_pos = host_address.rfind(':');
  if ((colon_pos == std::string::npos) || (colon_pos == (host_address.size() - 1))) {
    return nullptr;
  }

  const std::string ip_address = host_address.substr(0, colon_pos);
  const bool ipv6 = (ip_address.find(':') != std::string::npos);
  std::string host_address_map_key;
  Network::Address::InstanceConstSharedPtr address_ptr;

  if (!ipv6) {
    host_address_map_key = host_address;
  } else {
    const auto ip_port = absl::string_view(host_address).substr(colon_pos + 1);
    uint32_t ip_port_number;
    if (!absl::SimpleAtoi(ip_port, &ip_port_number) || (ip_port_number > 65535)) {
      return nullptr;
    }
    TRY_NEEDS_AUDIT {
      address_ptr = std::make_shared<Network::Address::Ipv6Instance>(ip_address, ip_port_number);
    }
    END_TRY catch (const EnvoyException&) { return nullptr; }
    host_address_map_key = address_ptr->asString();
  }

  auto it = host_address_map_.find(host_address_map_key);
  if (it == host_address_map_.end()) {
    // This host is not known to the cluster manager. Create a new host and insert it into the map.
    if (created_via_redirect_hosts_.size() == config_->maxUpstreamUnknownConnections()) {
      // Too many upstream connections to unknown hosts have been created.
      redis_cluster_stats_.max_upstream_unknown_connections_reached_.inc();
      return nullptr;
    }
    if (!ipv6) {
      // Only create an IPv4 address instance if we need a new Upstream::HostImpl.
      const auto ip_port = absl::string_view(host_address).substr(colon_pos + 1);
      uint32_t ip_port_number;
      if (!absl::SimpleAtoi(ip_port, &ip_port_number) || (ip_port_number > 65535)) {
        return nullptr;
      }
      TRY_NEEDS_AUDIT {
        address_ptr = std::make_shared<Network::Address::Ipv4Instance>(ip_address, ip_port_number);
      }
      END_TRY catch (const EnvoyException&) { return nullptr; }
    }
    Upstream::HostSharedPtr new_host{THROW_OR_RETURN_VALUE(
        Upstream::HostImpl::create(
            cluster_->info(), "", address_ptr, nullptr, nullptr, 1,
            envoy::config::core::v3::Locality(),
            envoy::config::endpoint::v3::Endpoint::HealthCheckConfig::default_instance(), 0,
            envoy::config::core::v3::UNKNOWN, dispatcher_.timeSource()),
        std::unique_ptr<Upstream::HostImpl>)};
    host_address_map_[host_address_map_key] = new_host;
    created_via_redirect_hosts_.push_back(new_host);
    it = host_address_map_.find(host_address_map_key);
  }

  ThreadLocalActiveClientPtr& client = threadLocalActiveClient(it->second);
  if (!client) {
    ENVOY_LOG(debug, "redis connection is rate limited, erasing empty client");
    client_map_.erase(it->second);
    return nullptr;
  }

  return client->redis_client_->makeRequest(request, callbacks);
}

Common::Redis::Client::PoolRequest*
InstanceImpl::ThreadLocalPool::makeRequestToHost(Upstream::HostConstSharedPtr& host,
                                                 RespVariant&& request, PoolCallbacks& callbacks,
                                                 Common::Redis::Client::Transaction& transaction) {
  uint32_t client_idx = transaction.current_client_idx_;
  if (transaction.active_ && !transaction.connection_established_) {
    std::string client_auth_user = iam_auth_enabled_ ? "" : auth_username_;
    std::string client_auth_pass = iam_auth_enabled_ ? "" : auth_password_;
    transaction.clients_[client_idx] = client_factory_.create(
        host, dispatcher_, config_, redis_command_stats_, *(stats_scope_), client_auth_user,
        client_auth_pass, true);
    if (transaction.connection_cb_) {
      transaction.clients_[client_idx]->addConnectionCallbacks(*transaction.connection_cb_);
    }
    // Note: IAM auth for transactional clients is not explicitly handled here.
    // It's assumed the connection becomes ready after this, and if IAM is needed,
    // it would have to be handled by the transactional client's connection events.
  }

  pending_requests_.emplace_back(*this, std::move(request), callbacks, host);
  PendingRequest& pending_request = pending_requests_.back();

  if (!transaction.active_) {
    ThreadLocalActiveClientPtr& client_ptr_ref = this->threadLocalActiveClient(host);
    if (!client_ptr_ref || !client_ptr_ref->redis_client_) {
      ENVOY_LOG(debug,
                "Redis connection is rate limited or client is invalid for host {}; failing request.",
                host->address()->asString());
      pending_request.request_handler_ = nullptr;
      pending_request.onFailure(); // This will also call parent_.onRequestCompleted().
      return nullptr;
    }

    ThreadLocalActiveClient* client = client_ptr_ref.get();
    if (iam_auth_enabled_) { // parent_.iam_auth_enabled_ can be used too
      if (client->iam_auth_pending_) {
        ENVOY_LOG(debug, "IAM auth pending for host {}, queueing request in ActiveClient.",
                  host->address()->asString());
        client->iam_waiting_requests_.push_back(&pending_request);
        // request_handler_ remains null for now. It will be set when auth completes.
      } else if (client->iam_auth_completed_) {
        ENVOY_LOG(debug, "IAM auth completed for host {}, making request immediately.",
                  host->address()->asString());
        pending_request.request_handler_ = client->redis_client_->makeRequest(
            getRequest(pending_request.incoming_request_), pending_request);
      } else {
        // IAM is enabled, but auth is not pending and not completed.
        // This typically means auth failed, or connection just came up and onEvent(Connected)
        // hasn't run yet to kick off auth. If auth failed, client should be closing.
        // Queueing it here allows onEvent to pick it up if it's a new connection.
        ENVOY_LOG(warn,
                  "IAM auth enabled but not complete or pending for host {}; queueing request. Possible race or prior auth failure.",
                  host->address()->asString());
        client->iam_waiting_requests_.push_back(&pending_request);
      }
    } else { // IAM not enabled
      pending_request.request_handler_ = client->redis_client_->makeRequest(
          getRequest(pending_request.incoming_request_), pending_request);
    }
  } else { // Transaction active
     if (iam_auth_enabled_) {
        ENVOY_LOG(warn, "Using transaction with IAM-enabled host {}. Assuming transactional client handles its own auth if necessary. This path may have issues with token expiry.", host->address()->asString());
     }
    pending_request.request_handler_ = transaction.clients_[client_idx]->makeRequest(
        getRequest(pending_request.incoming_request_), pending_request);
  }

  // If the request wasn't queued and failed to be made synchronously
  if (pending_request.request_handler_ == nullptr &&
      !(iam_auth_enabled_ && client_ptr_ref && client_ptr_ref.get()->iam_auth_pending_ && !transaction.active_)) {
     // The check for iam_auth_pending is to ensure we don't prematurely fail requests that are intentionally queued.
    ENVOY_LOG(debug, "Request handler is null and not IAM pending for host {}, failing request.", host->address()->asString());
    // Call onFailure() on the pending_request to ensure it's cleaned up from the ThreadLocalPool's list.
    // This covers cases where makeRequest fails synchronously (e.g., buffer limits).
    pending_request.onFailure();
    return nullptr;
  }
  
  if (pending_request.request_handler_) {
    return &pending_request;
  } else {
    onRequestCompleted();
    return nullptr;
  }
}

void InstanceImpl::ThreadLocalPool::onRequestCompleted() {
  ASSERT(!pending_requests_.empty());

  // The response we got might not be in order, so flush out what we can. (A new response may
  // unlock several out of order responses).
  while (!pending_requests_.empty() && !pending_requests_.front().request_handler_) {
    pending_requests_.pop_front();
  }
}

void InstanceImpl::ThreadLocalActiveClient::onEvent(Network::ConnectionEvent event) {
  if (event == Network::ConnectionEvent::RemoteClose ||
      event == Network::ConnectionEvent::LocalClose) {
    // If IAM auth was pending and connection dropped, log it.
    if (parent_.iam_auth_enabled_ && iam_auth_pending_) {
      ENVOY_LOG(warn, "Connection closed during IAM authentication for host {}",
                host_->address()->asString());
    }
    iam_auth_pending_ = false;
    iam_auth_completed_ = false;

    // Existing cleanup logic
    auto client_to_delete = parent_.client_map_.find(host_);
    if (client_to_delete != parent_.client_map_.end()) {
      // Only schedule for deletion if it's the current client instance.
      if (client_to_delete->second.get() == this) {
        parent_.dispatcher_.deferredDelete(std::move(redis_client_));
        parent_.client_map_.erase(client_to_delete);
      }
    } else {
      for (auto it = parent_.clients_to_drain_.begin(); it != parent_.clients_to_drain_.end();
           it++) {
        if ((*it).get() == this) {
          if (redis_client_ && !redis_client_->active()) { // Check if it was active before being drained
            parent_.redis_cluster_stats_.upstream_cx_drained_.inc();
          }
          parent_.dispatcher_.deferredDelete(std::move(redis_client_));
          parent_.clients_to_drain_.erase(it);
          break;
        }
      }
    }
  } else if (event == Network::ConnectionEvent::Connected) {
    if (parent_.iam_auth_enabled_) {
      iam_auth_pending_ = true;
      iam_auth_completed_ = false;
      original_callbacks_ = nullptr; 
      sendIAMAuthRequest();
    } else {
      // No IAM auth, connection is ready for requests immediately.
      // Existing logic in ThreadLocalPool::makeRequestToHost will handle pending requests.
      ENVOY_LOG(debug, "Connection established and IAM auth not enabled for host {}", host_->address()->asString());
    }
  }
}

void InstanceImpl::ThreadLocalActiveClient::sendIAMAuthRequest() {
  ASSERT(parent_.iam_auth_enabled_ && iam_auth_pending_ && !iam_auth_completed_);
  ENVOY_LOG(debug, "Attempting IAM authentication for host {}", host_->address()->asString());

  auto parent_instance = parent_.parent_.lock();
  if (!parent_instance) {
    ENVOY_LOG(warn, "Parent InstanceImpl is gone, cannot generate IAM token for host {}", host_->address()->asString());
    handleAuthFailure("Parent instance for IAM token generation unavailable");
    return;
  }
  
  std::string region = parent_instance->aws_client_config_ ? parent_instance->aws_client_config_->region : "";
  if (region.empty()){
    ENVOY_LOG(warn, "AWS region is empty, cannot generate IAM token for host {}. Check AWS_REGION env var or FQDN.", host_->address()->asString());
    handleAuthFailure("AWS region not configured for IAM token generation");
    return;
  }

  std::string hostname = host_->address()->asString(); 

  // The hostname for SigV4 is just the host part, without port for ElastiCache token generation.
  std::string service_hostname = hostname;
  size_t colon_pos = hostname.rfind(':');
  if (colon_pos != std::string::npos) {
    service_hostname = hostname.substr(0, colon_pos);
  }
  
  std::string token = parent_instance->generateIAMAuthToken(parent_.redis_user_, service_hostname, region);

  if (token.empty()) {
    handleAuthFailure("IAM token generation failed");
    return;
  }

  Common::Redis::RespValue auth_command;
  auth_command.type(Common::Redis::RespType::Array);
  auth_command.append(Common::Redis::BulkStringValue("AUTH"));
  auth_command.append(Common::Redis::BulkStringValue(parent_.redis_user_));
  auth_command.append(Common::Redis::BulkStringValue(token));

  ENVOY_LOG(debug, "Sending IAM AUTH command for user {} to host {}", parent_.redis_user_, hostname);
  // Use 'this' as ClientCallbacks for the AUTH command itself.
  // Capture original_callbacks_ from the pending request that triggered this connection, if any.
  // This is tricky because sendIAMAuthRequest is called from onEvent(Connected), not directly from makeRequest.
  // For now, original_callbacks_ remains null here. The actual user request processing
  // will happen after iam_auth_completed_ is true.
  if (!redis_client_->makeRequest(auth_command, *this)) { // `*this` means ThreadLocalActiveClient handles callbacks for AUTH
      handleAuthFailure("Failed to send AUTH command to Redis client");
  }
}

// This is the onResponse for the AUTH command itself.
void InstanceImpl::ThreadLocalActiveClient::onResponse(Common::Redis::RespValuePtr&& response) {
  if (!iam_auth_pending_) {
    // This implies the response is for a user request that was made after auth completed,
    // or this client doesn't use IAM. The PendingRequest itself is the callback.
    ENVOY_LOG(error, "ThreadLocalActiveClient::onResponse called when not expecting AUTH response for host {}",
              host_ ? host_->address()->asString() : "unknown_host");
    return;
  }

  iam_auth_pending_ = false;
  if (response && response->type() == Common::Redis::RespType::SimpleString &&
      response->asString() == "OK") {
    ENVOY_LOG(debug, "IAM AUTH successful for user {} on host {}", parent_.redis_user_,
              host_->address()->asString());
    iam_auth_completed_ = true;

    // Dispatch any queued requests
    for (PendingRequest* waiting_req : iam_waiting_requests_) {
      ENVOY_LOG(debug, "Dispatching queued request for host {} after IAM auth success",
                host_->address()->asString());
      if (waiting_req->request_handler_ != nullptr) {
          ENVOY_LOG(warn, "Queued request for host {} already has a handler before dispatch after IAM. Skipping.", host_->address()->asString());
          continue;
      }
      // It's crucial that `*waiting_req` (which is ClientCallbacks) is correctly handled
      // if this makeRequest fails synchronously or asynchronously.
      waiting_req->request_handler_ = redis_client_->makeRequest(
          getRequest(waiting_req->incoming_request_), *waiting_req);
      if (!waiting_req->request_handler_) {
        ENVOY_LOG(warn, "Failed to make queued request for host {} after IAM auth success. Triggering failure for pending request.",
                  host_->address()->asString());
        // If makeRequest fails immediately (e.g. client closed, buffer full),
        // directly trigger onFailure for the PendingRequest.
        waiting_req->onFailure();
      }
    }
    iam_waiting_requests_.clear();

  } else {
    std::string error_msg = response ? response->toString() : "unknown error or null response";
    ENVOY_LOG(warn, "IAM AUTH failed for user {} on host {}: {}", parent_.redis_user_,
              host_->address()->asString(), error_msg);
    handleAuthFailure("IAM AUTH command failed: " + error_msg);
  }
}

// This is the onFailure for the AUTH command itself.
void InstanceImpl::ThreadLocalActiveClient::onFailure() {
    if (!iam_auth_pending_) {
        ENVOY_LOG(error, "ThreadLocalActiveClient::onFailure called when not expecting AUTH failure for host {}",
              host_ ? host_->address()->asString() : "unknown_host");
        return;
    }
    ENVOY_LOG(warn, "IAM AUTH connection failure during AUTH command for user {} on host {}", parent_.redis_user_,
              host_->address()->asString());
    handleAuthFailure("AUTH command connection failure");
}

void InstanceImpl::ThreadLocalActiveClient::handleAuthFailure(const std::string& reason) {
  ENVOY_LOG(warn, "IAM Authentication failure for host {}: {}",
            host_ ? host_->address()->asString() : "unknown_host", reason);
  
  iam_auth_pending_ = false;
  iam_auth_completed_ = false;

  for (PendingRequest* req_to_fail : iam_waiting_requests_) {
    ENVOY_LOG(warn, "Failing queued request for host {} due to IAM auth failure on client",
              host_ ? host_->address()->asString() : "unknown_host");
    // req_to_fail is a raw pointer to an object managed by parent_.pending_requests_ (a std::list).
    // Calling onFailure() on it will trigger its own cleanup, including removal from parent_.pending_requests_.
    if (req_to_fail->request_handler_ == nullptr) { // Ensure it wasn't somehow processed.
        req_to_fail->onFailure();
    } else {
        // If it has a handler, it means it was dispatched, which shouldn't happen if auth failed.
        // However, to be safe, cancel it.
        ENVOY_LOG(warn, "Queued request for host {} had a handler during auth failure. Cancelling.", 
                  host_ ? host_->address()->asString() : "unknown_host");
        req_to_fail->cancel(); // This will also call onFailure and clean up.
    }
  }
  iam_waiting_requests_.clear();

  if (redis_client_) {
    redis_client_->close(); 
  }
}

InstanceImpl::PendingRequest::PendingRequest(InstanceImpl::ThreadLocalPool& parent,
                                             RespVariant&& incoming_request,
                                             PoolCallbacks& pool_callbacks,
                                             Upstream::HostConstSharedPtr& host)
    : parent_(parent), incoming_request_(std::move(incoming_request)),
      pool_callbacks_(pool_callbacks), host_(host) {}

InstanceImpl::PendingRequest::~PendingRequest() {
  cache_load_handle_.reset();

  if (request_handler_) {
    request_handler_->cancel();
    request_handler_ = nullptr;
    // If we have to cancel the request on the client, then we'll treat this as failure for pool
    // callback
    pool_callbacks_.onFailure();
  }
}

void InstanceImpl::PendingRequest::onResponse(Common::Redis::RespValuePtr&& response) {
  request_handler_ = nullptr;
  pool_callbacks_.onResponse(std::move(response));
  parent_.onRequestCompleted();
}

void InstanceImpl::PendingRequest::onFailure() {
  request_handler_ = nullptr;
  pool_callbacks_.onFailure();
  parent_.refresh_manager_->onFailure(parent_.cluster_name_);
  parent_.onRequestCompleted();
}

void InstanceImpl::PendingRequest::onRedirection(Common::Redis::RespValuePtr&& value,
                                                 const std::string& host_address,
                                                 bool ask_redirection) {
  if (!parent_.dns_cache_) {
    doRedirection(std::move(value), host_address, ask_redirection);
    return;
  }

  resp_value_ = std::move(value);
  ask_redirection_ = ask_redirection;
  auto result = parent_.dns_cache_->loadDnsCacheEntry(host_address, default_port, false, *this);
  cache_load_handle_ = std::move(result.handle_);

  switch (result.status_) {
  case Extensions::Common::DynamicForwardProxy::DnsCache::LoadDnsCacheEntryStatus::InCache: {
    ASSERT(cache_load_handle_ == nullptr);
    if (!result.host_info_.has_value() || !result.host_info_.value()->address()) {
      ENVOY_LOG(debug, "DNS entry for '{}' was in cache but did not contain an address",
                host_address);
      auto host = host_;
      onResponse(std::move(resp_value_));
      host->cluster().trafficStats()->upstream_internal_redirect_failed_total_.inc();
    } else {
      doRedirection(std::move(resp_value_),
                    formatAddress(*result.host_info_.value()->address()->ip()), ask_redirection_);
    }
    return;
  }
  case Extensions::Common::DynamicForwardProxy::DnsCache::LoadDnsCacheEntryStatus::Loading:
    ASSERT(cache_load_handle_ != nullptr);
    return;
  case Extensions::Common::DynamicForwardProxy::DnsCache::LoadDnsCacheEntryStatus::Overflow:
    ASSERT(cache_load_handle_ == nullptr);
    ENVOY_LOG(debug, "DNS lookup for '{}' was not performed due to an overflow in the cache",
              host_address);
    auto host = host_;
    onResponse(std::move(resp_value_));
    host->cluster().trafficStats()->upstream_internal_redirect_failed_total_.inc();
    return;
  }
  PANIC_DUE_TO_CORRUPT_ENUM;
}

std::string InstanceImpl::PendingRequest::formatAddress(const Envoy::Network::Address::Ip& ip) {
  return fmt::format("{}:{}", ip.addressAsString(), ip.port());
}
void InstanceImpl::PendingRequest::onLoadDnsCacheComplete(
    const Extensions::Common::DynamicForwardProxy::DnsHostInfoSharedPtr& host_info) {
  cache_load_handle_.reset();

  if (!host_info || !host_info->address()) {
    ENVOY_LOG(debug, "DNS lookup failed");
    auto host = host_;
    onResponse(std::move(resp_value_));
    host->cluster().trafficStats()->upstream_internal_redirect_failed_total_.inc();
  } else {
    doRedirection(std::move(resp_value_), formatAddress(*host_info->address()->ip()),
                  ask_redirection_);
  }
}

void InstanceImpl::PendingRequest::doRedirection(Common::Redis::RespValuePtr&& value,
                                                 const std::string& host_address,
                                                 bool ask_redirection) {
  // This request might go away, so keep a copy of host.
  auto host = host_;

  // Prepend request with an asking command if redirected via an ASK error. The returned handle is
  // not important since there is no point in being able to cancel the request. The use of
  // null_pool_callbacks ensures the transparent filtering of the Redis server's response to the
  // "asking" command; this is fine since the server either responds with an OK or an error message
  // if cluster support is not enabled (in which case we should not get an ASK redirection error).
  if (ask_redirection &&
      !parent_.makeRequestToHost(host_address, Common::Redis::Utility::AskingRequest::instance(),
                                 null_client_callbacks)) {
    onResponse(std::move(value));
    host->cluster().trafficStats()->upstream_internal_redirect_failed_total_.inc();
  } else {
    request_handler_ =
        parent_.makeRequestToHost(host_address, getRequest(incoming_request_), *this);
    if (!request_handler_) {
      onResponse(std::move(value));
      host->cluster().trafficStats()->upstream_internal_redirect_failed_total_.inc();
    } else {
      parent_.refresh_manager_->onRedirection(parent_.cluster_name_);
      host->cluster().trafficStats()->upstream_internal_redirect_succeeded_total_.inc();
    }
  }
}

void InstanceImpl::PendingRequest::cancel() {
  request_handler_->cancel();
  request_handler_ = nullptr;
  parent_.onRequestCompleted();
}

} // namespace ConnPool
} // namespace RedisProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
