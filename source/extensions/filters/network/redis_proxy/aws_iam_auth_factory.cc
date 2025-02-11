#include "source/extensions/filters/network/redis_proxy/aws_iam_auth_factory.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RedisProxy {

AwsIamAuthenticatorPtr AwsIamAuthenticatorFactory::create(
    const envoy::config::filter::network::redis_proxy::v2::RedisProxy::AwsIamAuthConfig& config,
    Server::Configuration::FactoryContext& context) {
  
  auto credentials_provider = createCredentialsProvider(config, context);
  auto signer = createSigner(config, context);

  return std::make_unique<AwsIamAuthenticator>(
      config.region(), config.user_id(), credentials_provider, std::move(signer), 
      context.timeSource());
}

Extensions::Common::Aws::CredentialsProviderSharedPtr 
AwsIamAuthenticatorFactory::createCredentialsProvider(
    const envoy::config::filter::network::redis_proxy::v2::RedisProxy::AwsIamAuthConfig& config,
    Server::Configuration::FactoryContext& context) {

  if (config.has_credentials()) {
    // Use static credentials if provided
    return std::make_shared<Extensions::Common::Aws::ConfigCredentialsProvider>(
        config.credentials().access_key_id(),
        config.credentials().secret_access_key(),
        config.credentials().session_token());
  }

  // Use default credentials provider chain
  return std::make_shared<Extensions::Common::Aws::DefaultCredentialsProviderChain>(
      context.api(),
      OptRef<Server::Configuration::ServerFactoryContext>(context),
      config.region(),
      Extensions::Common::Aws::Utility::fetchMetadata);
}

Extensions::Common::Aws::SignerPtr 
AwsIamAuthenticatorFactory::createSigner(
    const envoy::config::filter::network::redis_proxy::v2::RedisProxy::AwsIamAuthConfig& config,
    Server::Configuration::FactoryContext& context) {

  return std::make_unique<Extensions::Common::Aws::SigV4SignerImpl>(
      "elasticache", config.region(),
      createCredentialsProvider(config, context),
      context,
      std::vector<envoy::type::matcher::v3::StringMatcher>{});
}

} // namespace RedisProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy 