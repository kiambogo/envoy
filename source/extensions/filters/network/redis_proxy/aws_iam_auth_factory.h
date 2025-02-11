#pragma once

#include "envoy/server/factory_context.h"
#include "source/extensions/filters/network/redis_proxy/aws_iam_auth.h"
#include "source/extensions/common/aws/credentials_provider.h"
#include "source/extensions/common/aws/sigv4_signer_impl.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RedisProxy {

class AwsIamAuthenticatorFactory {
public:
  static AwsIamAuthenticatorPtr create(
      const envoy::config::filter::network::redis_proxy::v2::RedisProxy::AwsIamAuthConfig& config,
      Server::Configuration::FactoryContext& context);

private:
  static Extensions::Common::Aws::CredentialsProviderSharedPtr createCredentialsProvider(
      const envoy::config::filter::network::redis_proxy::v2::RedisProxy::AwsIamAuthConfig& config,
      Server::Configuration::FactoryContext& context);
      
  static Extensions::Common::Aws::SignerPtr createSigner(
      const envoy::config::filter::network::redis_proxy::v2::RedisProxy::AwsIamAuthConfig& config,
      Server::Configuration::FactoryContext& context);
};

} // namespace RedisProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy 