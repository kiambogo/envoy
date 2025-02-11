#pragma once

#include <string>
#include <memory>

#include "envoy/common/time.h"
#include "source/common/common/logger.h"
#include "source/extensions/common/aws/credentials_provider.h"
#include "source/extensions/common/aws/signer.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RedisProxy {

/**
 * Class for managing AWS IAM authentication tokens for ElastiCache.
 * Handles token generation, caching, and automatic refresh for ElastiCache IAM auth.
 */
class AwsIamAuthenticator : public Logger::Loggable<Logger::Id::redis> {
public:
  AwsIamAuthenticator(const std::string& region,
                      const std::string& user_id,  // ElastiCache user-id
                      const Extensions::Common::Aws::CredentialsProviderSharedPtr& credentials_provider,
                      const Extensions::Common::Aws::SignerPtr& signer,
                      TimeSource& time_source);
  
  /**
   * Get a valid authentication token. Will generate/refresh if needed.
   * @return valid authentication token in format: user-<access_key_id>:<token>
   */
  std::string getAuthToken();

private:
  /**
   * Generate a new authentication token using AWS IAM credentials
   */
  void refreshToken();

  const std::string region_;
  const std::string user_id_;  // ElastiCache user-id
  
  Extensions::Common::Aws::CredentialsProviderSharedPtr credentials_provider_;
  Extensions::Common::Aws::SignerPtr signer_;
  TimeSource& time_source_;
  
  std::string current_token_;
  MonotonicTime token_expiry_;
  
  // Token validity period (15 minutes)
  const std::chrono::seconds TOKEN_VALIDITY{900};
};

using AwsIamAuthenticatorPtr = std::unique_ptr<AwsIamAuthenticator>;

} // namespace RedisProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy 