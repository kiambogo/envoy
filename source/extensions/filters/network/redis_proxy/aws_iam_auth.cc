#include "source/extensions/filters/network/redis_proxy/aws_iam_auth.h"

#include <cstddef>
#include <memory>

#include "source/common/http/message_impl.h"
#include "source/common/crypto/utility.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RedisProxy {

AwsIamAuthenticator::AwsIamAuthenticator(
    const std::string& region,
    const std::string& user_id,
    const Extensions::Common::Aws::CredentialsProviderSharedPtr& credentials_provider,
    const Extensions::Common::Aws::SignerPtr& signer,
    TimeSource& time_source)
    : region_(region), user_id_(user_id), credentials_provider_(credentials_provider),
      signer_(signer), time_source_(time_source) {}

std::string AwsIamAuthenticator::getAuthToken() {
  const MonotonicTime now = time_source_.monotonicTime();
  
  // Check if we need to refresh the token
  if (current_token_.empty() || now >= token_expiry_) {
    refreshToken();
  }
  
  return current_token_;
}

void AwsIamAuthenticator::refreshToken() {
  // Get AWS credentials
  const auto credentials = credentials_provider_->getCredentials();
  if (!credentials.accessKeyId() || !credentials.secretAccessKey()) {
    ENVOY_LOG(error, "Failed to get AWS credentials for ElastiCache authentication");
    return;
  }

  // Create request to sign
  Http::RequestMessageImpl message;
  message.headers().setMethod("GET");
  message.headers().setPath("/");
  message.headers().setHost("elasticache." + region_ + ".amazonaws.com");
  
  // Add required headers for ElastiCache auth
  message.headers().addCopy(Http::LowerCaseString("x-amz-elasticache-user-id"), user_id_);
  
  // Sign the request
  auto status = signer_->sign(message, true);
  if (!status.ok()) {
    ENVOY_LOG(error, "Failed to sign ElastiCache auth request: {}", status.ToString());
    return;
  }

  // Extract signature and create ElastiCache auth token
  // Format: user-<access_key_id>:<token>
  const auto auth_header = message.headers().get(Http::CustomHeaders::get().Authorization);
  if (auth_header.empty()) {
    ENVOY_LOG(error, "Missing Authorization header after signing");
    return;
  }

  // Extract signature from Authorization header
  const auto signature_pos = auth_header[0]->value().getStringView().find("Signature=");
  if (signature_pos == absl::string_view::npos) {
    ENVOY_LOG(error, "Failed to extract signature from Authorization header");
    return;
  }
  const std::string signature = std::string(
      auth_header[0]->value().getStringView().substr(signature_pos + 10));

  // Format ElastiCache auth token
  current_token_ = absl::StrCat("user-", credentials.accessKeyId().value(), ":", signature);

  // Set token expiry (15 minutes from now)
  token_expiry_ = time_source_.monotonicTime() + TOKEN_VALIDITY;
}

} // namespace RedisProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy 