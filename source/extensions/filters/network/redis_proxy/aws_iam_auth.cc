#include "source/extensions/filters/network/redis_proxy/aws_iam_auth.h"

#include <cstddef>
#include <memory>

#include "source/common/http/message_impl.h"
#include "source/common/crypto/utility.h"
#include "source/common/common/hex.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RedisProxy {

AwsIamAuthenticator::AwsIamAuthenticator(
    const Extensions::Common::Aws::CredentialsProviderSharedPtr& credentials_provider,
    const Extensions::Common::Aws::SignerPtr& signer,
    TimeSource& time_source)
    : credentials_provider_(credentials_provider), signer_(signer), time_source_(time_source) {}

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
    ENVOY_LOG(error, "Failed to get AWS credentials for Redis authentication");
    return;
  }

  // Create request to sign
  Http::RequestMessageImpl message;
  message.headers().setMethod("GET");
  message.headers().setPath("/redis-auth");
  message.headers().setHost("redis-auth.amazonaws.com");

  // Add required headers for Redis auth token
  message.headers().addCopy(Http::LowerCaseString("x-amz-redis-cluster-id"), "my-cluster");
  
  // Sign the request
  auto status = signer_->sign(message, true);
  if (!status.ok()) {
    ENVOY_LOG(error, "Failed to sign Redis auth request: {}", status.ToString());
    return;
  }

  // Extract signed headers and create Redis auth token
  // Format: <AccessKeyId>:<Signature>:<Token>
  const auto auth_header = message.headers().get(Http::CustomHeaders::get().Authorization);
  if (auth_header.empty()) {
    ENVOY_LOG(error, "Missing Authorization header after signing");
    return;
  }

  // Combine components into Redis auth token format
  current_token_ = absl::StrCat(
      credentials.accessKeyId().value(), ":",
      extractSignature(auth_header[0]->value().getStringView()),
      credentials.sessionToken().has_value() ? ":" + credentials.sessionToken().value() : "");

  // Set token expiry
  token_expiry_ = time_source_.monotonicTime() + TOKEN_VALIDITY;
}

std::string AwsIamAuthenticator::extractSignature(absl::string_view auth_header) {
  // Extract signature from Authorization header
  // Format: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=<signature>
  const auto signature_pos = auth_header.find("Signature=");
  if (signature_pos == absl::string_view::npos) {
    return "";
  }
  return std::string(auth_header.substr(signature_pos + 10));
}

} // namespace RedisProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy 