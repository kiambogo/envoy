#include <memory>
#include <string>

#include "source/extensions/filters/network/redis_proxy/aws_iam_auth.h"
#include "source/extensions/filters/network/redis_proxy/aws_iam_auth_factory.h"

#include "test/mocks/common.h"
#include "test/mocks/server/factory_context.h"
#include "test/test_common/utility.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using testing::NiceMock;
using testing::Return;
using testing::ReturnRef;

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RedisProxy {
namespace {

class MockAwsCredentialsProvider : public Extensions::Common::Aws::CredentialsProvider {
public:
  MOCK_METHOD(Extensions::Common::Aws::Credentials, getCredentials, ());
};

class AwsIamAuthTest : public testing::Test {
public:
  void SetUp() override {
    credentials_provider_ = std::make_shared<NiceMock<MockAwsCredentialsProvider>>();
    signer_ = std::make_unique<NiceMock<Extensions::Common::Aws::MockSigner>>();
    time_source_ = std::make_unique<NiceMock<MockTimeSystem>>();
  }

protected:
  std::shared_ptr<MockAwsCredentialsProvider> credentials_provider_;
  std::unique_ptr<Extensions::Common::Aws::MockSigner> signer_;
  std::unique_ptr<MockTimeSystem> time_source_;
};

// Test successful token generation
TEST_F(AwsIamAuthTest, GetAuthTokenSuccess) {
  // Setup mock credentials
  Extensions::Common::Aws::Credentials creds("access_key", "secret_key", "token");
  EXPECT_CALL(*credentials_provider_, getCredentials())
      .WillOnce(Return(creds));

  // Setup mock signer response
  Http::TestResponseHeaderMapImpl headers;
  headers.addCopy(Http::CustomHeaders::get().Authorization, 
                 "AWS4-HMAC-SHA256 Credential=access_key/20240101/us-west-2/redis/aws4_request, "
                 "SignedHeaders=host;x-amz-date, Signature=abcdef123456");
  
  EXPECT_CALL(*signer_, sign(testing::_, true))
      .WillOnce(Return(absl::OkStatus()));

  // Create authenticator
  AwsIamAuthenticator auth(credentials_provider_, std::move(signer_), *time_source_);

  // Get auth token
  std::string token = auth.getAuthToken();
  
  // Verify token format: <access_key>:<signature>:<session_token>
  EXPECT_EQ(token, "access_key:abcdef123456:token");
}

// Test token caching
TEST_F(AwsIamAuthTest, TokenCaching) {
  Extensions::Common::Aws::Credentials creds("access_key", "secret_key", "token");
  EXPECT_CALL(*credentials_provider_, getCredentials())
      .WillOnce(Return(creds));

  Http::TestResponseHeaderMapImpl headers;
  headers.addCopy(Http::CustomHeaders::get().Authorization,
                 "AWS4-HMAC-SHA256 Credential=access_key/20240101/us-west-2/redis/aws4_request, "
                 "SignedHeaders=host;x-amz-date, Signature=abcdef123456");

  EXPECT_CALL(*signer_, sign(testing::_, true))
      .WillOnce(Return(absl::OkStatus()));

  AwsIamAuthenticator auth(credentials_provider_, std::move(signer_), *time_source_);

  // First call should generate token
  std::string token1 = auth.getAuthToken();

  // Second call within cache window should return same token without calling AWS
  std::string token2 = auth.getAuthToken();
  EXPECT_EQ(token1, token2);
}

// Test token refresh after expiration
TEST_F(AwsIamAuthTest, TokenRefresh) {
  Extensions::Common::Aws::Credentials creds("access_key", "secret_key", "token");
  EXPECT_CALL(*credentials_provider_, getCredentials())
      .Times(2)
      .WillRepeatedly(Return(creds));

  Http::TestResponseHeaderMapImpl headers1, headers2;
  headers1.addCopy(Http::CustomHeaders::get().Authorization,
                  "AWS4-HMAC-SHA256 Signature=abc123");
  headers2.addCopy(Http::CustomHeaders::get().Authorization, 
                  "AWS4-HMAC-SHA256 Signature=def456");

  EXPECT_CALL(*signer_, sign(testing::_, true))
      .Times(2)
      .WillRepeatedly(Return(absl::OkStatus()));

  AwsIamAuthenticator auth(credentials_provider_, std::move(signer_), *time_source_);

  // Get initial token
  std::string token1 = auth.getAuthToken();

  // Advance time past token expiry
  time_source_->advanceTimeWait(std::chrono::seconds(901));

  // Should get new token
  std::string token2 = auth.getAuthToken();
  EXPECT_NE(token1, token2);
}

// Test integration with Redis proxy filter
class RedisProxyAwsAuthTest : public testing::Test {
public:
  void SetUp() override {
    // Setup filter config with AWS IAM auth
    envoy::config::filter::network::redis_proxy::v2::RedisProxy::AwsIamAuthConfig aws_config;
    aws_config.set_region("us-west-2");
    aws_config.set_cluster_id("test-cluster");
    
    config_.mutable_aws_iam_auth()->CopyFrom(aws_config);
  }

protected:
  envoy::config::filter::network::redis_proxy::v2::RedisProxy config_;
  NiceMock<Server::Configuration::MockFactoryContext> factory_context_;
};

// Test Redis AUTH command with AWS IAM token
TEST_F(RedisProxyAwsAuthTest, AuthCommandWithAwsToken) {
  // Create filter with AWS IAM auth config
  auto aws_auth = AwsIamAuthenticatorFactory::create(config_.aws_iam_auth(), factory_context_);
  ASSERT_NE(aws_auth, nullptr);

  Common::Redis::RespValuePtr auth_command(new Common::Redis::RespValue());
  auth_command->type(Common::Redis::RespType::Array);
  auth_command->asArray().push_back(Common::Redis::RespValue());
  auth_command->asArray().push_back(Common::Redis::RespValue());
  auth_command->asArray()[0].type(Common::Redis::RespType::BulkString);
  auth_command->asArray()[0].asString() = "AUTH";
  auth_command->asArray()[1].type(Common::Redis::RespType::BulkString);
  auth_command->asArray()[1].asString() = aws_auth->getAuthToken();

  // Create Redis proxy filter
  ProxyFilter filter(decoder_factory_, std::move(encoder_), splitter_, config_, nullptr);

  // Process AUTH command
  filter.onRespValue(std::move(auth_command));

  // Verify successful authentication
  EXPECT_TRUE(filter.connectionAllowed());
}

} // namespace
} // namespace RedisProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy 