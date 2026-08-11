#include "chronos/network/security.hpp"

#include <gtest/gtest.h>

namespace chronos::network {
namespace {

class FixedAuthenticator final : public ConnectionAuthenticator {
public:
  explicit FixedAuthenticator(PeerAuthenticationResult result) : result_(result) {}

  common::Result<PeerAuthenticationResult>
  authenticate(const PeerAuthenticationRequest& request) override {
    last_request = request;
    return result_;
  }

  PeerAuthenticationRequest last_request;

private:
  PeerAuthenticationResult result_;
};

TEST(NetworkSecurityTest, PlaintextIsRestrictedToLoopback) {
  NetworkSecurityConfig config;
  EXPECT_TRUE(validate_network_security_config(config, {127U, 0U, 0U, 1U}).is_ok());
  EXPECT_FALSE(validate_network_security_config(config, {0U, 0U, 0U, 0U}).is_ok());
  EXPECT_TRUE(authenticate_peer(config, {.ipv4_address = {127U, 1U, 2U, 3U}}).has_value());
  EXPECT_FALSE(authenticate_peer(config, {.ipv4_address = {10U, 0U, 0U, 1U}}).has_value());
}

TEST(NetworkSecurityTest, CustomAuthenticatorAttachesOnlyNonzeroAuthorizedPrincipal) {
  FixedAuthenticator allowed{{.authorized = true, .principal_id = 42U}};
  NetworkSecurityConfig config{.authenticator = &allowed};
  const auto result = authenticate_peer(config, {.ipv4_address = {127U, 0U, 0U, 1U}});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->principal_id, 42U);
  EXPECT_EQ(allowed.last_request.ipv4_address[0], 127U);

  FixedAuthenticator invalid_identity{{.authorized = true, .principal_id = 0U}};
  config.authenticator = &invalid_identity;
  EXPECT_FALSE(authenticate_peer(config, {.ipv4_address = {127U, 0U, 0U, 1U}}).has_value());
  FixedAuthenticator denied{{.authorized = false}};
  config.authenticator = &denied;
  const auto rejection = authenticate_peer(config, {.ipv4_address = {127U, 0U, 0U, 1U}});
  ASSERT_TRUE(rejection.has_value());
  EXPECT_FALSE(rejection->authorized);
}

TEST(NetworkSecurityTest, TlsRequiredFailsClosedWithoutCredentialsAndVerifiedIdentity) {
  NetworkSecurityConfig config{.mode = TransportSecurityMode::kTlsRequired};
  EXPECT_EQ(validate_network_security_config(config, {127U, 0U, 0U, 1U}).code(),
            common::StatusCode::kInvalidArgument);
  const auto peer = authenticate_peer(config, {.ipv4_address = {127U, 0U, 0U, 1U}});
  ASSERT_FALSE(peer.has_value());
  EXPECT_EQ(peer.error().code(), common::StatusCode::kUnauthenticated);
}

} // namespace
} // namespace chronos::network
