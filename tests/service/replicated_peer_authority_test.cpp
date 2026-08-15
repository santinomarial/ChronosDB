#include "chronos/service/replicated_peer_authority.hpp"

#include <gtest/gtest.h>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

struct PeerEndpointSeed {
  std::uint8_t address;
  std::uint16_t port;
  std::uint8_t fingerprint;
};

[[nodiscard]] ReplicatedPeer peer(const raft::NodeId node_id, const PeerEndpointSeed seed) {
  ReplicatedPeer value{.node_id = node_id,
                       .endpoint = {{127U, 0U, 0U, seed.address}, seed.port},
                       .tls_server_identity = "node.example.test"};
  value.certificate_sha256.fill(seed.fingerprint);
  return value;
}

TEST(ReplicatedPeerAuthorityTest, AuthenticatesAndAuthorizesOneExactConfiguredNode) {
  auto authority = ReplicatedPeerAuthority::create(
      2U, {peer(1U, {.address = 1U, .port = 7001U, .fingerprint = 11U}),
           peer(2U, {.address = 2U, .port = 7002U, .fingerprint = 22U}),
           peer(3U, {.address = 3U, .port = 7003U, .fingerprint = 33U})});
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  EXPECT_EQ(authority->local_node_id(), 2U);
  EXPECT_EQ(authority->local_peer().endpoint.port, 7002U);
  ASSERT_EQ(authority->peers().size(), 3U);
  ASSERT_NE(authority->find_peer(3U), nullptr);
  EXPECT_EQ(authority->find_peer(3U)->endpoint.address.back(), 3U);
  EXPECT_EQ(authority->find_peer(4U), nullptr);

  const ReplicatedPeer* first = authority->find_peer(1U);
  ASSERT_NE(first, nullptr);
  auto authenticated =
      authority->authenticate({.ipv4_address = first->endpoint.address,
                               .transport_authenticated = true,
                               .peer_certificate_sha256 = first->certificate_sha256});
  ASSERT_TRUE(authenticated.has_value());
  EXPECT_TRUE(authenticated->authorized);
  EXPECT_EQ(authenticated->principal_id, 1U);
  auto authorized = authority->authorize_node(authenticated->principal_id, 1U);
  ASSERT_TRUE(authorized.has_value());
  EXPECT_TRUE(*authorized);
  EXPECT_FALSE(*authority->authorize_node(authenticated->principal_id, 2U));
  EXPECT_FALSE(*authority->authorize_node(4U, 4U));
}

TEST(ReplicatedPeerAuthorityTest, RejectsUnverifiedUnknownAndWrongAddressPeers) {
  ReplicatedPeer remote = peer(2U, {.address = 2U, .port = 7002U, .fingerprint = 22U});
  auto authority = ReplicatedPeerAuthority::create(
      1U, {peer(1U, {.address = 1U, .port = 7001U, .fingerprint = 11U}), remote});
  ASSERT_TRUE(authority.has_value());
  auto unverified = authority->authenticate({.ipv4_address = remote.endpoint.address});
  ASSERT_FALSE(unverified.has_value());
  EXPECT_EQ(unverified.error().code(), common::StatusCode::kUnauthenticated);

  auto wrong_address =
      authority->authenticate({.ipv4_address = {127U, 0U, 0U, 9U},
                               .transport_authenticated = true,
                               .peer_certificate_sha256 = remote.certificate_sha256});
  ASSERT_TRUE(wrong_address.has_value());
  EXPECT_FALSE(wrong_address->authorized);
  EXPECT_EQ(wrong_address->principal_id, 0U);

  remote.certificate_sha256.fill(99U);
  auto unknown = authority->authenticate({.ipv4_address = remote.endpoint.address,
                                          .transport_authenticated = true,
                                          .peer_certificate_sha256 = remote.certificate_sha256});
  ASSERT_TRUE(unknown.has_value());
  EXPECT_FALSE(unknown->authorized);
}

TEST(ReplicatedPeerAuthorityTest, RejectsMissingLocalAndAmbiguousConfiguration) {
  EXPECT_FALSE(ReplicatedPeerAuthority::create(
                   0U, {peer(1U, {.address = 1U, .port = 7001U, .fingerprint = 11U})})
                   .has_value());
  EXPECT_FALSE(ReplicatedPeerAuthority::create(
                   2U, {peer(1U, {.address = 1U, .port = 7001U, .fingerprint = 11U})})
                   .has_value());
  EXPECT_FALSE(ReplicatedPeerAuthority::create(
                   1U, {peer(2U, {.address = 2U, .port = 7002U, .fingerprint = 22U}),
                        peer(1U, {.address = 1U, .port = 7001U, .fingerprint = 11U})})
                   .has_value());
  EXPECT_FALSE(ReplicatedPeerAuthority::create(
                   1U, {peer(1U, {.address = 1U, .port = 7001U, .fingerprint = 11U}),
                        peer(2U, {.address = 1U, .port = 7001U, .fingerprint = 22U})})
                   .has_value());
  EXPECT_FALSE(ReplicatedPeerAuthority::create(
                   1U, {peer(1U, {.address = 1U, .port = 7001U, .fingerprint = 11U}),
                        peer(2U, {.address = 2U, .port = 7002U, .fingerprint = 11U})})
                   .has_value());
}

} // namespace
} // namespace chronos::service
