#include "chronos/service/replicated_peer_config.hpp"

#include <gtest/gtest.h>
#include <string>
#include <string_view>

namespace chronos::service {
namespace {

constexpr std::string_view kFingerprint1{
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"};
constexpr std::string_view kFingerprint2{
    "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"};

TEST(ReplicatedPeerConfigTest, ParsesCanonicalAuthenticatedRoutes) {
  const std::string text = "CHRONOSDB_REPLICATED_PEERS_V1\n1=127.0.0.1:7441,"
                           "node-1.example.test," +
                           std::string{kFingerprint1} + "\n2=10.20.30.40:65535,10.20.30.40," +
                           std::string{kFingerprint2} + "\n";
  auto parsed = parse_replicated_peer_config(text);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().to_string();
  ASSERT_EQ(parsed->size(), 2U);
  EXPECT_EQ((*parsed)[0].node_id, 1U);
  EXPECT_EQ((*parsed)[0].endpoint, (network::Ipv4Endpoint{{127U, 0U, 0U, 1U}, 7441U}));
  EXPECT_EQ((*parsed)[0].tls_server_identity, "node-1.example.test");
  EXPECT_EQ((*parsed)[0].certificate_sha256.front(), 0U);
  EXPECT_EQ((*parsed)[0].certificate_sha256.back(), 31U);
  EXPECT_EQ((*parsed)[1].node_id, 2U);
  EXPECT_EQ((*parsed)[1].endpoint.port, 65535U);
  EXPECT_EQ((*parsed)[1].tls_server_identity, "10.20.30.40");
  EXPECT_EQ((*parsed)[1].certificate_sha256.front(), 32U);
}

TEST(ReplicatedPeerConfigTest, RejectsNoncanonicalDamageAndAmbiguousAuthority) {
  constexpr std::string_view prefix{"CHRONOSDB_REPLICATED_PEERS_V1\n"};
  const auto line = [](const std::string_view node, const std::string_view endpoint,
                       const std::string_view identity, const std::string_view fingerprint) {
    return std::string{node} + "=" + std::string{endpoint} + "," + std::string{identity} + "," +
           std::string{fingerprint};
  };
  const auto reject = [&](const std::string& suffix, const common::StatusCode expected =
                                                         common::StatusCode::kInvalidArgument) {
    auto parsed = parse_replicated_peer_config(std::string{prefix} + suffix);
    ASSERT_FALSE(parsed.has_value()) << suffix;
    EXPECT_EQ(parsed.error().code(), expected) << suffix;
  };

  reject("");
  reject(line("0", "127.0.0.1:1", "node.test", kFingerprint1));
  reject(line("01", "127.0.0.1:1", "node.test", kFingerprint1));
  reject(line("1", "127.00.0.1:1", "node.test", kFingerprint1));
  reject(line("1", "256.0.0.1:1", "node.test", kFingerprint1));
  reject(line("1", "127.0.0.1:0", "node.test", kFingerprint1));
  reject(line("1", "127.0.0.1:65536", "node.test", kFingerprint1));
  reject(line("1", "127.0.0.1:1", "Node.test", kFingerprint1));
  reject(line("1", "127.0.0.1:1", "-node.test", kFingerprint1));
  reject(line("1", "127.0.0.1:1", "node..test", kFingerprint1));
  reject(line("1", "127.0.0.1:1", "127.00.0.1", kFingerprint1));
  reject(line("1", "127.0.0.1:1", "node.test", kFingerprint1.substr(1U)));
  reject(line("1", "127.0.0.1:1", "node.test",
              "000102030405060708090A0b0c0d0e0f101112131415161718191a1b1c1d1e1f"));
  reject(line("2", "127.0.0.1:2", "two.test", kFingerprint2) + "\n" +
         line("1", "127.0.0.1:1", "one.test", kFingerprint1));
  reject(line("1", "127.0.0.1:1", "one.test", kFingerprint1) + "\n" +
         line("2", "127.0.0.1:1", "two.test", kFingerprint2));
  reject(line("1", "127.0.0.1:1", "one.test", kFingerprint1) + "\n" +
         line("2", "127.0.0.1:2", "two.test", kFingerprint1));
  reject(line("1", "127.0.0.1:1", "one.test", kFingerprint1) + "\n\n");
  reject(line("1", "127.0.0.1:1", "one.test", kFingerprint1) + "\r\n");

  auto count_limited = parse_replicated_peer_config(
      std::string{prefix} + line("1", "127.0.0.1:1", "one.test", kFingerprint1) + "\n" +
          line("2", "127.0.0.1:2", "two.test", kFingerprint2),
      {.maximum_nodes = 1U});
  ASSERT_FALSE(count_limited.has_value());
  EXPECT_EQ(count_limited.error().code(), common::StatusCode::kResourceExhausted);
  auto identity_limited = parse_replicated_peer_config(
      std::string{prefix} + line("1", "127.0.0.1:1", "one.test", kFingerprint1),
      {.maximum_tls_identity_bytes = 3U});
  ASSERT_FALSE(identity_limited.has_value());
  EXPECT_EQ(identity_limited.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::service
