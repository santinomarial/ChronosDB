#include "chronos/network/tcp_socket.hpp"
#include "chronos/service/replicated_raft_transport_runtime.hpp"

#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>

namespace chronos::service {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-replicated-transport-XXXXXX").string();
    if (char* created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] raft::GroupId group() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{42U});
  return raft::GroupId{bytes};
}

[[nodiscard]] ReplicatedPeer peer(const raft::NodeId node_id, const network::Ipv4Endpoint endpoint,
                                  const std::uint8_t fingerprint) {
  ReplicatedPeer value{
      .node_id = node_id, .endpoint = endpoint, .tls_server_identity = "127.0.0.1"};
  value.certificate_sha256.fill(fingerprint);
  return value;
}

TEST(ReplicatedRaftTransportRuntimeTest, OwnsAuthenticatedTransportAndDurableTimerObservations) {
  TemporaryDirectory directory;
  auto remote_listener = network::TcpListener::bind({});
  auto local_reservation = network::TcpListener::bind({});
  if (!remote_listener.has_value() || !local_reservation.has_value())
    GTEST_SKIP() << "workspace does not permit loopback listener creation";
  const network::Ipv4Endpoint local_endpoint = local_reservation->bound_endpoint();
  ASSERT_TRUE(local_reservation->close().is_ok());
  auto durable = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group(), {1U, 2U}}});
  ASSERT_TRUE(durable.has_value()) << durable.error().to_string();
  auto transport = ReplicatedRaftTransportRuntime::create(
      {.local_node_id = 1U,
       .durable_runtime = &*durable,
       .peers = {peer(1U, local_endpoint, 11U), peer(2U, remote_listener->bound_endpoint(), 22U)},
       .resident_groups = {group()},
       .tls = {.certificate_chain_file = fixture("server.pem").string(),
               .private_key_file = fixture("server-key.pem").string(),
               .trust_store_file = fixture("ca.pem").string()},
       .limits = {.minimum_election_timeout = std::chrono::milliseconds{1000},
                  .maximum_election_timeout = std::chrono::milliseconds{1000},
                  .peer_pool = {.maximum_peers = 1U}}});
  ASSERT_TRUE(transport.has_value()) << transport.error().to_string();
  EXPECT_TRUE(transport->is_running());
  EXPECT_EQ(transport->bound_endpoint(), local_endpoint);
  EXPECT_TRUE(transport->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_GE(transport->metrics().polls, 1U);
  EXPECT_TRUE(transport->shutdown().is_ok());
  EXPECT_FALSE(transport->is_running());
  EXPECT_TRUE(durable->shutdown().is_ok());
}

TEST(ReplicatedRaftTransportRuntimeTest, RejectsMissingRemoteAndDuplicateGroups) {
  TemporaryDirectory directory;
  auto durable = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group(), {1U}}});
  ASSERT_TRUE(durable.has_value());
  ReplicatedRaftTransportRuntimeConfig config{
      .local_node_id = 1U,
      .durable_runtime = &*durable,
      .peers = {peer(1U, {{127U, 0U, 0U, 1U}, 7001U}, 11U)},
      .resident_groups = {group()},
      .tls = {.certificate_chain_file = fixture("server.pem").string(),
              .private_key_file = fixture("server-key.pem").string(),
              .trust_store_file = fixture("ca.pem").string()}};
  EXPECT_FALSE(ReplicatedRaftTransportRuntime::create(std::move(config)).has_value());

  config = {.local_node_id = 1U,
            .durable_runtime = &*durable,
            .peers = {peer(1U, {{127U, 0U, 0U, 1U}, 7001U}, 11U),
                      peer(2U, {{127U, 0U, 0U, 1U}, 7002U}, 22U)},
            .resident_groups = {group(), group()},
            .tls = {.certificate_chain_file = fixture("server.pem").string(),
                    .private_key_file = fixture("server-key.pem").string(),
                    .trust_store_file = fixture("ca.pem").string()}};
  auto duplicate = ReplicatedRaftTransportRuntime::create(std::move(config));
  ASSERT_FALSE(duplicate.has_value());
  EXPECT_EQ(duplicate.error().code(), common::StatusCode::kAlreadyExists);

  config = {.local_node_id = 1U,
            .durable_runtime = &*durable,
            .peers = {peer(1U, {{127U, 0U, 0U, 1U}, 7001U}, 11U),
                      peer(2U, {{127U, 0U, 0U, 1U}, 7002U}, 22U)},
            .resident_groups = {group()},
            .tls = {.certificate_chain_file = fixture("server.pem").string(),
                    .private_key_file = fixture("server-key.pem").string(),
                    .trust_store_file = fixture("ca.pem").string()},
            .limits = {.minimum_election_timeout = std::chrono::milliseconds{100},
                       .maximum_election_timeout = std::chrono::milliseconds{100}}};
  auto unsafe_timeout = ReplicatedRaftTransportRuntime::create(std::move(config));
  ASSERT_FALSE(unsafe_timeout.has_value());
  EXPECT_EQ(unsafe_timeout.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(durable->shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
