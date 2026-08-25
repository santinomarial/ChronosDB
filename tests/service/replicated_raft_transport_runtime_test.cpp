#include "chronos/network/tcp_socket.hpp"
#include "chronos/service/replicated_raft_transport_runtime.hpp"
#include "chronos/service/replicated_read_barrier.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <gtest/gtest.h>
#include <iterator>
#include <memory>
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

[[nodiscard]] std::string fixture_bytes(const char* name) {
  std::ifstream stream{fixture(name), std::ios::binary};
  return {std::istreambuf_iterator<char>{stream}, std::istreambuf_iterator<char>{}};
}

[[nodiscard]] std::shared_ptr<const network::TlsPemCredentials> server_pem_credentials() {
  return std::make_shared<const network::TlsPemCredentials>(
      network::TlsPemCredentials{.certificate_chain = fixture_bytes("server.pem"),
                                 .private_key = fixture_bytes("server-key.pem"),
                                 .trust_store = fixture_bytes("ca.pem")});
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
  auto moved_transport = std::move(*transport);
  EXPECT_FALSE(transport->is_running());
  EXPECT_EQ(transport->poll_once(std::chrono::milliseconds{0}).code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(transport->take_completed().error().code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(transport->shutdown().is_ok());
  EXPECT_TRUE(moved_transport.is_running());
  EXPECT_EQ(moved_transport.bound_endpoint(), local_endpoint);
  EXPECT_TRUE(moved_transport.poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_GE(moved_transport.metrics().polls, 1U);
  EXPECT_TRUE(moved_transport.shutdown().is_ok());
  EXPECT_FALSE(moved_transport.is_running());
  EXPECT_TRUE(durable->shutdown().is_ok());
}

TEST(ReplicatedRaftTransportRuntimeTest, DrivesTransportedSingleVoterReadBarrier) {
  TemporaryDirectory directory;
  auto remote_listener = network::TcpListener::bind({});
  auto local_reservation = network::TcpListener::bind({});
  if (!remote_listener.has_value() || !local_reservation.has_value())
    GTEST_SKIP() << "workspace does not permit loopback listener creation";
  const network::Ipv4Endpoint local_endpoint = local_reservation->bound_endpoint();
  ASSERT_TRUE(local_reservation->close().is_ok());
  auto durable = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group(), {1U}}});
  ASSERT_TRUE(durable.has_value()) << durable.error().to_string();
  auto election = durable->try_submit({{group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
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
  auto barrier = ReplicatedReadBarrier::create_transported(
      {group()}, {.maximum_groups = 1U, .request_timeout = std::chrono::seconds{2}});
  ASSERT_TRUE(barrier.has_value()) << barrier.error().to_string();
  auto waiting =
      std::async(std::launch::async, [&] { return barrier->await_group_authority(group()); });
  for (std::size_t iteration = 0U;
       iteration < 1024U &&
       waiting.wait_for(std::chrono::milliseconds{0}) != std::future_status::ready;
       ++iteration) {
    ASSERT_TRUE(barrier->poll_owner_drive(*transport).is_ok());
    ASSERT_TRUE(transport->poll_once(std::chrono::milliseconds{10}).is_ok());
    for (;;) {
      auto completed = transport->take_completed();
      if (!completed.has_value()) {
        ASSERT_EQ(completed.error().code(), common::StatusCode::kUnavailable);
        break;
      }
      ASSERT_TRUE(barrier->poll_owner_observe(*completed).is_ok());
    }
  }
  auto ready = waiting.get();
  ASSERT_TRUE(ready.has_value()) << ready.error().to_string();
  EXPECT_EQ(ready->barrier.group_id, group());
  EXPECT_EQ(ready->barrier.barrier.read_index, 1U);
  EXPECT_EQ(ready->observation.group_id, group());
  EXPECT_EQ(ready->observation.role, raft::Role::kLeader);
  EXPECT_EQ(ready->observation.current_term, ready->barrier.barrier.term);
  EXPECT_GE(ready->observation.commit_index, ready->barrier.barrier.read_index);
  EXPECT_TRUE(barrier->shutdown().is_ok());
  EXPECT_TRUE(transport->shutdown().is_ok());
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
            .tls = {.pem_credentials = server_pem_credentials()}};
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
