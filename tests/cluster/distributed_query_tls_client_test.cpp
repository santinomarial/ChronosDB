#include "chronos/cluster/distributed_query_tls_client.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

namespace chronos::cluster {
namespace {

struct SocketPair {
  std::array<int, 2> sockets{-1, -1};
  ~SocketPair() {
    for (const int socket : sockets) {
      if (socket >= 0)
        ::close(socket);
    }
  }
};

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig server_config() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig client_config() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] SocketPair nonblocking_socket_pair() {
  SocketPair pair;
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair.sockets.data()), 0);
  for (const int socket : pair.sockets) {
    const int flags = ::fcntl(socket, F_GETFL, 0);
    EXPECT_GE(flags, 0);
    EXPECT_EQ(::fcntl(socket, F_SETFL, flags | O_NONBLOCK), 0);
  }
  return pair;
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] query::DistributedAggregateFragmentDispatch dispatch() {
  return {.raft_group_id = uuid(9U),
          .fragment = {
              .query_id = uuid(1U),
              .database_id = id<manifest::DatabaseId>(2U),
              .table_id = id<schema::TableId>(3U),
              .tablet_id = id<schema::TabletId>(4U),
              .destination_schema_id = id<schema::SchemaId>(5U),
              .snapshot_generation = 6U,
              .serving_node = 2U,
              .applied_position = 10U,
              .observed_leader_commit_position = 10U,
              .placement_epoch = 8U,
              .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable,
                              .maximum_staleness_positions = std::nullopt},
              .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
              .destination_column_ordinals = {1U},
              .aggregate_input_index = 0U,
              .event_time_predicate = std::nullopt}};
}

[[nodiscard]] query::ExchangeMessage message() {
  query::MergeableAggregateState partial;
  EXPECT_TRUE(partial.add(2.5).is_ok());
  return {.query_id = uuid(1U),
          .tablet_id = id<schema::TabletId>(4U),
          .sequence = 1U,
          .partial = partial,
          .terminal = true};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 91U};
  }

  bool saw_fingerprint{};
};

class NodeAuthorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId claimed_node_id) const override {
    return allow && principal_id == 91U && claimed_node_id == 2U;
  }

  bool allow{true};
};

[[nodiscard]] DistributedQueryTlsClientConfig carrier_config(Authenticator& authenticator,
                                                             NodeAuthorizer& authorizer) {
  return {.authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .peer_ipv4_address = {127U, 0U, 0U, 1U},
          .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                     .exchange_timeout = std::chrono::milliseconds{100}}};
}

TEST(DistributedQueryTlsClientTest, AuthenticatesWritesReadsAndFeedsExactSenderResponse) {
  auto sender = DistributedQuerySender::create(1U, dispatch());
  ASSERT_TRUE(sender.has_value());
  const auto start = DistributedQuerySender::TimePoint{};
  auto attempt = sender->begin_attempt(start);
  ASSERT_TRUE(attempt.has_value());

  auto server_context = network::TlsServerContext::create(server_config());
  auto client_context_owner = network::TlsClientContext::create(client_config());
  ASSERT_TRUE(server_context.has_value()) << server_context.error().message();
  ASSERT_TRUE(client_context_owner.has_value()) << client_context_owner.error().message();
  SocketPair sockets = nonblocking_socket_pair();
  auto server = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context_owner, sockets.sockets[1]);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(client_socket.has_value());

  Authenticator authenticator;
  NodeAuthorizer authorizer;
  auto carrier =
      DistributedQueryTlsClient::create(std::move(*client_socket), std::move(*attempt),
                                        carrier_config(authenticator, authorizer), start);
  ASSERT_TRUE(carrier.has_value()) << carrier.error().message();
  EXPECT_TRUE(carrier->interest().want_write);
  EXPECT_FALSE(carrier->response_bytes().has_value());

  DistributedQueryRequestReader request_reader;
  std::array<std::byte, kMaximumDistributedQueryRequestSize> request_scratch{};
  std::optional<DistributedQueryFrameWriteCursor> response_writer;
  for (std::size_t iteration = 0U; iteration < 1024U; ++iteration) {
    ASSERT_TRUE(carrier->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << carrier->failure().message();
    if (!server->handshake_complete()) {
      auto progress = server->handshake();
      ASSERT_TRUE(progress.has_value()) << progress.error().message();
    } else if (!response_writer.has_value()) {
      auto read = server->read(request_scratch);
      ASSERT_TRUE(read.has_value()) << read.error().message();
      if (read->state == network::TlsIoState::kComplete) {
        auto step = request_reader.consume(
            common::ByteView{request_scratch}.first(read->bytes_transferred));
        ASSERT_TRUE(step.has_value()) << step.error().message();
        if (step->request.has_value()) {
          auto encoded = encode_distributed_query_response_v1(
              {.source_node_id = 2U,
               .target_node_id = 1U,
               .query_id = step->request->dispatch.fragment.query_id,
               .tablet_id = step->request->dispatch.fragment.tablet_id,
               .status_code = common::StatusCode::kOk,
               .message = message()});
          ASSERT_TRUE(encoded.has_value());
          auto cursor = DistributedQueryFrameWriteCursor::create(std::move(*encoded));
          ASSERT_TRUE(cursor.has_value());
          response_writer.emplace(std::move(*cursor));
        }
      }
    } else if (!response_writer->complete()) {
      auto write = server->write(response_writer->pending_write());
      ASSERT_TRUE(write.has_value()) << write.error().message();
      if (write->state == network::TlsIoState::kComplete)
        ASSERT_TRUE(response_writer->consume_written(write->bytes_transferred).is_ok());
    }
    if (carrier->state() == DistributedQueryTlsClientState::kComplete)
      break;
  }

  ASSERT_EQ(carrier->state(), DistributedQueryTlsClientState::kComplete);
  EXPECT_TRUE(authenticator.saw_fingerprint);
  const auto response = carrier->response_bytes();
  ASSERT_TRUE(response.has_value());
  ASSERT_TRUE(sender->accept_response(*response, start + std::chrono::milliseconds{2}).is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kSucceeded);
  ASSERT_TRUE(sender->result().has_value());
  EXPECT_EQ(sender->result()->partial.sum, 2.5);
}

TEST(DistributedQueryTlsClientTest, RejectsServerPrincipalBeforeWritingRequest) {
  auto sender = DistributedQuerySender::create(1U, dispatch());
  auto attempt = sender->begin_attempt(DistributedQuerySender::TimePoint{});
  ASSERT_TRUE(attempt.has_value());
  auto server_context = network::TlsServerContext::create(server_config());
  auto client_context_owner = network::TlsClientContext::create(client_config());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context_owner.has_value());
  SocketPair sockets = nonblocking_socket_pair();
  auto server = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context_owner, sockets.sockets[1]);
  ASSERT_TRUE(server.has_value());
  ASSERT_TRUE(client_socket.has_value());
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  authorizer.allow = false;
  const auto start = DistributedQuerySender::TimePoint{};
  auto carrier =
      DistributedQueryTlsClient::create(std::move(*client_socket), std::move(*attempt),
                                        carrier_config(authenticator, authorizer), start);
  ASSERT_TRUE(carrier.has_value());

  common::Status progress = common::Status::ok();
  for (std::size_t iteration = 0U; iteration < 1024U && progress.is_ok(); ++iteration) {
    progress = carrier->on_ready(true, true, start + std::chrono::milliseconds{1});
    if (!server->handshake_complete())
      (void)server->handshake();
  }
  EXPECT_EQ(progress.code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(carrier->state(), DistributedQueryTlsClientState::kFailed);
  EXPECT_TRUE(authenticator.saw_fingerprint);
  EXPECT_EQ(carrier->failure().code(), common::StatusCode::kUnauthenticated);
}

TEST(DistributedQueryTlsClientTest, DeadlinesAndAttemptBindingFailClosed) {
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  auto sender = DistributedQuerySender::create(1U, dispatch());
  const auto start = DistributedQuerySender::TimePoint{};
  auto attempt = sender->begin_attempt(start);
  ASSERT_TRUE(attempt.has_value());
  DistributedQueryAttempt inconsistent = *attempt;
  inconsistent.target_node_id = 3U;
  EXPECT_EQ(DistributedQueryTlsClient::create(network::TlsSocket{}, std::move(inconsistent),
                                              carrier_config(authenticator, authorizer), start)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto config = carrier_config(authenticator, authorizer);
  config.limits.handshake_timeout = std::chrono::milliseconds{5};
  auto carrier =
      DistributedQueryTlsClient::create(network::TlsSocket{}, std::move(*attempt), config, start);
  ASSERT_TRUE(carrier.has_value());
  EXPECT_TRUE(carrier->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const common::Status timed_out =
      carrier->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(timed_out.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(carrier->state(), DistributedQueryTlsClientState::kFailed);
  EXPECT_EQ(carrier->on_ready(true, true, start + std::chrono::milliseconds{6}), timed_out);
}

} // namespace
} // namespace chronos::cluster
