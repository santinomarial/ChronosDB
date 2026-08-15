#include "chronos/cluster/distributed_grouped_query_tls.hpp"

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
#include <vector>

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

[[nodiscard]] network::TlsServerConfig server_tls() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig client_tls() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] SocketPair socket_pair() {
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

[[nodiscard]] query::DistributedGroupedFloat64FragmentDispatch dispatch() {
  return {
      .raft_group_id = uuid(9U),
      .fragment = {
          .aggregate = {.query_id = uuid(1U),
                        .database_id = id<manifest::DatabaseId>(2U),
                        .table_id = id<schema::TableId>(3U),
                        .tablet_id = id<schema::TabletId>(4U),
                        .destination_schema_id = id<schema::SchemaId>(5U),
                        .snapshot_generation = 6U,
                        .serving_node = 2U,
                        .applied_position = 10U,
                        .observed_leader_commit_position = 10U,
                        .placement_epoch = 8U,
                        .read_policy = {.consistency =
                                            query::DistributedReadConsistency::kLeaderLinearizable,
                                        .maximum_staleness_positions = std::nullopt},
                        .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
                        .destination_column_ordinals = {1U},
                        .aggregate_input_index = 0U,
                        .event_time_predicate = std::nullopt},
          .group_key_input_index = 0U}};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

  bool saw_fingerprint{};

private:
  std::uint64_t principal_{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 91U && node == 1U) || (principal == 92U && node == 2U);
  }
};

class Worker final : public DistributedGroupedQueryWorkerService {
public:
  common::Result<query::DistributedGroupedFloat64WorkerResult>
  execute(const query::DistributedGroupedFloat64FragmentDispatch& received) override {
    ++calls;
    query::MergeableAggregateState first_state;
    query::MergeableAggregateState second_state;
    if (const common::Status status = first_state.add(2.5); !status.is_ok())
      return common::make_unexpected(status);
    if (const common::Status status = second_state.add(7.5); !status.is_ok())
      return common::make_unexpected(status);
    return query::DistributedGroupedFloat64WorkerResult{
        std::vector<query::GroupedFloat64ExchangeMessage>{
            {.query_id = received.fragment.aggregate.query_id,
             .tablet_id = received.fragment.aggregate.tablet_id,
             .sequence = 1U,
             .group_key = 2.5,
             .partial = first_state,
             .terminal = false},
            {.query_id = received.fragment.aggregate.query_id,
             .tablet_id = received.fragment.aggregate.tablet_id,
             .sequence = 2U,
             .group_key = 7.5,
             .partial = second_state,
             .terminal = true}}};
  }

  std::size_t calls{};
};

TEST(DistributedGroupedQueryTlsTest, CarriesCompleteAuthenticatedMultiResponseStream) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedGroupedQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  auto request = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()});
  ASSERT_TRUE(request.has_value());
  DistributedGroupedQueryAttempt attempt{1U, 2U, std::move(*request)};

  auto server_context = network::TlsServerContext::create(server_tls());
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  SocketPair sockets = socket_pair();
  auto server_socket = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  auto client_socket = network::TlsSocket::connect(*client_context, sockets.sockets[1]);
  ASSERT_TRUE(server_socket.has_value());
  ASSERT_TRUE(client_socket.has_value());

  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  const auto start = DistributedGroupedQueryTlsClient::TimePoint{};
  auto server = DistributedGroupedQueryTlsServer::create(
      std::move(*server_socket),
      {.authenticator = &client_authenticator,
       .receiver = &*receiver,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                  .exchange_timeout = std::chrono::milliseconds{100},
                  .maximum_response_frames = 4U}},
      start);
  auto client = DistributedGroupedQueryTlsClient::create(
      std::move(*client_socket), std::move(attempt),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                  .exchange_timeout = std::chrono::milliseconds{100},
                  .maximum_response_frames = 4U}},
      start);
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  EXPECT_FALSE(client->responses().has_value());

  for (std::size_t iteration = 0U; iteration < 2048U; ++iteration) {
    ASSERT_TRUE(client->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->on_ready(true, true, start + std::chrono::milliseconds{1}).is_ok())
        << server->failure().to_string();
    if (client->state() == DistributedGroupedQueryTlsState::kComplete &&
        server->state() == DistributedGroupedQueryTlsState::kComplete) {
      break;
    }
  }
  ASSERT_EQ(client->state(), DistributedGroupedQueryTlsState::kComplete);
  ASSERT_EQ(server->state(), DistributedGroupedQueryTlsState::kComplete);
  ASSERT_TRUE(client_authenticator.saw_fingerprint);
  ASSERT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(worker.calls, 1U);
  const auto responses = client->responses();
  ASSERT_TRUE(responses.has_value());
  ASSERT_EQ(responses->size(), 2U);
  const auto* first = (*responses)[0]
                          .payload
                          .transform([](const auto& payload) {
                            return std::get_if<query::GroupedFloat64ExchangeMessage>(&payload);
                          })
                          .value_or(nullptr);
  const auto* second = (*responses)[1]
                           .payload
                           .transform([](const auto& payload) {
                             return std::get_if<query::GroupedFloat64ExchangeMessage>(&payload);
                           })
                           .value_or(nullptr);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(first->sequence, 1U);
  EXPECT_FALSE(first->terminal);
  EXPECT_EQ(first->partial.sum, 2.5);
  EXPECT_EQ(second->sequence, 2U);
  EXPECT_TRUE(second->terminal);
  EXPECT_EQ(second->partial.sum, 7.5);
}

TEST(DistributedGroupedQueryTlsTest, RejectsInvalidLimitsAndExpiresExactly) {
  Authenticator authenticator{92U};
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedGroupedQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  auto request = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()}).value();
  DistributedGroupedQueryAttempt attempt{1U, 2U, std::move(request)};
  DistributedGroupedQueryTlsClientConfig config{
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .limits = {.handshake_timeout = std::chrono::milliseconds{5},
                 .exchange_timeout = std::chrono::milliseconds{5},
                 .maximum_response_frames = 2U}};
  auto client = DistributedGroupedQueryTlsClient::create(network::TlsSocket{}, std::move(attempt),
                                                         config, {});
  ASSERT_TRUE(client.has_value());
  const auto start = DistributedGroupedQueryTlsClient::TimePoint{};
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const auto expired = client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(expired.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), DistributedGroupedQueryTlsState::kFailed);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), expired);

  auto server = DistributedGroupedQueryTlsServer::create(
      network::TlsSocket{},
      {.authenticator = &authenticator,
       .receiver = &*receiver,
       .limits = {.handshake_timeout = std::chrono::milliseconds{5},
                  .exchange_timeout = std::chrono::milliseconds{5},
                  .maximum_response_frames = 2U}},
      start);
  ASSERT_TRUE(server.has_value());
  EXPECT_TRUE(server->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const auto server_expired = server->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(server_expired.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(server->state(), DistributedGroupedQueryTlsState::kFailed);
  EXPECT_EQ(server->on_ready(true, true, start + std::chrono::milliseconds{6}), server_expired);

  config.limits.maximum_response_frames = 0U;
  auto second = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()}).value();
  EXPECT_EQ(DistributedGroupedQueryTlsClient::create(network::TlsSocket{},
                                                     {1U, 2U, std::move(second)}, config, {})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
