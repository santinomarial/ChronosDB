#include "chronos/cluster/distributed_grouped_query_tcp_client.hpp"
#include "chronos/cluster/distributed_grouped_query_tcp_server.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <poll.h>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

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
    query::MergeableAggregateState first;
    query::MergeableAggregateState second;
    if (const common::Status added = first.add(2.5); !added.is_ok())
      return common::make_unexpected(added);
    if (const common::Status added = second.add(7.5); !added.is_ok())
      return common::make_unexpected(added);
    return query::DistributedGroupedFloat64WorkerResult{
        std::vector<query::GroupedFloat64ExchangeMessage>{
            {.query_id = received.fragment.aggregate.query_id,
             .tablet_id = received.fragment.aggregate.tablet_id,
             .sequence = 1U,
             .group_key = 2.5,
             .partial = first,
             .terminal = false},
            {.query_id = received.fragment.aggregate.query_id,
             .tablet_id = received.fragment.aggregate.tablet_id,
             .sequence = 2U,
             .group_key = 7.5,
             .partial = second,
             .terminal = true}}};
  }

  std::size_t calls{};
};

[[nodiscard]] DistributedGroupedQueryTcpServerConfig
server_config(Authenticator& authenticator, DistributedGroupedQueryReceiver& receiver) {
  return {.listener = {},
          .tls = server_tls(),
          .authenticator = &authenticator,
          .receiver = &receiver,
          .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000},
                             .maximum_response_frames = 4U},
          .maximum_connections = 8U,
          .maximum_accepts_per_poll = 8U};
}

TEST(DistributedGroupedQueryTcpClientTest, OwnsConnectAndCompleteMutualTlsStream) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedGroupedQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  auto listener = network::TcpListener::bind();
  auto server_context = network::TlsServerContext::create(server_tls());
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(receiver.has_value());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());

  auto request = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()});
  ASSERT_TRUE(request.has_value());
  Authenticator server_authenticator{92U};
  const auto start = DistributedGroupedQueryTcpClient::TimePoint::clock::now();
  auto client = DistributedGroupedQueryTcpClient::begin(
      {1U, 2U, std::move(*request)},
      {.remote_endpoint = listener->bound_endpoint(),
       .tls_context = &*client_context,
       .carrier = {.authenticator = &server_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000},
                              .maximum_response_frames = 4U}},
       .connect_timeout = std::chrono::milliseconds{1000}},
      start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  EXPECT_FALSE(client->responses().has_value());

  Authenticator client_authenticator{91U};
  std::optional<network::TcpSocket> accepted_socket;
  std::optional<DistributedGroupedQueryTlsServer> server;
  for (std::size_t iteration = 0U; iteration < 2048U; ++iteration) {
    if (!accepted_socket.has_value()) {
      auto accepted = listener->accept_one();
      ASSERT_TRUE(accepted.has_value());
      if (accepted->has_value()) {
        accepted_socket.emplace(std::move(**accepted));
        auto tls = network::TlsSocket::accept(*server_context, accepted_socket->descriptor());
        ASSERT_TRUE(tls.has_value());
        auto carrier = DistributedGroupedQueryTlsServer::create(
            std::move(*tls),
            {.authenticator = &client_authenticator,
             .receiver = &*receiver,
             .peer_ipv4_address = accepted_socket->peer_endpoint().value().address,
             .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                        .exchange_timeout = std::chrono::milliseconds{1000},
                        .maximum_response_frames = 4U}},
            DistributedGroupedQueryTlsServer::TimePoint::clock::now());
        ASSERT_TRUE(carrier.has_value());
        server.emplace(std::move(*carrier));
      }
    }

    pollfd descriptors[2]{};
    std::size_t count = 0U;
    const auto client_interest = client->interest();
    descriptors[count++] = {.fd = client->descriptor(),
                            .events =
                                static_cast<short>((client_interest.want_read ? POLLIN : 0) |
                                                   (client_interest.want_write ? POLLOUT : 0))};
    if (server.has_value()) {
      const auto server_interest = server->interest();
      descriptors[count++] = {.fd = accepted_socket->descriptor(),
                              .events =
                                  static_cast<short>((server_interest.want_read ? POLLIN : 0) |
                                                     (server_interest.want_write ? POLLOUT : 0))};
    }
    ASSERT_GE(::poll(descriptors, static_cast<nfds_t>(count), 1), 0);
    const auto now = DistributedGroupedQueryTcpClient::TimePoint::clock::now();
    ASSERT_TRUE(client
                    ->on_ready((descriptors[0].revents & POLLIN) != 0,
                               (descriptors[0].revents & POLLOUT) != 0, now)
                    .is_ok())
        << client->failure().to_string();
    if (server.has_value()) {
      ASSERT_TRUE(server
                      ->on_ready((descriptors[1].revents & POLLIN) != 0,
                                 (descriptors[1].revents & POLLOUT) != 0, now)
                      .is_ok())
          << server->failure().to_string();
    }
    if (client->state() == DistributedGroupedQueryTcpClientState::kComplete && server.has_value() &&
        server->state() == DistributedGroupedQueryTlsState::kComplete) {
      break;
    }
  }

  ASSERT_EQ(client->state(), DistributedGroupedQueryTcpClientState::kComplete);
  ASSERT_TRUE(server.has_value());
  ASSERT_EQ(server->state(), DistributedGroupedQueryTlsState::kComplete);
  ASSERT_TRUE(client_authenticator.saw_fingerprint);
  ASSERT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(worker.calls, 1U);
  auto responses = client->responses();
  ASSERT_TRUE(responses.has_value());
  ASSERT_EQ(responses->size(), 2U);
  EXPECT_EQ(std::get<query::GroupedFloat64ExchangeMessage>(*(*responses)[0].payload).partial.sum,
            2.5);
  EXPECT_EQ(std::get<query::GroupedFloat64ExchangeMessage>(*(*responses)[1].payload).partial.sum,
            7.5);
}

TEST(DistributedGroupedQueryTcpClientTest, ValidatesBeforeConnectAndExpiresExactly) {
  Authorizer authorizer;
  Authenticator authenticator{92U};
  auto listener = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(tls_context.has_value());
  auto request = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()});
  ASSERT_TRUE(request.has_value());
  const auto start = DistributedGroupedQueryTcpClient::TimePoint{};
  auto config = DistributedGroupedQueryTcpClientConfig{
      .remote_endpoint = listener->bound_endpoint(),
      .tls_context = &*tls_context,
      .carrier = {.authenticator = &authenticator,
                  .node_authorizer = &authorizer,
                  .peer_ipv4_address = {127U, 0U, 0U, 1U},
                  .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                             .exchange_timeout = std::chrono::milliseconds{100},
                             .maximum_response_frames = 2U}},
      .connect_timeout = std::chrono::milliseconds{5}};
  auto client =
      DistributedGroupedQueryTcpClient::begin({1U, 2U, std::move(*request)}, config, start);
  ASSERT_TRUE(client.has_value());
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const auto expired = client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(expired.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), DistributedGroupedQueryTcpClientState::kFailed);
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), expired);

  config.carrier.limits.maximum_response_frames = 0U;
  auto second = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()});
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(DistributedGroupedQueryTcpClient::begin({1U, 2U, std::move(*second)}, config, start)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedGroupedQueryTcpServerTest, ServesRealTcpMutualTlsStream) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedGroupedQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  auto server =
      DistributedGroupedQueryTcpServer::start(server_config(client_authenticator, *receiver));
  ASSERT_TRUE(server.has_value()) << server.error().to_string();

  auto client_context = network::TlsClientContext::create(client_tls());
  auto request = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()});
  ASSERT_TRUE(client_context.has_value());
  ASSERT_TRUE(request.has_value());
  Authenticator server_authenticator{92U};
  const auto start = DistributedGroupedQueryTcpClient::TimePoint::clock::now();
  auto client = DistributedGroupedQueryTcpClient::begin(
      {1U, 2U, std::move(*request)},
      {.remote_endpoint = server->bound_endpoint(),
       .tls_context = &*client_context,
       .carrier = {.authenticator = &server_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000},
                              .maximum_response_frames = 4U}},
       .connect_timeout = std::chrono::milliseconds{1000}},
      start);
  ASSERT_TRUE(client.has_value());

  for (std::size_t iteration = 0U; iteration < 2048U; ++iteration) {
    const auto interest = client->interest();
    pollfd descriptor{.fd = client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(client
                    ->on_ready((descriptor.revents & POLLIN) != 0,
                               (descriptor.revents & POLLOUT) != 0,
                               DistributedGroupedQueryTcpClient::TimePoint::clock::now())
                    .is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    if (client->state() == DistributedGroupedQueryTcpClientState::kComplete)
      break;
  }

  ASSERT_EQ(client->state(), DistributedGroupedQueryTcpClientState::kComplete);
  auto responses = client->responses();
  ASSERT_TRUE(responses.has_value());
  ASSERT_EQ(responses->size(), 2U);
  EXPECT_EQ(std::get<query::GroupedFloat64ExchangeMessage>(*(*responses)[0].payload).partial.sum,
            2.5);
  EXPECT_EQ(std::get<query::GroupedFloat64ExchangeMessage>(*(*responses)[1].payload).partial.sum,
            7.5);
  EXPECT_EQ(worker.calls, 1U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  const auto metrics = server->metrics();
  EXPECT_EQ(metrics.accepted_connections, 1U);
  EXPECT_EQ(metrics.completed_connections, 1U);
  EXPECT_EQ(metrics.active_connections, 0U);
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_FALSE(server->is_running());
}

TEST(DistributedGroupedQueryTcpServerTest, BoundsAdmissionAndValidatesConfiguration) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedGroupedQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator authenticator{91U};
  auto invalid = server_config(authenticator, *receiver);
  invalid.maximum_connections = 0U;
  EXPECT_EQ(DistributedGroupedQueryTcpServer::start(std::move(invalid)).error().code(),
            common::StatusCode::kInvalidArgument);

  auto config = server_config(authenticator, *receiver);
  config.maximum_connections = 1U;
  auto server = DistributedGroupedQueryTcpServer::start(std::move(config));
  ASSERT_TRUE(server.has_value());
  auto first = network::TcpSocket::begin_connect(server->bound_endpoint());
  auto second = network::TcpSocket::begin_connect(server->bound_endpoint());
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  for (std::size_t iteration = 0U; iteration < 64U && server->metrics().rejected_connections == 0U;
       ++iteration) {
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
    for (network::TcpSocket* socket : {&*first, &*second}) {
      if (socket->valid() && socket->connect_state() == network::TcpConnectState::kInProgress) {
        pollfd descriptor{.fd = socket->descriptor(), .events = POLLOUT};
        if (::poll(&descriptor, 1U, 0) > 0)
          (void)socket->finish_connect();
      }
    }
  }
  const auto metrics = server->metrics();
  EXPECT_EQ(metrics.accepted_connections, 1U);
  EXPECT_EQ(metrics.rejected_connections, 1U);
  EXPECT_EQ(metrics.active_connections, 1U);
  EXPECT_TRUE(server->shutdown().is_ok());
  EXPECT_EQ(server->metrics().active_connections, 0U);
}

} // namespace
} // namespace chronos::cluster
