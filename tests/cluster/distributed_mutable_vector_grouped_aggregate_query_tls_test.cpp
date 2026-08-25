#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tls.hpp"

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
  SocketPair() = default;
  SocketPair(const SocketPair&) = delete;
  SocketPair& operator=(const SocketPair&) = delete;
  SocketPair(SocketPair&& other) noexcept : sockets(other.sockets) {
    other.sockets = {-1, -1};
  }
  SocketPair& operator=(SocketPair&& other) noexcept {
    if (this != &other) {
      for (const int socket : sockets) {
        if (socket >= 0)
          ::close(socket);
      }
      sockets = other.sockets;
      other.sockets = {-1, -1};
    }
    return *this;
  }
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
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment() {
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {
      .query_id = uuid(1U),
      .database_id = id<manifest::DatabaseId>(2U),
      .table_id = id<schema::TableId>(3U),
      .tablet_id = id<schema::TabletId>(4U),
      .destination_schema_id = id<schema::SchemaId>(5U),
      .raft_group_id = uuid(9U),
      .serving_node = 2U,
      .applied_position = 10U,
      .observed_leader_commit_position = 10U,
      .placement_epoch = 8U,
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
      .destination_column_ordinals = {0U},
      .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
               .group_key_input_indices = {0U},
               .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}},
      .result_schema = {.columns = {{"region", string_type(), false}, {"count", int64, false}}}};
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
    return common::Result<bool>{(principal == 91U && node == 1U) ||
                                (allow_server && principal == 92U && node == 2U)};
  }

  bool allow_server{true};
};

class Worker final : public DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedMutableVectorFragment&) override {
    ++bind_calls;
    return query::DistributedVectorGroupedAggregateAuthority{keys(), aggregates()};
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedMutableVectorFragment& received) override {
    ++execute_calls;
    query::DistributedVectorGroupedAggregateWorkerResultV2 result{
        .authority = {.keys = keys(), .aggregates = aggregates()},
        .input_rows = 3U,
        .group_count = empty ? 0U : 2U};
    if (empty) {
      query::DistributedVectorGroupedAggregateExchangeMessage message{
          {.query_id = received.query_id,
           .tablet_id = received.tablet_id,
           .sequence = 1U,
           .group_ordinal = 0U,
           .group_count = 0U,
           .terminal = true,
           .empty = true},
          {},
          {}};
      auto encoded = query::encode_distributed_vector_grouped_aggregate_exchange_message(
          message, result.authority.keys, result.authority.aggregates);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      result.encoded_bytes = encoded->bytes().size();
      result.messages.push_back(std::move(*encoded));
      return result;
    }
    for (std::size_t ordinal = 0U; ordinal < 2U; ++ordinal) {
      auto state =
          query::MergeableVectorAggregateState::create(result.authority.aggregates.front()).value();
      for (std::size_t count = 0U; count <= ordinal; ++count)
        EXPECT_TRUE(state.accumulate_count_star().has_value());
      std::vector<query::ScalarValue> key_values;
      key_values.push_back(
          query::ScalarValue::text(string_type(), ordinal == 0U ? "east" : "west").value());
      std::vector<query::MergeableVectorAggregateState> states;
      states.push_back(std::move(state));
      query::DistributedVectorGroupedAggregateExchangeMessage message{
          query::DistributedVectorGroupedAggregateExchangePosition{
              .query_id = received.query_id,
              .tablet_id = received.tablet_id,
              .sequence = ordinal + 1U,
              .group_ordinal = static_cast<std::uint32_t>(ordinal),
              .group_count = 2U,
              .terminal = ordinal == 1U,
              .empty = false},
          std::move(key_values), std::move(states)};
      auto encoded = query::encode_distributed_vector_grouped_aggregate_exchange_message(
          message, result.authority.keys, result.authority.aggregates);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      result.encoded_bytes += encoded->bytes().size();
      result.messages.push_back(std::move(*encoded));
    }
    return result;
  }

  std::size_t bind_calls{};
  std::size_t execute_calls{};
  bool empty{};
};

struct Carriers {
  network::TlsServerContext server_context;
  network::TlsClientContext client_context;
  SocketPair sockets;
  DistributedMutableVectorGroupedAggregateQueryTlsServer server;
  DistributedMutableVectorGroupedAggregateQueryTlsClient client;
};

struct CarrierSetup {
  DistributedMutableVectorGroupedAggregateQueryReceiver* receiver{};
  const Authorizer* authorizer{};
  Authenticator* client_authenticator{};
  Authenticator* server_authenticator{};
  DistributedMutableVectorGroupedAggregateQueryTlsLimits server_limits;
  DistributedMutableVectorGroupedAggregateQueryTlsLimits client_limits;
};

[[nodiscard]] common::Result<Carriers> create_carriers(const CarrierSetup& setup,
                                                       query::QueryResourceContext resources) {
  auto request = encode_distributed_mutable_vector_query_request({1U, 2U, fragment()});
  if (!request.has_value())
    return common::make_unexpected(request.error());
  auto server_context = network::TlsServerContext::create(server_tls());
  if (!server_context.has_value())
    return common::make_unexpected(server_context.error());
  auto client_context = network::TlsClientContext::create(client_tls());
  if (!client_context.has_value())
    return common::make_unexpected(client_context.error());
  SocketPair sockets = socket_pair();
  auto server_socket = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  if (!server_socket.has_value())
    return common::make_unexpected(server_socket.error());
  auto client_socket = network::TlsSocket::connect(*client_context, sockets.sockets[1]);
  if (!client_socket.has_value())
    return common::make_unexpected(client_socket.error());
  const auto start = DistributedMutableVectorGroupedAggregateQueryTlsClient::TimePoint{};
  auto server = DistributedMutableVectorGroupedAggregateQueryTlsServer::create(
      std::move(*server_socket),
      {.authenticator = setup.client_authenticator,
       .receiver = setup.receiver,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .limits = setup.server_limits},
      start);
  if (!server.has_value())
    return common::make_unexpected(server.error());
  auto expected_keys = keys();
  auto expected_aggregates = aggregates();
  auto client = DistributedMutableVectorGroupedAggregateQueryTlsClient::create(
      std::move(*client_socket), {1U, 2U, std::move(*request)}, std::move(expected_keys),
      std::move(expected_aggregates), std::move(resources),
      {.authenticator = setup.server_authenticator,
       .node_authorizer = setup.authorizer,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .limits = setup.client_limits},
      start);
  if (!client.has_value())
    return common::make_unexpected(client.error());
  return Carriers{std::move(*server_context), std::move(*client_context), std::move(sockets),
                  std::move(*server), std::move(*client)};
}

// Optional values below are asserted present before access.
// NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST(DistributedMutableVectorGroupedAggregateQueryTlsTest,
     CarriesCompleteAuthenticatedGroupedStateVector) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  const DistributedMutableVectorGroupedAggregateQueryTlsLimits limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100},
      .maximum_response_frames = 2U,
      .maximum_response_bytes = std::size_t{1024U} * 1024U};
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  auto carriers = create_carriers({.receiver = &*receiver,
                                   .authorizer = &authorizer,
                                   .client_authenticator = &client_authenticator,
                                   .server_authenticator = &server_authenticator,
                                   .server_limits = limits,
                                   .client_limits = limits},
                                  resources);
  ASSERT_TRUE(carriers.has_value()) << carriers.error().to_string();
  EXPECT_FALSE(carriers->client.responses().has_value());
  const auto progress_time = DistributedMutableVectorGroupedAggregateQueryTlsClient::TimePoint{} +
                             std::chrono::milliseconds{1};
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    ASSERT_TRUE(carriers->client.on_ready(true, true, progress_time).is_ok())
        << carriers->client.failure().to_string();
    ASSERT_TRUE(carriers->server.on_ready(true, true, progress_time).is_ok())
        << carriers->server.failure().to_string();
    if (carriers->client.state() ==
            DistributedMutableVectorGroupedAggregateQueryTlsState::kComplete &&
        carriers->server.state() ==
            DistributedMutableVectorGroupedAggregateQueryTlsState::kComplete) {
      break;
    }
  }
  ASSERT_EQ(carriers->client.state(),
            DistributedMutableVectorGroupedAggregateQueryTlsState::kComplete);
  ASSERT_EQ(carriers->server.state(),
            DistributedMutableVectorGroupedAggregateQueryTlsState::kComplete);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(worker.bind_calls, 1U);
  EXPECT_EQ(worker.execute_calls, 1U);
  auto responses = carriers->client.responses();
  ASSERT_TRUE(responses.has_value());
  ASSERT_EQ(responses->size(), 2U);
  for (std::size_t ordinal = 0U; ordinal < responses->size(); ++ordinal) {
    ASSERT_TRUE((*responses)[ordinal].payload.has_value());
    const auto& position = (*responses)[ordinal].payload->position();
    EXPECT_EQ(position.group_ordinal, ordinal);
    EXPECT_EQ(position.group_count, 2U);
    EXPECT_EQ(position.sequence, ordinal + 1U);
    EXPECT_EQ(position.terminal, ordinal + 1U == responses->size());
    ASSERT_EQ((*responses)[ordinal].payload->states().size(), 1U);
    EXPECT_EQ((*responses)[ordinal].payload->states().front().definition(), aggregates().front());
  }
}

TEST(DistributedMutableVectorGroupedAggregateQueryTlsTest,
     RejectsInvalidAuthorityBoundsTargetAndExpiresExactly) {
  Authenticator authenticator{92U};
  Authorizer authorizer;
  auto request = encode_distributed_mutable_vector_query_request({1U, 2U, fragment()}).value();
  auto expected_keys = keys();
  auto expected_aggregates = aggregates();
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  DistributedMutableVectorGroupedAggregateQueryTlsClientConfig config{
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .limits = {.handshake_timeout = std::chrono::milliseconds{5},
                 .exchange_timeout = std::chrono::milliseconds{5},
                 .maximum_response_frames = 2U,
                 .maximum_response_bytes = 1024U}};
  auto client = DistributedMutableVectorGroupedAggregateQueryTlsClient::create(
      network::TlsSocket{}, {1U, 2U, std::move(request)}, std::move(expected_keys),
      std::move(expected_aggregates), resources, config, {});
  ASSERT_TRUE(client.has_value());
  const auto start = DistributedMutableVectorGroupedAggregateQueryTlsClient::TimePoint{};
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const common::Status expired =
      client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(expired.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), DistributedMutableVectorGroupedAggregateQueryTlsState::kFailed);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), expired);

  auto narrow_request =
      encode_distributed_mutable_vector_query_request({1U, 2U, fragment()}).value();
  auto narrow_keys = keys();
  auto narrow_aggregates = aggregates();
  config.limits.maximum_response_frames = 0U;
  EXPECT_EQ(DistributedMutableVectorGroupedAggregateQueryTlsClient::create(
                network::TlsSocket{}, {1U, 2U, std::move(narrow_request)}, std::move(narrow_keys),
                std::move(narrow_aggregates), resources, config, {})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto mismatch_request =
      encode_distributed_mutable_vector_query_request({1U, 2U, fragment()}).value();
  auto mismatched_keys = keys();
  auto mismatched_aggregates = aggregates();
  mismatched_keys.front().nullable = true;
  config.limits.maximum_response_frames = 2U;
  EXPECT_EQ(DistributedMutableVectorGroupedAggregateQueryTlsClient::create(
                network::TlsSocket{}, {1U, 2U, std::move(mismatch_request)},
                std::move(mismatched_keys), std::move(mismatched_aggregates), resources, config, {})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedMutableVectorGroupedAggregateQueryTlsTest,
     CarriesOneDistinctEmptyTerminalWithoutInventingAGroup) {
  Authorizer authorizer;
  Worker worker;
  worker.empty = true;
  auto receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  const DistributedMutableVectorGroupedAggregateQueryTlsLimits limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100},
      .maximum_response_frames = 2U,
      .maximum_response_bytes = 4096U};
  auto carriers = create_carriers({.receiver = &*receiver,
                                   .authorizer = &authorizer,
                                   .client_authenticator = &client_authenticator,
                                   .server_authenticator = &server_authenticator,
                                   .server_limits = limits,
                                   .client_limits = limits},
                                  query::QueryResourceContext::create(1U << 20U).value());
  ASSERT_TRUE(carriers.has_value()) << carriers.error().to_string();
  const auto now = DistributedMutableVectorGroupedAggregateQueryTlsClient::TimePoint{} +
                   std::chrono::milliseconds{1};
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    ASSERT_TRUE(carriers->client.on_ready(true, true, now).is_ok())
        << carriers->client.failure().to_string();
    ASSERT_TRUE(carriers->server.on_ready(true, true, now).is_ok())
        << carriers->server.failure().to_string();
    if (carriers->client.state() ==
            DistributedMutableVectorGroupedAggregateQueryTlsState::kComplete &&
        carriers->server.state() ==
            DistributedMutableVectorGroupedAggregateQueryTlsState::kComplete) {
      break;
    }
  }
  auto responses = carriers->client.responses();
  ASSERT_TRUE(responses.has_value());
  ASSERT_EQ(responses->size(), 1U);
  ASSERT_TRUE(responses->front().payload.has_value());
  EXPECT_TRUE(responses->front().payload->position().empty);
  EXPECT_EQ(responses->front().payload->position().group_count, 0U);
}

TEST(DistributedMutableVectorGroupedAggregateQueryTlsTest,
     RejectsServerPrincipalBeforeWritingTheAggregateRequest) {
  Authorizer authorizer;
  authorizer.allow_server = false;
  Worker worker;
  auto receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  const DistributedMutableVectorGroupedAggregateQueryTlsLimits limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100},
      .maximum_response_frames = 2U,
      .maximum_response_bytes = 4096U};
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  auto carriers = create_carriers({.receiver = &*receiver,
                                   .authorizer = &authorizer,
                                   .client_authenticator = &client_authenticator,
                                   .server_authenticator = &server_authenticator,
                                   .server_limits = limits,
                                   .client_limits = limits},
                                  resources);
  ASSERT_TRUE(carriers.has_value()) << carriers.error().to_string();
  const auto progress_time = DistributedMutableVectorGroupedAggregateQueryTlsClient::TimePoint{} +
                             std::chrono::milliseconds{1};
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const common::Status client_status = carriers->client.on_ready(true, true, progress_time);
    ASSERT_TRUE(carriers->server.on_ready(true, true, progress_time).is_ok())
        << carriers->server.failure().to_string();
    if (!client_status.is_ok())
      break;
  }
  EXPECT_EQ(carriers->client.state(),
            DistributedMutableVectorGroupedAggregateQueryTlsState::kFailed);
  EXPECT_EQ(carriers->client.failure().code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(carriers->server.state(),
            DistributedMutableVectorGroupedAggregateQueryTlsState::kReadingRequest);
  EXPECT_EQ(worker.bind_calls, 0U);
  EXPECT_EQ(worker.execute_calls, 0U);
}

TEST(DistributedMutableVectorGroupedAggregateQueryTlsTest,
     ClearsIncompletePrefixAboveClientByteBudget) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  Authenticator client_authenticator{91U};
  Authenticator server_authenticator{92U};
  const DistributedMutableVectorGroupedAggregateQueryTlsLimits server_limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100},
      .maximum_response_frames = 2U,
      .maximum_response_bytes = 4096U};
  auto client_limits = server_limits;
  client_limits.maximum_response_bytes = 400U;
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  auto carriers = create_carriers({.receiver = &*receiver,
                                   .authorizer = &authorizer,
                                   .client_authenticator = &client_authenticator,
                                   .server_authenticator = &server_authenticator,
                                   .server_limits = server_limits,
                                   .client_limits = client_limits},
                                  resources);
  ASSERT_TRUE(carriers.has_value()) << carriers.error().to_string();
  const auto progress_time = DistributedMutableVectorGroupedAggregateQueryTlsClient::TimePoint{} +
                             std::chrono::milliseconds{1};
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const common::Status client_status = carriers->client.on_ready(true, true, progress_time);
    const common::Status server_status = carriers->server.on_ready(true, true, progress_time);
    ASSERT_TRUE(server_status.is_ok()) << carriers->server.failure().to_string();
    if (!client_status.is_ok())
      break;
  }
  EXPECT_EQ(carriers->client.state(),
            DistributedMutableVectorGroupedAggregateQueryTlsState::kFailed);
  EXPECT_EQ(carriers->client.failure().code(), common::StatusCode::kResourceExhausted);
  EXPECT_FALSE(carriers->client.responses().has_value());
  EXPECT_EQ(worker.execute_calls, 1U);
}
// NOLINTEND(bugprone-unchecked-optional-access)

} // namespace
} // namespace chronos::cluster
