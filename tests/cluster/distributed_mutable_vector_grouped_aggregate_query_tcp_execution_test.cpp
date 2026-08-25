#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_server.hpp"
#include "chronos/common/byte_reader.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
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

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const std::uint8_t tablet_seed,
                                                               const raft::NodeId node) {
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {
      .query_id = uuid(1U),
      .database_id = id<manifest::DatabaseId>(2U),
      .table_id = id<schema::TableId>(3U),
      .tablet_id = id<schema::TabletId>(tablet_seed),
      .destination_schema_id = id<schema::SchemaId>(5U),
      .raft_group_id = uuid(static_cast<std::uint8_t>(tablet_seed + 10U)),
      .serving_node = node,
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
    return (principal == 91U && node == 1U) || (principal == 92U && (node == 11U || node == 12U));
  }
};

class Worker final : public DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  explicit Worker(const std::size_t count) : count_(count) {}

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
        .input_rows = count_,
        .group_count = 1U};
    auto state =
        query::MergeableVectorAggregateState::create(result.authority.aggregates.front()).value();
    for (std::size_t index = 0U; index < count_; ++index)
      EXPECT_TRUE(state.accumulate_count_star().has_value());
    std::vector<query::ScalarValue> key_values;
    key_values.push_back(query::ScalarValue::text(string_type(), "east").value());
    std::vector<query::MergeableVectorAggregateState> states;
    states.push_back(std::move(state));
    query::DistributedVectorGroupedAggregateExchangeMessage message{
        {.query_id = received.query_id,
         .tablet_id = received.tablet_id,
         .sequence = 1U,
         .group_ordinal = 0U,
         .group_count = 1U,
         .terminal = true,
         .empty = false},
        std::move(key_values),
        std::move(states)};
    auto encoded = query::encode_distributed_vector_grouped_aggregate_exchange_message(
        message, result.authority.keys, result.authority.aggregates);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    result.encoded_bytes = encoded->bytes().size();
    result.messages.push_back(std::move(*encoded));
    return result;
  }

  std::size_t bind_calls{};
  std::size_t execute_calls{};

private:
  std::size_t count_{};
};

[[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTlsLimits carrier_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .maximum_response_frames = 1U,
          .maximum_response_bytes = std::size_t{1024U} * 1024U};
}

[[nodiscard]] common::Result<DistributedMutableVectorGroupedAggregateQueryTcpServer>
start_server(Authenticator& authenticator,
             DistributedMutableVectorGroupedAggregateQueryReceiver& receiver) {
  return DistributedMutableVectorGroupedAggregateQueryTcpServer::start(
      {.listener = {},
       .tls = server_tls(),
       .authenticator = &authenticator,
       .receiver = &receiver,
       .carrier_limits = carrier_limits(),
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
}

[[nodiscard]] common::Result<DistributedMutableVectorGroupedAggregateQueryExecution> execution() {
  std::vector fragments{fragment(4U, 11U), fragment(6U, 12U)};
  return DistributedMutableVectorGroupedAggregateQueryExecution::create(
      1U, std::move(fragments), keys(), aggregates(),
      {.sender = {.retry = {.maximum_attempts = 2U,
                            .initial_backoff = std::chrono::milliseconds{1},
                            .maximum_backoff = std::chrono::milliseconds{1}},
                  .maximum_response_frames = 1U,
                  .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .coordinator = {.messages = {.maximum_messages_per_fragment = 1U,
                                    .maximum_total_messages = 2U},
                       .maximum_total_encoded_bytes = std::size_t{1024U} * 1024U},
       .maximum_decode_memory_bytes = std::size_t{1024U} * 1024U});
}

[[nodiscard]] common::Result<DistributedMutableVectorGroupedAggregateQueryExecution>
mixed_execution() {
  std::vector fragments{fragment(4U, 1U), fragment(6U, 12U)};
  return DistributedMutableVectorGroupedAggregateQueryExecution::create(
      1U, std::move(fragments), keys(), aggregates(),
      {.sender = {.retry = {.maximum_attempts = 1U},
                  .maximum_response_frames = 1U,
                  .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .coordinator = {.messages = {.maximum_messages_per_fragment = 1U,
                                    .maximum_total_messages = 2U},
                       .maximum_total_encoded_bytes = std::size_t{1024U} * 1024U},
       .maximum_decode_memory_bytes = std::size_t{1024U} * 1024U});
}

TEST(DistributedMutableVectorGroupedAggregateQueryTcpExecutionTest,
     SchedulesSplitMutableLeadersAndPublishesOnlyCompleteMergedGroups) {
  auto refused_listener = network::TcpListener::bind();
  ASSERT_TRUE(refused_listener.has_value()) << refused_listener.error().to_string();
  const network::Ipv4Endpoint refused_endpoint = refused_listener->bound_endpoint();
  ASSERT_TRUE(refused_listener->close().is_ok());

  Authorizer authorizer;
  Worker first_worker{1U};
  Worker second_worker{2U};
  auto first_receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .worker = &first_worker});
  auto second_receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .worker = &second_worker});
  ASSERT_TRUE(first_receiver.has_value()) << first_receiver.error().to_string();
  ASSERT_TRUE(second_receiver.has_value()) << second_receiver.error().to_string();
  Authenticator client_authenticator{91U};
  auto first_server = start_server(client_authenticator, *first_receiver);
  auto second_server = start_server(client_authenticator, *second_receiver);
  ASSERT_TRUE(first_server.has_value()) << first_server.error().to_string();
  ASSERT_TRUE(second_server.has_value()) << second_server.error().to_string();
  auto context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator server_authenticator{92U};
  auto portable = execution();
  ASSERT_TRUE(portable.has_value()) << portable.error().to_string();
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  auto doubled_count = query::VectorExpression::create(
      {query::VectorInputExpression{.input_column_ordinal = 1U, .type = int64, .nullable = false},
       query::VectorConstantExpression{query::ScalarValue::signed_value(int64, 2).value()},
       query::VectorBinaryExpression{.operation = query::VectorBinaryOperation::kMultiply,
                                     .left_instruction = 0U,
                                     .right_instruction = 1U}});
  ASSERT_TRUE(doubled_count.has_value()) << doubled_count.error().to_string();
  auto scheduled = DistributedMutableVectorGroupedAggregateQueryTcpExecution::create(
      std::move(*portable),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{.node_id = 11U,
                   .endpoints = {refused_endpoint, first_server->bound_endpoint()},
                   .tls_context = std::addressof(*context)},
                  {.node_id = 12U,
                   .endpoints = {second_server->bound_endpoint()},
                   .tls_context = std::addressof(*context)}},
       .carrier_limits = carrier_limits(),
       .coordinator_projection =
           query::DistributedVectorGroupedAggregateCoordinatorProjection{
               .outputs = {std::move(*doubled_count)},
               .result_schema = {.columns = {{"doubled_count", int64, false}}}},
       .connect_timeout = std::chrono::milliseconds{1000},
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}});
  ASSERT_TRUE(scheduled.has_value()) << scheduled.error().to_string();
  EXPECT_FALSE(scheduled->result().has_value());
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       scheduled->state() ==
           DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(scheduled->poll_once(std::chrono::milliseconds{1}).is_ok())
        << scheduled->failure().to_string();
    ASSERT_TRUE(first_server->poll_once(std::chrono::milliseconds{0}).is_ok());
    ASSERT_TRUE(second_server->poll_once(std::chrono::milliseconds{0}).is_ok());
  }
  ASSERT_EQ(scheduled->state(),
            DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kComplete)
      << scheduled->failure().to_string();
  ASSERT_TRUE(scheduled->result().has_value());
  ASSERT_EQ(scheduled->result()->row_count, 1U);
  ASSERT_EQ(scheduled->result()->encoded_batches.size(), 1U);
  auto decoded = network::decode_query_result_batch(scheduled->result()->encoded_batches.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  ASSERT_EQ(decoded->columns().size(), 1U);
  EXPECT_EQ(decoded->columns().front().name, "doubled_count");
  const network::QueryResultCell* count = decoded->cell(0U, 0U);
  ASSERT_NE(count, nullptr);
  common::ByteReader count_reader{count->value};
  const auto count_value = count_reader.read_i64_le();
  ASSERT_TRUE(count_value.has_value());
  EXPECT_EQ(*count_value, 6);
  EXPECT_TRUE(scheduled->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(first_worker.bind_calls, 1U);
  EXPECT_EQ(first_worker.execute_calls, 1U);
  EXPECT_EQ(second_worker.bind_calls, 1U);
  EXPECT_EQ(second_worker.execute_calls, 1U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  const auto metrics = scheduled->metrics();
  EXPECT_EQ(metrics.attempts_started, 3U);
  EXPECT_EQ(metrics.retries_started, 1U);
  EXPECT_EQ(metrics.transport_completed_attempts, 2U);
  EXPECT_EQ(metrics.transport_failed_attempts, 1U);
  EXPECT_EQ(metrics.active_attempts, 0U);
  auto taken = scheduled->take_result();
  ASSERT_TRUE(taken.has_value()) << taken.error().to_string();
  EXPECT_EQ(taken->row_count, 1U);
  EXPECT_FALSE(scheduled->result().has_value());
  EXPECT_EQ(scheduled->take_result().error().code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(first_server->shutdown().is_ok());
  EXPECT_TRUE(second_server->shutdown().is_ok());
}

TEST(DistributedMutableVectorGroupedAggregateQueryTcpExecutionTest,
     RejectsIncompleteRoutesAndOwnsDeadlineAndExplicitCancellation) {
  Authorizer authorizer;
  Authenticator authenticator{92U};
  auto context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  auto listener = network::TcpListener::bind();
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  const DistributedQueryNodeRoute first_route{.node_id = 11U,
                                              .endpoints = {listener->bound_endpoint()},
                                              .tls_context = std::addressof(*context)};
  const DistributedQueryNodeRoute second_route{.node_id = 12U,
                                               .endpoints = {listener->bound_endpoint()},
                                               .tls_context = std::addressof(*context)};

  auto missing_portable = execution();
  ASSERT_TRUE(missing_portable.has_value());
  EXPECT_EQ(DistributedMutableVectorGroupedAggregateQueryTcpExecution::create(
                std::move(*missing_portable), {.authenticator = &authenticator,
                                               .node_authorizer = &authorizer,
                                               .routes = {first_route}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto invalid_limits = execution();
  ASSERT_TRUE(invalid_limits.has_value());
  auto invalid_config = DistributedMutableVectorGroupedAggregateQueryTcpExecutionConfig{
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .routes = {first_route, second_route}};
  invalid_config.carrier_limits.payload.state.maximum_variable_extremum_bytes = 0U;
  EXPECT_EQ(DistributedMutableVectorGroupedAggregateQueryTcpExecution::create(
                std::move(*invalid_limits), std::move(invalid_config))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto invalid_finalization = execution();
  ASSERT_TRUE(invalid_finalization.has_value());
  EXPECT_EQ(
      DistributedMutableVectorGroupedAggregateQueryTcpExecution::create(
          std::move(*invalid_finalization), {.authenticator = &authenticator,
                                             .node_authorizer = &authorizer,
                                             .routes = {first_route, second_route},
                                             .finalization_limits = {.maximum_output_batches = 0U}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  auto expired_portable = execution();
  ASSERT_TRUE(expired_portable.has_value());
  auto expired = DistributedMutableVectorGroupedAggregateQueryTcpExecution::create(
      std::move(*expired_portable),
      {.authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .routes = {first_route, second_route},
       .execution_deadline = std::chrono::steady_clock::now() - std::chrono::milliseconds{1}});
  ASSERT_TRUE(expired.has_value()) << expired.error().to_string();
  EXPECT_EQ(expired->poll_once(std::chrono::milliseconds{0}).code(),
            common::StatusCode::kCancelled);
  EXPECT_EQ(expired->state(),
            DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kCancelled);
  EXPECT_EQ(expired->metrics().attempts_started, 0U);
  EXPECT_FALSE(expired->result().has_value());

  auto cancelled_portable = execution();
  ASSERT_TRUE(cancelled_portable.has_value());
  auto cancelled = DistributedMutableVectorGroupedAggregateQueryTcpExecution::create(
      std::move(*cancelled_portable), {.authenticator = &authenticator,
                                       .node_authorizer = &authorizer,
                                       .routes = {first_route, second_route}});
  ASSERT_TRUE(cancelled.has_value()) << cancelled.error().to_string();
  ASSERT_TRUE(cancelled->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(cancelled->state(),
            DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kRunning);
  EXPECT_GT(cancelled->metrics().active_attempts, 0U);
  EXPECT_EQ(cancelled->cancel().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(cancelled->state(),
            DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kCancelled);
  EXPECT_EQ(cancelled->metrics().active_attempts, 0U);
  EXPECT_FALSE(cancelled->result().has_value());
  EXPECT_TRUE(listener->close().is_ok());
}

TEST(DistributedMutableVectorGroupedAggregateQueryTcpExecutionTest,
     MergesLocalAndRemoteMutableGroupedWorkersBeforeNativePublication) {
  Authorizer authorizer;
  Worker local_worker{1U};
  Worker remote_worker{2U};
  auto remote_receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .worker = &remote_worker});
  ASSERT_TRUE(remote_receiver.has_value()) << remote_receiver.error().to_string();
  Authenticator client_authenticator{91U};
  auto remote_server = start_server(client_authenticator, *remote_receiver);
  ASSERT_TRUE(remote_server.has_value()) << remote_server.error().to_string();
  auto context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator server_authenticator{92U};

  auto missing_local = mixed_execution();
  ASSERT_TRUE(missing_local.has_value()) << missing_local.error().to_string();
  EXPECT_EQ(
      DistributedMutableVectorGroupedAggregateQueryTcpExecution::create(
          std::move(*missing_local), {.authenticator = &server_authenticator,
                                      .node_authorizer = &authorizer,
                                      .routes = {{.node_id = 12U,
                                                  .endpoints = {remote_server->bound_endpoint()},
                                                  .tls_context = std::addressof(*context)}}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  auto portable = mixed_execution();
  ASSERT_TRUE(portable.has_value()) << portable.error().to_string();
  auto scheduled = DistributedMutableVectorGroupedAggregateQueryTcpExecution::create(
      std::move(*portable),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .local_node_id = 1U,
       .local_worker = &local_worker,
       .routes = {{.node_id = 12U,
                   .endpoints = {remote_server->bound_endpoint()},
                   .tls_context = std::addressof(*context)}},
       .carrier_limits = carrier_limits(),
       .connect_timeout = std::chrono::milliseconds{1000},
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}});
  ASSERT_TRUE(scheduled.has_value()) << scheduled.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       scheduled->state() ==
           DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(scheduled->poll_once(std::chrono::milliseconds{1}).is_ok())
        << scheduled->failure().to_string();
    ASSERT_TRUE(remote_server->poll_once(std::chrono::milliseconds{0}).is_ok());
  }
  ASSERT_EQ(scheduled->state(),
            DistributedMutableVectorGroupedAggregateQueryTcpExecutionState::kComplete)
      << scheduled->failure().to_string();
  ASSERT_TRUE(scheduled->result().has_value());
  auto decoded = network::decode_query_result_batch(scheduled->result()->encoded_batches.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  const network::QueryResultCell* count = decoded->cell(0U, 1U);
  ASSERT_NE(count, nullptr);
  common::ByteReader count_reader{count->value};
  EXPECT_EQ(count_reader.read_i64_le().value(), 3);
  EXPECT_EQ(local_worker.bind_calls, 1U);
  EXPECT_EQ(local_worker.execute_calls, 1U);
  EXPECT_EQ(remote_worker.bind_calls, 1U);
  EXPECT_EQ(remote_worker.execute_calls, 1U);
  const auto metrics = scheduled->metrics();
  EXPECT_EQ(metrics.attempts_started, 2U);
  EXPECT_EQ(metrics.local_completed_attempts, 1U);
  EXPECT_EQ(metrics.local_failed_attempts, 0U);
  EXPECT_EQ(metrics.transport_completed_attempts, 1U);
  EXPECT_EQ(metrics.transport_failed_attempts, 0U);
  EXPECT_TRUE(remote_server->shutdown().is_ok());
}

} // namespace
} // namespace chronos::cluster
