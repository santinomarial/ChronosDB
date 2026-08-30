#include "chronos/cluster/distributed_mutable_query_control_tcp.hpp"
#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_shuffle_job_execution.hpp"
#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_shuffle_source_worker.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_coordinator_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_service.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_source_plan.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
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
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] schema::LogicalType int64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"region", string_type(), false}, {"count", int64_type(), false}}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const std::uint8_t tablet_seed = 2U,
                                                               const raft::NodeId node = 2U) {
  return {.query_id = uuid(1U),
          .database_id = id<manifest::DatabaseId>(8U),
          .table_id = id<schema::TableId>(9U),
          .tablet_id = id<schema::TabletId>(tablet_seed),
          .destination_schema_id = id<schema::SchemaId>(10U),
          .raft_group_id = uuid(static_cast<std::uint8_t>(tablet_seed + 9U)),
          .serving_node = node,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 3U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                   .group_key_input_indices = {0U},
                   .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}},
          .result_schema = result_schema()};
}

struct Proofs {
  std::vector<query::DistributedMutableVectorFragment> fragments;
  DistributedVectorGroupedAggregateShuffleAuthority authority;
  DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2 finalization;

  explicit Proofs(std::vector<query::DistributedMutableVectorFragment> input = {fragment()})
      : fragments(std::move(input)),
        authority(*DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
            fragments, keys(), aggregates())),
        finalization(*DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2::create(
            authority, fragments)) {}
};

[[nodiscard]] std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>
input(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      const schema::TabletId& tablet_id = id<schema::TabletId>(2U)) {
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "east").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> encoded;
  encoded.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                        {.query_id = authority.query_id(),
                         .tablet_id = tablet_id,
                         .sequence = 1U,
                         .group_ordinal = 0U,
                         .group_count = 1U,
                         .terminal = true,
                         .empty = false},
                        values, states, authority.key_definitions(),
                        authority.aggregate_definitions())
                        .value());
  return encoded;
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
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static,bugprone-easily-swappable-parameters)
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return common::Result<bool>{
        (principal == 91U && node == 9U) || (principal == 92U && node == 2U) ||
        (principal == 93U && node == 9U) || (principal == 94U && node == 2U) ||
        (principal == 95U && node == 3U)};
  }
};

class UnusedMutableWorker final : public DistributedMutableVectorQueryWorkerService {
public:
  common::Result<std::vector<DistributedVectorResultExchangeMessage>>
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  execute(const query::DistributedMutableVectorFragment&) override {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "unused mutable worker was called"});
  }
};

class UnusedGroupedWorker final
    : public DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  common::Result<query::DistributedVectorGroupedAggregateAuthority>
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  bind_authority(const query::DistributedMutableVectorFragment&) override {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "unused grouped worker was called"});
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  execute(const query::DistributedMutableVectorFragment&) override {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "unused grouped worker was called"});
  }
};

class FixedGroupedWorker final : public DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  explicit FixedGroupedWorker(const DistributedVectorGroupedAggregateShuffleAuthority& authority)
      : authority_(&authority) {}

  common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedMutableVectorFragment&) override {
    return query::DistributedVectorGroupedAggregateAuthority{keys(), aggregates()};
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedMutableVectorFragment& fragment) override {
    auto messages = input(*authority_, fragment.tablet_id);
    const std::size_t encoded_bytes = messages.front().bytes().size();
    return query::DistributedVectorGroupedAggregateWorkerResultV2{
        .authority = {keys(), aggregates()},
        .messages = std::move(messages),
        .input_rows = 1U,
        .group_count = 1U,
        .encoded_bytes = encoded_bytes};
  }

private:
  const DistributedVectorGroupedAggregateShuffleAuthority* authority_{};
};

class UnusedAuthorityService final : public RaftReadAuthorityService {
public:
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  common::Result<RaftReadAuthority> acquire(const raft::GroupId&) override {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "unused authority service was called"});
  }
};

struct UnusedReceivers {
  explicit UnusedReceivers(Authorizer& authorizer, const raft::NodeId local_node_id = 2U)
      : mutable_receiver(
            DistributedMutableVectorQueryReceiver::create({.local_node_id = local_node_id,
                                                           .authorizer = &authorizer,
                                                           .worker = &mutable_worker})
                .value()),
        grouped_receiver(DistributedMutableVectorGroupedAggregateQueryReceiver::create(
                             {.local_node_id = local_node_id,
                              .authorizer = &authorizer,
                              .worker = &grouped_worker})
                             .value()),
        authority_receiver(RaftReadAuthorityReceiver::create({.local_node_id = local_node_id,
                                                              .authorizer = &authorizer,
                                                              .service = &authority_service})
                               .value()) {}

  UnusedMutableWorker mutable_worker;
  UnusedGroupedWorker grouped_worker;
  UnusedAuthorityService authority_service;
  DistributedMutableVectorQueryReceiver mutable_receiver;
  DistributedMutableVectorGroupedAggregateQueryReceiver grouped_receiver;
  RaftReadAuthorityReceiver authority_receiver;
};

[[nodiscard]] DistributedVectorGroupedAggregateShuffleTlsLimits shuffle_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTlsLimits result_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTlsLimits control_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000}};
}

TEST(DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionTest,
     PreparesAllRoutesSealsAndPublishesOnlyTheCompleteNativeResult) {
  Proofs proof;
  Authorizer authorizer;
  UnusedReceivers receivers{authorizer};
  Authenticator control_server_authenticator{91U};
  Authenticator control_client_authenticator{92U};
  Authenticator result_client_authenticator{93U};
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(client_context.has_value()) << client_context.error().to_string();
  const std::array result_contexts{DistributedQueryNodeTlsContext{9U, &*client_context}};
  auto job_service = DistributedVectorGroupedAggregateShuffleJobService::create(
      {.local_node_id = 2U,
       .shuffle_listener = {},
       .shuffle_tls = server_tls(),
       .shuffle_authenticator = &control_server_authenticator,
       .result_authenticator = &result_client_authenticator,
       .node_authorizer = &authorizer,
       .result_tls_contexts = result_contexts,
       .shuffle_carrier_limits = shuffle_limits(),
       .result_retry_limits = {.retry = {.maximum_attempts = 3U,
                                         .initial_backoff = std::chrono::milliseconds{1},
                                         .maximum_backoff = std::chrono::milliseconds{2}},
                               .stream = result_limits().stream},
       .result_carrier_limits = result_limits(),
       .maximum_jobs = 1U,
       .maximum_job_query_memory_bytes = 16U << 20U,
       .maximum_retained_streams_per_job = 1U,
       .maximum_accepts_per_job_poll = 1U,
       .maximum_reducer_admissions_per_job_poll = 1U});
  ASSERT_TRUE(job_service.has_value()) << job_service.error().to_string();
  auto control_server = DistributedMutableQueryControlTcpServer::start(
      {.listener = {},
       .tls = server_tls(),
       .authenticator = &control_server_authenticator,
       .mutable_receiver = &receivers.mutable_receiver,
       .mutable_grouped_receiver = &receivers.grouped_receiver,
       .read_authority_receiver = &receivers.authority_receiver,
       .grouped_shuffle_job_service = &*job_service,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .maximum_connections = 2U,
       .maximum_accepts_per_poll = 2U});
  ASSERT_TRUE(control_server.has_value()) << control_server.error().to_string();

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
  auto coordinator = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::create(
      proof.authority, proof.finalization,
      {.coordinator_node_id = 9U,
       .reducer_control_routes = {{.node_id = 2U,
                                   .endpoints = {control_server->bound_endpoint()},
                                   .tls_context = &*client_context}},
       .authenticator = &control_client_authenticator,
       .node_authorizer = &authorizer,
       .carrier_limits = control_limits(),
       .connect_timeout = std::chrono::milliseconds{1000},
       .prepare_retry = {.maximum_attempts = 2U,
                         .initial_backoff = std::chrono::milliseconds{1},
                         .maximum_backoff = std::chrono::milliseconds{1}},
       .seal_retry = {.maximum_attempts = 3U,
                      .initial_backoff = std::chrono::milliseconds{1},
                      .maximum_backoff = std::chrono::milliseconds{2}},
       .lease_retry = {.maximum_attempts = 2U,
                       .initial_backoff = std::chrono::milliseconds{1},
                       .maximum_backoff = std::chrono::milliseconds{2}},
       .reducer_execution_timeout = std::chrono::seconds{5},
       .lease_duration = std::chrono::milliseconds{100},
       .lease_renew_interval = std::chrono::milliseconds{5},
       .execution_deadline = deadline,
       .result = {.listener = {},
                  .tls = server_tls(),
                  .authenticator = &control_client_authenticator,
                  .node_authorizer = &authorizer,
                  .coordinator_node_id = 9U,
                  .carrier_limits = result_limits(),
                  .maximum_retained_server_streams = 1U,
                  .maximum_accepts_per_poll = 1U}});
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  EXPECT_TRUE(coordinator->prepared_routes().empty());
  EXPECT_EQ(coordinator->take_result().error().code(), common::StatusCode::kUnavailable);

  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const auto client_progress = coordinator->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(client_progress.is_ok()) << client_progress.to_string();
    const auto server_progress = control_server->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(server_progress.is_ok()) << server_progress.to_string();
    if (coordinator->state() ==
        DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPrepared)
      break;
    EXPECT_TRUE(coordinator->prepared_routes().empty());
  }
  ASSERT_EQ(coordinator->state(),
            DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPrepared)
      << coordinator->failure().to_string();
  EXPECT_TRUE(coordinator->prepared_routes().empty());
  EXPECT_EQ(job_service->metrics().active_jobs, 1U);
  EXPECT_EQ(job_service->metrics().lease_activations, 1U);

  for (std::size_t iteration = 0U; iteration < 4096U && job_service->metrics().lease_renewals == 0U;
       ++iteration) {
    ASSERT_TRUE(coordinator->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(control_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_GE(job_service->metrics().lease_renewals, 1U);
  ASSERT_EQ(coordinator->state(),
            DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kPrepared);

  auto resources = query::QueryResourceContext::create(16U << 20U).value();
  const auto encoded_input = input(proof.authority);
  auto source_plan = DistributedVectorGroupedAggregateShuffleSourcePlan::create(
      proof.authority, id<schema::TabletId>(2U), encoded_input, resources);
  ASSERT_TRUE(source_plan.has_value()) << source_plan.error().to_string();
  auto local_streams = source_plan->take_local_streams();
  ASSERT_EQ(local_streams.size(), 1U);
  ASSERT_TRUE(
      job_service->accept_local_stream(proof.authority.query_id(), local_streams.front()).is_ok());
  ASSERT_TRUE(coordinator->seal().is_ok());
  EXPECT_EQ(coordinator->state(),
            DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kSealing);
  EXPECT_EQ(coordinator->take_result().error().code(), common::StatusCode::kUnavailable);

  for (std::size_t iteration = 0U; iteration < 8192U; ++iteration) {
    const auto client_progress = coordinator->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(client_progress.is_ok()) << client_progress.to_string();
    const auto server_progress = control_server->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(server_progress.is_ok()) << server_progress.to_string();
    if (coordinator->state() ==
        DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kComplete)
      break;
    EXPECT_EQ(coordinator->take_result().error().code(), common::StatusCode::kUnavailable);
  }
  ASSERT_EQ(coordinator->state(),
            DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kComplete)
      << coordinator->failure().to_string();
  const auto metrics = coordinator->metrics();
  EXPECT_EQ(metrics.reducer_nodes, 1U);
  EXPECT_EQ(metrics.prepared_reducers, 1U);
  EXPECT_EQ(metrics.route_installed_reducers, 1U);
  EXPECT_EQ(metrics.sealed_reducers, 1U);
  EXPECT_EQ(metrics.control_attempts_started, 3U);
  EXPECT_EQ(metrics.control_retries_started, 0U);
  EXPECT_GE(metrics.lease_rounds_completed, 2U);
  EXPECT_GE(metrics.lease_responses_accepted, 2U);
  EXPECT_EQ(metrics.lease_failures, 0U);
  EXPECT_EQ(metrics.result.collector.accepted_partitions, 1U);
  EXPECT_EQ(metrics.result.finalization_attempts, 1U);
  EXPECT_TRUE(control_server_authenticator.saw_fingerprint);
  EXPECT_TRUE(control_client_authenticator.saw_fingerprint);
  EXPECT_TRUE(result_client_authenticator.saw_fingerprint);

  auto result = coordinator->take_result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->encoded_batches.size(), 1U);
  auto decoded = network::decode_query_result_batch(result->encoded_batches.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  const auto* region = decoded->cell(0U, 0U);
  const auto* count = decoded->cell(0U, 1U);
  ASSERT_NE(region, nullptr);
  ASSERT_NE(count, nullptr);
  const std::string expected_region{"east"};
  EXPECT_TRUE(std::ranges::equal(region->value, std::as_bytes(std::span{expected_region})));
  ASSERT_EQ(count->value.size(), sizeof(std::uint64_t));
  EXPECT_EQ(count->value.front(), std::byte{1U});
  for (std::size_t index = 1U; index < count->value.size(); ++index)
    EXPECT_EQ(count->value[index], std::byte{});
  EXPECT_EQ(coordinator->state(),
            DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kResultTaken);
  EXPECT_EQ(coordinator->take_result().error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(job_service->metrics().completed_jobs, 1U);
}

TEST(DistributedMutableVectorGroupedAggregateShuffleJobExecutionTest,
     OwnsPreparedWorkerSourceSealAndNativeResultAsOneLifecycle) {
  Proofs proof;
  Authorizer authorizer;
  UnusedReceivers receivers{authorizer};
  Authenticator control_server_authenticator{91U};
  Authenticator control_client_authenticator{92U};
  Authenticator result_client_authenticator{93U};
  auto client_context = network::TlsClientContext::create(client_tls()).value();
  const std::array result_contexts{DistributedQueryNodeTlsContext{9U, &client_context}};
  auto job_service =
      DistributedVectorGroupedAggregateShuffleJobService::create(
          {.local_node_id = 2U,
           .shuffle_listener = {},
           .shuffle_tls = server_tls(),
           .shuffle_authenticator = &control_server_authenticator,
           .result_authenticator = &result_client_authenticator,
           .node_authorizer = &authorizer,
           .result_tls_contexts = result_contexts,
           .shuffle_carrier_limits = shuffle_limits(),
           .result_retry_limits = {.retry = {.maximum_attempts = 3U,
                                             .initial_backoff = std::chrono::milliseconds{1},
                                             .maximum_backoff = std::chrono::milliseconds{2}},
                                   .stream = result_limits().stream},
           .result_carrier_limits = result_limits(),
           .maximum_jobs = 1U,
           .maximum_job_query_memory_bytes = 16U << 20U,
           .maximum_retained_streams_per_job = 1U,
           .maximum_accepts_per_job_poll = 1U,
           .maximum_reducer_admissions_per_job_poll = 1U})
          .value();
  FixedGroupedWorker worker{proof.authority};
  auto publishing_worker =
      DistributedMutableVectorGroupedAggregateShuffleSourceWorker::create(worker, job_service)
          .value();
  auto grouped_receiver =
      DistributedMutableVectorGroupedAggregateQueryReceiver::create(
          {.local_node_id = 2U, .authorizer = &authorizer, .worker = &publishing_worker})
          .value();
  auto control_server =
      DistributedMutableQueryControlTcpServer::start(
          {.listener = {},
           .tls = server_tls(),
           .authenticator = &control_server_authenticator,
           .mutable_receiver = &receivers.mutable_receiver,
           .mutable_grouped_receiver = &grouped_receiver,
           .read_authority_receiver = &receivers.authority_receiver,
           .grouped_shuffle_job_service = &job_service,
           .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000}},
           .maximum_connections = 2U,
           .maximum_accepts_per_poll = 2U})
          .value();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
  auto execution = DistributedMutableVectorGroupedAggregateShuffleJobExecution::create(
      9U, proof.fragments, keys(), aggregates(),
      {.worker_transport = {.authenticator = &control_client_authenticator,
                            .node_authorizer = &authorizer,
                            .routes = {{.node_id = 2U,
                                        .endpoints = {control_server.bound_endpoint()},
                                        .tls_context = &client_context}},
                            .connect_timeout = std::chrono::milliseconds{1000},
                            .execution_deadline = deadline,
                            .maximum_rebindings = 0U},
       .reducers = {.coordinator_node_id = 9U,
                    .reducer_control_routes = {{.node_id = 2U,
                                                .endpoints = {control_server.bound_endpoint()},
                                                .tls_context = &client_context}},
                    .authenticator = &control_client_authenticator,
                    .node_authorizer = &authorizer,
                    .carrier_limits = control_limits(),
                    .connect_timeout = std::chrono::milliseconds{1000},
                    .prepare_retry = {.maximum_attempts = 2U,
                                      .initial_backoff = std::chrono::milliseconds{1},
                                      .maximum_backoff = std::chrono::milliseconds{2}},
                    .route_install_retry = {.maximum_attempts = 2U,
                                            .initial_backoff = std::chrono::milliseconds{1},
                                            .maximum_backoff = std::chrono::milliseconds{2}},
                    .seal_retry = {.maximum_attempts = 8U,
                                   .initial_backoff = std::chrono::milliseconds{1},
                                   .maximum_backoff = std::chrono::milliseconds{4}},
                    .reducer_execution_timeout = std::chrono::seconds{5},
                    .execution_deadline = deadline,
                    .result = {.listener = {},
                               .tls = server_tls(),
                               .authenticator = &control_client_authenticator,
                               .node_authorizer = &authorizer,
                               .coordinator_node_id = 9U,
                               .carrier_limits = result_limits(),
                               .maximum_retained_server_streams = 1U,
                               .maximum_accepts_per_poll = 1U}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->take_result().error().code(), common::StatusCode::kUnavailable);
  for (std::size_t iteration = 0U; iteration < 8192U; ++iteration) {
    const common::Status client = execution->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(client.is_ok()) << client.to_string();
    const common::Status server = control_server.poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(server.is_ok()) << server.to_string();
    if (execution->state() ==
        DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kComplete)
      break;
  }
  ASSERT_EQ(execution->state(),
            DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kComplete)
      << execution->failure().to_string();
  const auto metrics = execution->metrics();
  EXPECT_EQ(metrics.workers.transport_completed_attempts, 1U);
  EXPECT_EQ(metrics.reducers.prepared_reducers, 1U);
  EXPECT_EQ(metrics.reducers.route_installed_reducers, 1U);
  EXPECT_EQ(metrics.reducers.sealed_reducers, 1U);
  EXPECT_EQ(metrics.reducers.lease_rounds_completed, 1U);
  EXPECT_EQ(job_service.metrics().lease_activations, 1U);
  EXPECT_EQ(job_service.metrics().submitted_source_tablets, 1U);
  auto result = execution->take_result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->encoded_batches.size(), 1U);
  auto decoded = network::decode_query_result_batch(result->encoded_batches.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  const auto* count = decoded->cell(0U, 1U);
  ASSERT_NE(count, nullptr);
  EXPECT_EQ(count->value.front(), std::byte{1U});
  EXPECT_EQ(execution->state(),
            DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kResultTaken);
}

TEST(DistributedMutableVectorGroupedAggregateShuffleJobExecutionTest,
     OwnsLocalCoordinatorWorkerReducerLeaseAndResultWithoutNetworkSelfRoutes) {
  Proofs proof;
  Authorizer authorizer;
  Authenticator authenticator{92U};
  auto client_context = network::TlsClientContext::create(client_tls()).value();
  const std::array result_contexts{DistributedQueryNodeTlsContext{2U, &client_context}};
  auto job_service =
      DistributedVectorGroupedAggregateShuffleJobService::create(
          {.local_node_id = 2U,
           .shuffle_listener = {},
           .shuffle_tls = server_tls(),
           .shuffle_authenticator = &authenticator,
           .result_authenticator = &authenticator,
           .node_authorizer = &authorizer,
           .result_tls_contexts = result_contexts,
           .shuffle_carrier_limits = shuffle_limits(),
           .result_retry_limits = {.retry = {.maximum_attempts = 2U,
                                             .initial_backoff = std::chrono::milliseconds{1},
                                             .maximum_backoff = std::chrono::milliseconds{2}},
                                   .stream = result_limits().stream},
           .result_carrier_limits = result_limits(),
           .maximum_jobs = 1U,
           .maximum_job_query_memory_bytes = 16U << 20U,
           .maximum_retained_streams_per_job = 1U,
           .maximum_accepts_per_job_poll = 1U,
           .maximum_reducer_admissions_per_job_poll = 1U})
          .value();
  FixedGroupedWorker worker{proof.authority};
  auto publishing_worker =
      DistributedMutableVectorGroupedAggregateShuffleSourceWorker::create(worker, job_service)
          .value();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
  const DistributedQueryNodeRoute local_route{
      .node_id = 2U, .endpoints = {{{127U, 0U, 0U, 1U}, 9123U}}, .tls_context = &client_context};
  auto execution = DistributedMutableVectorGroupedAggregateShuffleJobExecution::create(
      2U, proof.fragments, keys(), aggregates(),
      {.worker_transport = {.authenticator = &authenticator,
                            .node_authorizer = &authorizer,
                            .local_node_id = 2U,
                            .local_worker = &publishing_worker,
                            .routes = {local_route},
                            .connect_timeout = std::chrono::milliseconds{1000},
                            .execution_deadline = deadline,
                            .maximum_rebindings = 0U},
       .reducers = {.coordinator_node_id = 2U,
                    .reducer_control_routes = {local_route},
                    .authenticator = &authenticator,
                    .node_authorizer = &authorizer,
                    .local_reducer_job_service = &job_service,
                    .carrier_limits = control_limits(),
                    .connect_timeout = std::chrono::milliseconds{1000},
                    .prepare_retry = {.maximum_attempts = 2U,
                                      .initial_backoff = std::chrono::milliseconds{1},
                                      .maximum_backoff = std::chrono::milliseconds{2}},
                    .route_install_retry = {.maximum_attempts = 2U,
                                            .initial_backoff = std::chrono::milliseconds{1},
                                            .maximum_backoff = std::chrono::milliseconds{2}},
                    .seal_retry = {.maximum_attempts = 4U,
                                   .initial_backoff = std::chrono::milliseconds{1},
                                   .maximum_backoff = std::chrono::milliseconds{2}},
                    .lease_retry = {.maximum_attempts = 2U,
                                    .initial_backoff = std::chrono::milliseconds{1},
                                    .maximum_backoff = std::chrono::milliseconds{2}},
                    .reducer_execution_timeout = std::chrono::seconds{5},
                    .lease_duration = std::chrono::milliseconds{100},
                    .lease_renew_interval = std::chrono::milliseconds{10},
                    .execution_deadline = deadline,
                    .result = {.listener = {},
                               .tls = server_tls(),
                               .authenticator = &authenticator,
                               .node_authorizer = &authorizer,
                               .coordinator_node_id = 2U,
                               .carrier_limits = result_limits(),
                               .maximum_retained_server_streams = 1U,
                               .maximum_accepts_per_poll = 1U}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const common::Status progress = execution->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(progress.is_ok()) << progress.to_string();
    if (execution->state() ==
        DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kComplete) {
      break;
    }
  }
  ASSERT_EQ(execution->state(),
            DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kComplete)
      << execution->failure().to_string();
  const auto metrics = execution->metrics();
  EXPECT_EQ(metrics.workers.local_completed_attempts, 1U);
  EXPECT_EQ(metrics.workers.transport_completed_attempts, 0U);
  EXPECT_EQ(metrics.reducers.prepared_reducers, 1U);
  EXPECT_EQ(metrics.reducers.route_installed_reducers, 1U);
  EXPECT_EQ(metrics.reducers.sealed_reducers, 1U);
  EXPECT_EQ(metrics.reducers.result.collector.accepted_partitions, 1U);
  EXPECT_EQ(job_service.metrics().lease_activations, 1U);
  EXPECT_EQ(job_service.metrics().completed_jobs, 1U);
  auto result = execution->take_result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->encoded_batches.size(), 1U);
  auto decoded = network::decode_query_result_batch(result->encoded_batches.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  const auto* count = decoded->cell(0U, 1U);
  ASSERT_NE(count, nullptr);
  EXPECT_EQ(count->value.front(), std::byte{1U});
}

TEST(DistributedMutableVectorGroupedAggregateShuffleJobExecutionTest,
     ComposesLocalAndRemoteWorkersReducersAndResultsInOneMixedRoleQuery) {
  Proofs proof{{fragment(2U, 2U), fragment(3U, 3U)}};
  Authorizer authorizer;
  UnusedReceivers remote_receivers{authorizer, 3U};
  Authenticator coordinator_authenticator{95U};
  Authenticator remote_node_authenticator{92U};
  Authenticator local_node_authenticator{95U};
  auto client_context = network::TlsClientContext::create(client_tls()).value();
  const std::array result_contexts{DistributedQueryNodeTlsContext{2U, &client_context},
                                   DistributedQueryNodeTlsContext{3U, &client_context}};
  auto local_jobs =
      DistributedVectorGroupedAggregateShuffleJobService::create(
          {.local_node_id = 2U,
           .shuffle_listener = {},
           .shuffle_tls = server_tls(),
           .shuffle_authenticator = &local_node_authenticator,
           .result_authenticator = &local_node_authenticator,
           .node_authorizer = &authorizer,
           .result_tls_contexts = result_contexts,
           .shuffle_carrier_limits = shuffle_limits(),
           .result_retry_limits = {.retry = {.maximum_attempts = 4U,
                                             .initial_backoff = std::chrono::milliseconds{1},
                                             .maximum_backoff = std::chrono::milliseconds{4}},
                                   .stream = result_limits().stream},
           .result_carrier_limits = result_limits(),
           .maximum_jobs = 1U,
           .maximum_job_query_memory_bytes = 16U << 20U,
           .maximum_retained_streams_per_job = 2U,
           .maximum_accepts_per_job_poll = 2U,
           .maximum_reducer_admissions_per_job_poll = 2U})
          .value();
  auto remote_jobs =
      DistributedVectorGroupedAggregateShuffleJobService::create(
          {.local_node_id = 3U,
           .shuffle_listener = {},
           .shuffle_tls = server_tls(),
           .shuffle_authenticator = &remote_node_authenticator,
           .result_authenticator = &remote_node_authenticator,
           .node_authorizer = &authorizer,
           .result_tls_contexts = result_contexts,
           .shuffle_carrier_limits = shuffle_limits(),
           .result_retry_limits = {.retry = {.maximum_attempts = 4U,
                                             .initial_backoff = std::chrono::milliseconds{1},
                                             .maximum_backoff = std::chrono::milliseconds{4}},
                                   .stream = result_limits().stream},
           .result_carrier_limits = result_limits(),
           .maximum_jobs = 1U,
           .maximum_job_query_memory_bytes = 16U << 20U,
           .maximum_retained_streams_per_job = 2U,
           .maximum_accepts_per_job_poll = 2U,
           .maximum_reducer_admissions_per_job_poll = 2U})
          .value();
  FixedGroupedWorker local_worker{proof.authority};
  FixedGroupedWorker remote_worker{proof.authority};
  auto local_publishing_worker =
      DistributedMutableVectorGroupedAggregateShuffleSourceWorker::create(local_worker, local_jobs)
          .value();
  auto remote_publishing_worker =
      DistributedMutableVectorGroupedAggregateShuffleSourceWorker::create(remote_worker,
                                                                          remote_jobs)
          .value();
  auto remote_grouped_receiver =
      DistributedMutableVectorGroupedAggregateQueryReceiver::create(
          {.local_node_id = 3U, .authorizer = &authorizer, .worker = &remote_publishing_worker})
          .value();
  auto remote_control_server =
      DistributedMutableQueryControlTcpServer::start(
          {.listener = {},
           .tls = server_tls(),
           .authenticator = &remote_node_authenticator,
           .mutable_receiver = &remote_receivers.mutable_receiver,
           .mutable_grouped_receiver = &remote_grouped_receiver,
           .read_authority_receiver = &remote_receivers.authority_receiver,
           .grouped_shuffle_job_service = &remote_jobs,
           .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000}},
           .maximum_connections = 4U,
           .maximum_accepts_per_poll = 4U})
          .value();
  const DistributedQueryNodeRoute local_route{
      .node_id = 2U, .endpoints = {{{127U, 0U, 0U, 1U}, 9123U}}, .tls_context = &client_context};
  const DistributedQueryNodeRoute remote_route{
      .node_id = 3U,
      .endpoints = {remote_control_server.bound_endpoint()},
      .tls_context = &client_context};
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
  auto execution = DistributedMutableVectorGroupedAggregateShuffleJobExecution::create(
      2U, proof.fragments, keys(), aggregates(),
      {.worker_transport = {.authenticator = &coordinator_authenticator,
                            .node_authorizer = &authorizer,
                            .local_node_id = 2U,
                            .local_worker = &local_publishing_worker,
                            .routes = {local_route, remote_route},
                            .carrier_limits = {},
                            .connect_timeout = std::chrono::milliseconds{1000},
                            .execution_deadline = deadline,
                            .maximum_rebindings = 0U},
       .reducers = {.coordinator_node_id = 2U,
                    .reducer_control_routes = {local_route, remote_route},
                    .authenticator = &coordinator_authenticator,
                    .node_authorizer = &authorizer,
                    .local_reducer_job_service = &local_jobs,
                    .carrier_limits = control_limits(),
                    .connect_timeout = std::chrono::milliseconds{1000},
                    .prepare_retry = {.maximum_attempts = 4U,
                                      .initial_backoff = std::chrono::milliseconds{1},
                                      .maximum_backoff = std::chrono::milliseconds{4}},
                    .route_install_retry = {.maximum_attempts = 4U,
                                            .initial_backoff = std::chrono::milliseconds{1},
                                            .maximum_backoff = std::chrono::milliseconds{4}},
                    .seal_retry = {.maximum_attempts = 8U,
                                   .initial_backoff = std::chrono::milliseconds{1},
                                   .maximum_backoff = std::chrono::milliseconds{4}},
                    .lease_retry = {.maximum_attempts = 4U,
                                    .initial_backoff = std::chrono::milliseconds{1},
                                    .maximum_backoff = std::chrono::milliseconds{4}},
                    .reducer_execution_timeout = std::chrono::seconds{5},
                    .lease_duration = std::chrono::milliseconds{200},
                    .lease_renew_interval = std::chrono::milliseconds{20},
                    .execution_deadline = deadline,
                    .result = {.listener = {},
                               .tls = server_tls(),
                               .authenticator = &coordinator_authenticator,
                               .node_authorizer = &authorizer,
                               .coordinator_node_id = 2U,
                               .carrier_limits = result_limits(),
                               .maximum_retained_server_streams = 2U,
                               .maximum_accepts_per_poll = 2U}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  for (std::size_t iteration = 0U; iteration < 16384U; ++iteration) {
    const common::Status execution_progress = execution->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(execution_progress.is_ok()) << execution_progress.to_string();
    const common::Status remote_progress =
        remote_control_server.poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(remote_progress.is_ok()) << remote_progress.to_string();
    ASSERT_TRUE(local_jobs.poll_once(std::chrono::milliseconds{0}, std::chrono::steady_clock::now())
                    .is_ok());
    if (execution->state() ==
        DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kComplete) {
      break;
    }
  }
  ASSERT_EQ(execution->state(),
            DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kComplete)
      << execution->failure().to_string();
  const auto metrics = execution->metrics();
  EXPECT_EQ(metrics.workers.local_completed_attempts, 1U);
  EXPECT_EQ(metrics.workers.transport_completed_attempts, 1U);
  EXPECT_EQ(metrics.reducers.prepared_reducers, 2U);
  EXPECT_EQ(metrics.reducers.route_installed_reducers, 2U);
  EXPECT_EQ(metrics.reducers.sealed_reducers, 2U);
  EXPECT_EQ(metrics.reducers.result.collector.accepted_partitions, 2U);
  EXPECT_EQ(local_jobs.metrics().submitted_source_tablets, 1U);
  EXPECT_EQ(remote_jobs.metrics().submitted_source_tablets, 1U);
  auto result = execution->take_result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->encoded_batches.size(), 1U);
  auto decoded = network::decode_query_result_batch(result->encoded_batches.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  const auto* count = decoded->cell(0U, 1U);
  ASSERT_NE(count, nullptr);
  EXPECT_EQ(count->value.front(), std::byte{2U});
}

TEST(DistributedMutableVectorGroupedAggregateShuffleJobExecutionTest,
     CancelsEveryReducerBeforePrepareCanBeAdmitted) {
  Proofs proof;
  Authorizer authorizer;
  UnusedReceivers receivers{authorizer};
  Authenticator control_server_authenticator{91U};
  Authenticator control_client_authenticator{92U};
  Authenticator result_client_authenticator{93U};
  auto client_context = network::TlsClientContext::create(client_tls()).value();
  const std::array result_contexts{DistributedQueryNodeTlsContext{9U, &client_context}};
  auto job_service = DistributedVectorGroupedAggregateShuffleJobService::create(
                         {.local_node_id = 2U,
                          .shuffle_tls = server_tls(),
                          .shuffle_authenticator = &control_server_authenticator,
                          .result_authenticator = &result_client_authenticator,
                          .node_authorizer = &authorizer,
                          .result_tls_contexts = result_contexts,
                          .shuffle_carrier_limits = shuffle_limits(),
                          .result_retry_limits = {.retry = {.maximum_attempts = 1U},
                                                  .stream = result_limits().stream},
                          .result_carrier_limits = result_limits(),
                          .maximum_jobs = 1U,
                          .maximum_job_query_memory_bytes = 16U << 20U,
                          .maximum_retained_streams_per_job = 1U,
                          .maximum_accepts_per_job_poll = 1U,
                          .maximum_reducer_admissions_per_job_poll = 1U,
                          .maximum_cancel_tombstones = 1U})
                         .value();
  auto control_server =
      DistributedMutableQueryControlTcpServer::start(
          {.listener = {},
           .tls = server_tls(),
           .authenticator = &control_server_authenticator,
           .mutable_receiver = &receivers.mutable_receiver,
           .mutable_grouped_receiver = &receivers.grouped_receiver,
           .read_authority_receiver = &receivers.authority_receiver,
           .grouped_shuffle_job_service = &job_service,
           .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000}},
           .maximum_connections = 1U,
           .maximum_accepts_per_poll = 1U})
          .value();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
  auto execution =
      DistributedMutableVectorGroupedAggregateShuffleJobExecution::create(
          9U, proof.fragments, keys(), aggregates(),
          {.worker_transport = {.authenticator = &control_client_authenticator,
                                .node_authorizer = &authorizer,
                                .routes = {{.node_id = 2U,
                                            .endpoints = {control_server.bound_endpoint()},
                                            .tls_context = &client_context}},
                                .connect_timeout = std::chrono::milliseconds{1000},
                                .execution_deadline = deadline,
                                .maximum_rebindings = 0U},
           .reducers = {.coordinator_node_id = 9U,
                        .reducer_control_routes = {{.node_id = 2U,
                                                    .endpoints = {control_server.bound_endpoint()},
                                                    .tls_context = &client_context}},
                        .authenticator = &control_client_authenticator,
                        .node_authorizer = &authorizer,
                        .carrier_limits = control_limits(),
                        .connect_timeout = std::chrono::milliseconds{1000},
                        .cancel_retry = {.maximum_attempts = 2U,
                                         .initial_backoff = std::chrono::milliseconds{1},
                                         .maximum_backoff = std::chrono::milliseconds{2}},
                        .reducer_execution_timeout = std::chrono::seconds{3},
                        .execution_deadline = deadline,
                        .result = {.tls = server_tls(),
                                   .authenticator = &control_client_authenticator,
                                   .node_authorizer = &authorizer,
                                   .coordinator_node_id = 9U,
                                   .carrier_limits = result_limits(),
                                   .maximum_retained_server_streams = 1U,
                                   .maximum_accepts_per_poll = 1U}}})
          .value();
  ASSERT_TRUE(execution.cancel().is_ok());
  ASSERT_EQ(execution.state(),
            DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kCancelling);
  for (std::size_t iteration = 0U; iteration < 8192U; ++iteration) {
    ASSERT_TRUE(execution.poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(control_server.poll_once(std::chrono::milliseconds{1}).is_ok());
    if (execution.state() ==
        DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kCancelled)
      break;
  }
  ASSERT_EQ(execution.state(),
            DistributedMutableVectorGroupedAggregateShuffleJobExecutionState::kCancelled)
      << execution.failure().to_string()
      << " attempts=" << execution.metrics().reducers.control_attempts_started
      << " failed_attempts=" << execution.metrics().reducers.control_failed_attempts
      << " delivered=" << execution.metrics().reducers.cancelled_reducers
      << " delivery_failures=" << execution.metrics().reducers.cancel_delivery_failures
      << " service_cancels=" << job_service.metrics().cancel_requests;
  EXPECT_EQ(execution.failure().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(execution.metrics().reducers.cancelled_reducers, 1U);
  EXPECT_EQ(execution.metrics().reducers.cancel_delivery_failures, 0U);
  EXPECT_EQ(job_service.metrics().cancel_requests, 1U);
  EXPECT_EQ(job_service.metrics().cancel_tombstones, 1U);
  EXPECT_EQ(job_service.metrics().active_jobs, 0U);
}

TEST(DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionTest,
     RejectsIncompleteRouteCoverageBeforePublishingOrConnecting) {
  Proofs proof;
  Authorizer authorizer;
  Authenticator authenticator{92U};
  auto context = network::TlsClientContext::create(client_tls()).value();
  auto copied_authority =
      DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
          proof.fragments, keys(), aggregates())
          .value();
  auto different_proof = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::create(
      copied_authority, proof.finalization,
      {.coordinator_node_id = 9U,
       .reducer_control_routes = {{.node_id = 2U,
                                   .endpoints = {{{127U, 0U, 0U, 1U}, 9U}},
                                   .tls_context = &context}},
       .authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{1},
       .result = {.tls = server_tls(),
                  .authenticator = &authenticator,
                  .node_authorizer = &authorizer,
                  .coordinator_node_id = 9U}});
  ASSERT_FALSE(different_proof.has_value());
  EXPECT_EQ(different_proof.error().code(), common::StatusCode::kInvalidArgument);

  auto incomplete = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::create(
      proof.authority, proof.finalization,
      {.coordinator_node_id = 9U,
       .reducer_control_routes = {{.node_id = 3U,
                                   .endpoints = {{{127U, 0U, 0U, 1U}, 9U}},
                                   .tls_context = &context}},
       .authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{1},
       .result = {.tls = server_tls(),
                  .authenticator = &authenticator,
                  .node_authorizer = &authorizer,
                  .coordinator_node_id = 9U}});
  ASSERT_FALSE(incomplete.has_value());
  EXPECT_EQ(incomplete.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionTest,
     DiscardsAnAcceptedPrepareWhenAnotherReducerCannotBeAcquired) {
  Proofs proof{{fragment(2U, 2U), fragment(3U, 3U)}};
  Authorizer authorizer;
  UnusedReceivers receivers{authorizer};
  Authenticator control_server_authenticator{91U};
  Authenticator control_client_authenticator{92U};
  Authenticator result_client_authenticator{93U};
  auto client_context = network::TlsClientContext::create(client_tls()).value();
  const std::array result_contexts{DistributedQueryNodeTlsContext{9U, &client_context}};
  auto job_service = DistributedVectorGroupedAggregateShuffleJobService::create(
                         {.local_node_id = 2U,
                          .shuffle_tls = server_tls(),
                          .shuffle_authenticator = &control_server_authenticator,
                          .result_authenticator = &result_client_authenticator,
                          .node_authorizer = &authorizer,
                          .result_tls_contexts = result_contexts,
                          .shuffle_carrier_limits = shuffle_limits(),
                          .result_retry_limits = {.retry = {.maximum_attempts = 1U},
                                                  .stream = result_limits().stream},
                          .result_carrier_limits = result_limits(),
                          .maximum_jobs = 1U,
                          .maximum_job_query_memory_bytes = 16U << 20U,
                          .maximum_retained_streams_per_job = 2U,
                          .maximum_accepts_per_job_poll = 1U,
                          .maximum_reducer_admissions_per_job_poll = 2U})
                         .value();
  auto control_server =
      DistributedMutableQueryControlTcpServer::start(
          {.listener = {},
           .tls = server_tls(),
           .authenticator = &control_server_authenticator,
           .mutable_receiver = &receivers.mutable_receiver,
           .mutable_grouped_receiver = &receivers.grouped_receiver,
           .read_authority_receiver = &receivers.authority_receiver,
           .grouped_shuffle_job_service = &job_service,
           .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000}},
           .maximum_connections = 2U,
           .maximum_accepts_per_poll = 2U})
          .value();
  auto stalled_listener = network::TcpListener::bind().value();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
  auto coordinator = DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::create(
      proof.authority, proof.finalization,
      {.coordinator_node_id = 9U,
       .reducer_control_routes = {{.node_id = 2U,
                                   .endpoints = {control_server.bound_endpoint()},
                                   .tls_context = &client_context},
                                  {.node_id = 3U,
                                   .endpoints = {stalled_listener.bound_endpoint()},
                                   .tls_context = &client_context}},
       .authenticator = &control_client_authenticator,
       .node_authorizer = &authorizer,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{50},
                          .exchange_timeout = std::chrono::milliseconds{50}},
       .connect_timeout = std::chrono::milliseconds{50},
       .prepare_retry = {.maximum_attempts = 1U,
                         .initial_backoff = std::chrono::milliseconds{1},
                         .maximum_backoff = std::chrono::milliseconds{1}},
       .reducer_execution_timeout = std::chrono::seconds{2},
       .execution_deadline = deadline,
       .result = {.tls = server_tls(),
                  .authenticator = &control_client_authenticator,
                  .node_authorizer = &authorizer,
                  .coordinator_node_id = 9U,
                  .carrier_limits = result_limits(),
                  .maximum_retained_server_streams = 2U,
                  .maximum_accepts_per_poll = 2U}});
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    const auto progress = coordinator->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(progress.is_ok() || progress.code() == common::StatusCode::kCancelled ||
                progress.code() == common::StatusCode::kUnavailable)
        << progress.to_string();
    ASSERT_TRUE(control_server.poll_once(std::chrono::milliseconds{1}).is_ok());
    if (coordinator->state() ==
        DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kFailed)
      break;
    EXPECT_TRUE(coordinator->prepared_routes().empty());
  }
  ASSERT_EQ(coordinator->state(),
            DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kFailed)
      << coordinator->failure().to_string();
  EXPECT_TRUE(coordinator->prepared_routes().empty());
  EXPECT_EQ(coordinator->metrics().prepared_reducers, 0U);
  EXPECT_EQ(job_service.metrics().active_jobs, 1U);
  EXPECT_EQ(job_service.metrics().prepare_requests, 1U);
  EXPECT_EQ(job_service.metrics().cancel_requests, 1U);
  EXPECT_EQ(job_service.metrics().cancelled_jobs, 1U);
  EXPECT_EQ(coordinator->metrics().cancelled_reducers, 1U);
  EXPECT_EQ(coordinator->metrics().cancel_delivery_failures, 1U);
  EXPECT_EQ(coordinator->take_result().error().code(), common::StatusCode::kUnavailable);
}

} // namespace
} // namespace chronos::cluster
