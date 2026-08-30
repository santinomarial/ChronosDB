#include "chronos/cluster/distributed_mutable_query_control_tcp.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority_codec.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_coordinator_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_service.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_coordinator_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_execution.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::integration {
namespace {

constexpr raft::NodeId kCoordinatorNode = 9U;

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_TEST_TLS_FIXTURE_DIR} / name;
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

[[nodiscard]] schema::LogicalType i64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"region", string_type(), false}, {"count", i64_type(), false}}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const std::uint8_t tablet_seed,
                                                               const raft::NodeId node_id,
                                                               const std::uint8_t query_seed = 1U) {
  return {.query_id = uuid(query_seed),
          .database_id = id<manifest::DatabaseId>(8U),
          .table_id = id<schema::TableId>(9U),
          .tablet_id = id<schema::TabletId>(tablet_seed),
          .destination_schema_id = id<schema::SchemaId>(10U),
          .raft_group_id = uuid(static_cast<std::uint8_t>(tablet_seed + 20U)),
          .serving_node = node_id,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 3U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                   .group_key_input_indices = {0U},
                   .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}},
                   .order_keys = {{.output_index = 1U,
                                   .direction = query::PhysicalSortDirection::kDescending}},
                   .limit = 1U},
          .result_schema = result_schema()};
}

struct Proofs {
  explicit Proofs(const std::uint8_t query_seed = 1U)
      : fragments{fragment(2U, 3U, query_seed), fragment(3U, 4U, query_seed)}, authority([&] {
          auto derived =
              cluster::DistributedVectorGroupedAggregateShuffleAuthority::
                  create_from_mutable_fragments(
                      fragments,
                      std::array{query::VectorGroupKeyDefinition{0U, string_type(), false}},
                      std::array{query::VectorAggregateDefinition{
                          query::VectorAggregateOperation::kCountStar, std::nullopt}})
                      .value();
          auto encoded =
              cluster::encode_distributed_vector_grouped_aggregate_shuffle_authority(derived)
                  .value();
          return cluster::decode_distributed_vector_grouped_aggregate_shuffle_authority_exact(
                     encoded.bytes())
              .value();
        }()),
        finalization(
            *cluster::DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2::create(
                authority, fragments)) {}

  std::vector<query::DistributedMutableVectorFragment> fragments;
  cluster::DistributedVectorGroupedAggregateShuffleAuthority authority;
  cluster::DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2 finalization;
};

struct LeaseProofs {
  struct Inputs {
    std::uint8_t query_seed;
    std::span<const raft::NodeId> reducer_nodes;
  };

  explicit LeaseProofs(const Inputs inputs)
      : fragments(make_fragments(inputs)),
        authority(*cluster::DistributedVectorGroupedAggregateShuffleAuthority::
                      create_from_mutable_fragments(
                          fragments,
                          std::array{query::VectorGroupKeyDefinition{0U, string_type(), false}},
                          std::array{query::VectorAggregateDefinition{
                              query::VectorAggregateOperation::kCountStar, std::nullopt}})),
        finalization(
            *cluster::DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2::create(
                authority, fragments)) {}

  [[nodiscard]] static std::vector<query::DistributedMutableVectorFragment>
  make_fragments(const Inputs inputs) {
    std::vector<query::DistributedMutableVectorFragment> result;
    result.reserve(inputs.reducer_nodes.size());
    for (std::size_t index = 0U; index < inputs.reducer_nodes.size(); ++index) {
      result.push_back(fragment(static_cast<std::uint8_t>(index + 2U), inputs.reducer_nodes[index],
                                inputs.query_seed));
    }
    return result;
  }

  std::vector<query::DistributedMutableVectorFragment> fragments;
  cluster::DistributedVectorGroupedAggregateShuffleAuthority authority;
  cluster::DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2 finalization;
};

[[nodiscard]] std::array<std::byte, sizeof(std::uint64_t)> encoded_u64(std::uint64_t value) {
  std::array<std::byte, sizeof(std::uint64_t)> bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index)
    bytes[index] = static_cast<std::byte>((value >> (index * 8U)) & std::uint64_t{0xffU});
  return bytes;
}

[[nodiscard]] std::vector<std::byte> batch(const std::string& value, const std::uint64_t count) {
  const std::array columns{network::QueryResultColumn{"region", string_type(), false},
                           network::QueryResultColumn{"count", i64_type(), false}};
  const auto encoded_count = encoded_u64(count);
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{value})},
                         network::QueryResultCell{.value = encoded_count}};
  return network::encode_query_result_batch(1U, columns, cells).value();
}

[[nodiscard]] cluster::DistributedVectorGroupedAggregateShuffleResultTlsLimits carrier_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}};
}

[[nodiscard]] cluster::DistributedVectorGroupedAggregateShuffleTlsLimits shuffle_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}};
}

[[nodiscard]] cluster::DistributedVectorGroupedAggregateShuffleJobControlTlsLimits
job_control_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000}};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

private:
  std::uint64_t principal_{};
};

class Authorizer final : public cluster::ClusterNodePrincipalAuthorizer {
public:
  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return common::Result<bool>{
        (principal == 91U &&
         (node == 2U || node == 3U || node == 4U || node == kCoordinatorNode)) ||
        (principal == 92U && (node == 2U || node == 3U || node == kCoordinatorNode))};
  }
};

class UnusedMutableWorker final : public cluster::DistributedMutableVectorQueryWorkerService {
public:
  common::Result<std::vector<cluster::DistributedVectorResultExchangeMessage>>
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  execute(const query::DistributedMutableVectorFragment&) override {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "unused mutable worker was called"});
  }
};

class UnusedGroupedWorker final
    : public cluster::DistributedMutableVectorGroupedAggregateQueryWorkerService {
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

class UnusedAuthorityService final : public cluster::RaftReadAuthorityService {
public:
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  common::Result<cluster::RaftReadAuthority> acquire(const raft::GroupId&) override {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "unused authority service was called"});
  }
};

struct UnusedReceivers {
  UnusedReceivers(Authorizer& authorizer, const raft::NodeId local_node_id)
      : mutable_receiver(
            cluster::DistributedMutableVectorQueryReceiver::create({.local_node_id = local_node_id,
                                                                    .authorizer = &authorizer,
                                                                    .worker = &mutable_worker})
                .value()),
        grouped_receiver(cluster::DistributedMutableVectorGroupedAggregateQueryReceiver::create(
                             {.local_node_id = local_node_id,
                              .authorizer = &authorizer,
                              .worker = &grouped_worker})
                             .value()),
        authority_receiver(
            cluster::RaftReadAuthorityReceiver::create({.local_node_id = local_node_id,
                                                        .authorizer = &authorizer,
                                                        .service = &authority_service})
                .value()) {}

  UnusedMutableWorker mutable_worker;
  UnusedGroupedWorker grouped_worker;
  UnusedAuthorityService authority_service;
  cluster::DistributedMutableVectorQueryReceiver mutable_receiver;
  cluster::DistributedMutableVectorGroupedAggregateQueryReceiver grouped_receiver;
  cluster::RaftReadAuthorityReceiver authority_receiver;
};

template <typename Integer>
[[nodiscard]] std::optional<Integer> parse_integer(std::string_view text) {
  Integer value{};
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size())
    return std::nullopt;
  return value;
}

[[nodiscard]] int run_coordinator(const std::uint16_t port,
                                  const std::chrono::milliseconds timeout) {
  Proofs proofs;
  Authenticator authenticator{91U};
  Authorizer authorizer;
  auto execution =
      cluster::DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::create(
          proofs.authority, proofs.finalization,
          {.listener = {.bind_endpoint = {{127U, 0U, 0U, 1U}, port}},
           .tls = server_tls(),
           .authenticator = &authenticator,
           .node_authorizer = &authorizer,
           .coordinator_node_id = kCoordinatorNode,
           .carrier_limits = carrier_limits(),
           .maximum_retained_server_streams = 2U,
           .maximum_accepts_per_poll = 2U,
           .execution_deadline = std::chrono::steady_clock::now() + timeout});
  if (!execution.has_value()) {
    std::cerr << execution.error().to_string() << '\n';
    return 2;
  }
  std::cout << "READY " << execution->bound_endpoint().port << '\n' << std::flush;
  while (
      execution->state() ==
      cluster::DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kRunning) {
    const common::Status status = execution->poll_once(std::chrono::milliseconds{10});
    if (!status.is_ok()) {
      if (status.code() == common::StatusCode::kCancelled) {
        std::cout << "CANCELLED\n" << std::flush;
        return 3;
      }
      std::cerr << status.to_string() << '\n';
      return 4;
    }
  }
  auto result = execution->take_result();
  if (!result.has_value() || result->encoded_batches.size() != 1U) {
    std::cerr << "final grouped result is unavailable\n";
    return 5;
  }
  auto decoded = network::decode_query_result_batch(result->encoded_batches.front());
  if (!decoded.has_value() || decoded->row_count() != 1U) {
    std::cerr << "final grouped result is invalid\n";
    return 6;
  }
  const auto* region = decoded->cell(0U, 0U);
  const auto* count = decoded->cell(0U, 1U);
  const std::string wanted{"west"};
  const auto wanted_bytes = std::as_bytes(std::span{wanted});
  if (region == nullptr || count == nullptr ||
      !std::equal(region->value.begin(), region->value.end(), wanted_bytes.begin(),
                  wanted_bytes.end()) ||
      count->value.size() != sizeof(std::uint64_t) || count->value.front() != std::byte{2U}) {
    std::cerr << "final grouped result has unexpected values\n";
    return 7;
  }
  std::cout << "RESULT west 2\n" << std::flush;
  return 0;
}

enum class JobReducerPauseBoundary : std::uint8_t {
  kNone = 0,
  kBeforeControl = 1,
  kAfterPrepare = 2,
  kAfterRoutes = 3,
  kAfterActivation = 4,
};

[[nodiscard]] int run_job_reducer(const raft::NodeId local_node_id,
                                  const JobReducerPauseBoundary pause_boundary) {
  Authenticator coordinator_authenticator{91U};
  Authorizer authorizer;
  UnusedReceivers receivers{authorizer, local_node_id};
  auto client_context = network::TlsClientContext::create(client_tls());
  if (!client_context.has_value()) {
    std::cerr << client_context.error().to_string() << '\n';
    return 2;
  }
  const std::array result_contexts{
      cluster::DistributedQueryNodeTlsContext{2U, &*client_context},
      cluster::DistributedQueryNodeTlsContext{3U, &*client_context},
      cluster::DistributedQueryNodeTlsContext{kCoordinatorNode, &*client_context}};
  auto job_config = cluster::DistributedVectorGroupedAggregateShuffleJobServiceConfig{};
  job_config.local_node_id = local_node_id;
  job_config.shuffle_tls = server_tls();
  job_config.shuffle_authenticator = &coordinator_authenticator;
  job_config.result_authenticator = &coordinator_authenticator;
  job_config.node_authorizer = &authorizer;
  job_config.result_tls_contexts = result_contexts;
  job_config.shuffle_carrier_limits = shuffle_limits();
  job_config.result_retry_limits = {.retry = {.maximum_attempts = 2U,
                                              .initial_backoff = std::chrono::milliseconds{1},
                                              .maximum_backoff = std::chrono::milliseconds{2}},
                                    .stream = carrier_limits().stream};
  job_config.result_carrier_limits = carrier_limits();
  job_config.maximum_jobs = 2U;
  job_config.maximum_job_query_memory_bytes = 16U << 20U;
  job_config.maximum_retained_streams_per_job = 1U;
  job_config.maximum_accepts_per_job_poll = 1U;
  job_config.maximum_reducer_admissions_per_job_poll = 1U;
  auto jobs =
      cluster::DistributedVectorGroupedAggregateShuffleJobService::create(std::move(job_config));
  if (!jobs.has_value()) {
    std::cerr << jobs.error().to_string() << '\n';
    return 3;
  }
  const auto make_server_config = [&]() {
    auto config = cluster::DistributedMutableQueryControlTcpServerConfig{};
    config.tls = server_tls();
    config.authenticator = &coordinator_authenticator;
    config.mutable_receiver = &receivers.mutable_receiver;
    config.mutable_grouped_receiver = &receivers.grouped_receiver;
    config.read_authority_receiver = &receivers.authority_receiver;
    config.grouped_shuffle_job_service = &*jobs;
    config.carrier_limits.handshake_timeout = std::chrono::milliseconds{1000};
    config.carrier_limits.exchange_timeout = std::chrono::milliseconds{1000};
    config.maximum_connections = 4U;
    config.maximum_accepts_per_poll = 4U;
    return config;
  };
  auto started = cluster::DistributedMutableQueryControlTcpServer::start(make_server_config());
  if (!started.has_value()) {
    std::cerr << started.error().to_string() << '\n';
    return 4;
  }
  auto server = std::move(*started);
  std::cout << "JOB_REDUCER_READY " << server.bound_endpoint().port << '\n' << std::flush;
  cluster::DistributedVectorGroupedAggregateShuffleJobServiceMetrics observed;
  bool paused{};
  const auto pause_and_restart = [&](const std::string_view label) -> common::Status {
    std::cout << label << '\n' << std::flush;
    if (::raise(SIGSTOP) != 0) {
      return {common::StatusCode::kIoError,
              "stopping reducer process at the requested boundary failed"};
    }
    common::Status shutdown = server.shutdown();
    if (!shutdown.is_ok())
      return shutdown;
    auto restarted = cluster::DistributedMutableQueryControlTcpServer::start(make_server_config());
    if (!restarted.has_value())
      return restarted.error();
    server = std::move(*restarted);
    std::cout << "JOB_REDUCER_RESUMED " << server.bound_endpoint().port << '\n' << std::flush;
    return common::Status::ok();
  };
  if (pause_boundary == JobReducerPauseBoundary::kBeforeControl) {
    paused = true;
    const common::Status resumed = pause_and_restart("PAUSED_BEFORE_CONTROL");
    if (!resumed.is_ok()) {
      std::cerr << resumed.to_string() << '\n';
      return 5;
    }
  }
  for (;;) {
    const common::Status progress = server.poll_once(std::chrono::milliseconds{5});
    if (!progress.is_ok()) {
      std::cerr << progress.to_string() << '\n';
      return 5;
    }
    const auto current = jobs->metrics();
    if (current.prepare_requests != observed.prepare_requests)
      std::cout << "PREPARED " << current.prepare_requests << '\n' << std::flush;
    if (current.route_install_requests != observed.route_install_requests)
      std::cout << "ROUTED " << current.route_install_requests << '\n' << std::flush;
    if (current.lease_activations != observed.lease_activations)
      std::cout << "ACTIVE " << current.lease_activations << '\n' << std::flush;
    if (current.lease_renewals != observed.lease_renewals)
      std::cout << "RENEWED " << current.lease_renewals << '\n' << std::flush;
    if (current.lease_expirations != observed.lease_expirations)
      std::cout << "EXPIRED " << current.lease_expirations << '\n' << std::flush;
    if (current.execution_expirations != observed.execution_expirations)
      std::cout << "EXECUTION_EXPIRED " << current.execution_expirations << '\n' << std::flush;
    const auto server_metrics = server.metrics();
    const bool prepare_boundary = pause_boundary == JobReducerPauseBoundary::kAfterPrepare &&
                                  current.prepare_requests == 1U &&
                                  current.route_install_requests == 0U &&
                                  server_metrics.completed_grouped_shuffle_job_controls >= 1U;
    const bool routes_boundary = pause_boundary == JobReducerPauseBoundary::kAfterRoutes &&
                                 current.route_install_requests == 1U &&
                                 current.lease_activations == 0U &&
                                 server_metrics.completed_grouped_shuffle_job_controls >= 2U;
    const bool activation_boundary = pause_boundary == JobReducerPauseBoundary::kAfterActivation &&
                                     current.lease_activations == 1U &&
                                     current.lease_renewals == 0U &&
                                     server_metrics.completed_grouped_shuffle_job_controls >= 3U;
    observed = current;
    if (!paused && (prepare_boundary || routes_boundary || activation_boundary)) {
      paused = true;
      const std::string_view label = prepare_boundary  ? "PAUSED_AFTER_PREPARE"
                                     : routes_boundary ? "PAUSED_AFTER_ROUTES"
                                                       : "PAUSED_AFTER_ACTIVATION";
      const common::Status resumed = pause_and_restart(label);
      if (!resumed.is_ok()) {
        std::cerr << resumed.to_string() << '\n';
        return 6;
      }
    }
  }
}

struct JobReducerEndpoint {
  raft::NodeId node_id{};
  std::uint16_t port{};
};

struct JobCoordinatorInvocation {
  std::vector<JobReducerEndpoint> reducers;
  std::uint8_t query_seed{};
  std::chrono::milliseconds reducer_execution_timeout{};
  std::chrono::milliseconds lease_duration{};
  bool cancel_after_renewal{};
};

[[nodiscard]] int run_job_coordinator(const JobCoordinatorInvocation& invocation) {
  std::vector<raft::NodeId> reducer_nodes;
  reducer_nodes.reserve(invocation.reducers.size());
  for (const auto& reducer : invocation.reducers)
    reducer_nodes.push_back(reducer.node_id);
  LeaseProofs proofs{
      LeaseProofs::Inputs{.query_seed = invocation.query_seed, .reducer_nodes = reducer_nodes}};
  Authenticator reducer_authenticator{92U};
  Authorizer authorizer;
  auto client_context = network::TlsClientContext::create(client_tls());
  if (!client_context.has_value()) {
    std::cerr << client_context.error().to_string() << '\n';
    return 2;
  }
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
  std::vector<cluster::DistributedQueryNodeRoute> reducer_routes;
  reducer_routes.reserve(invocation.reducers.size());
  for (const auto& reducer : invocation.reducers) {
    reducer_routes.push_back({.node_id = reducer.node_id,
                              .endpoints = {{{127U, 0U, 0U, 1U}, reducer.port}},
                              .tls_context = &*client_context});
  }
  const cluster::DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits retry{
      .maximum_attempts = 4U,
      .initial_backoff = std::chrono::milliseconds{1},
      .maximum_backoff = std::chrono::milliseconds{4}};
  auto result_config =
      cluster::DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionConfig{};
  result_config.tls = server_tls();
  result_config.authenticator = &reducer_authenticator;
  result_config.node_authorizer = &authorizer;
  result_config.coordinator_node_id = kCoordinatorNode;
  result_config.carrier_limits = carrier_limits();
  result_config.maximum_retained_server_streams = invocation.reducers.size();
  result_config.maximum_accepts_per_poll = invocation.reducers.size();
  result_config.execution_deadline = deadline;

  auto coordinator_config =
      cluster::DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig{};
  coordinator_config.coordinator_node_id = kCoordinatorNode;
  coordinator_config.reducer_control_routes = std::move(reducer_routes);
  coordinator_config.authenticator = &reducer_authenticator;
  coordinator_config.node_authorizer = &authorizer;
  coordinator_config.carrier_limits = job_control_limits();
  coordinator_config.connect_timeout = std::chrono::milliseconds{1000};
  coordinator_config.prepare_retry = retry;
  coordinator_config.route_install_retry = retry;
  coordinator_config.seal_retry = retry;
  coordinator_config.cancel_retry = retry;
  coordinator_config.lease_retry = retry;
  coordinator_config.reducer_execution_timeout = invocation.reducer_execution_timeout;
  coordinator_config.lease_duration = invocation.lease_duration;
  coordinator_config.lease_renew_interval = std::chrono::milliseconds{20};
  coordinator_config.execution_deadline = deadline;
  coordinator_config.result = std::move(result_config);
  auto coordinator =
      cluster::DistributedVectorGroupedAggregateShuffleJobCoordinatorExecution::create(
          proofs.authority, proofs.finalization, std::move(coordinator_config));
  if (!coordinator.has_value()) {
    std::cerr << coordinator.error().to_string() << '\n';
    return 3;
  }
  while (coordinator->metrics().lease_rounds_completed < 2U) {
    const common::Status progress = coordinator->poll_once(std::chrono::milliseconds{5});
    if (!progress.is_ok()) {
      std::cerr << progress.to_string() << '\n';
      return 4;
    }
    if (coordinator->state() ==
        cluster::DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kFailed) {
      std::cerr << coordinator->failure().to_string() << '\n';
      return 5;
    }
  }
  std::cout << "LEASED " << static_cast<unsigned int>(invocation.query_seed) << '\n' << std::flush;
  if (!invocation.cancel_after_renewal) {
    for (;;) {
      const common::Status progress = coordinator->poll_once(std::chrono::milliseconds{5});
      if (!progress.is_ok()) {
        std::cerr << progress.to_string() << '\n';
        return 6;
      }
    }
  }
  const common::Status requested = coordinator->cancel();
  if (!requested.is_ok()) {
    std::cerr << requested.to_string() << '\n';
    return 7;
  }
  while (
      coordinator->state() ==
      cluster::DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelling) {
    const common::Status progress = coordinator->poll_once(std::chrono::milliseconds{5});
    const bool terminal_cancel =
        progress.code() == common::StatusCode::kCancelled &&
        coordinator->state() ==
            cluster::DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::
                kCancelled;
    if (!progress.is_ok() && !terminal_cancel) {
      std::cerr << progress.to_string() << '\n';
      return 8;
    }
  }
  if (coordinator->state() !=
      cluster::DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionState::kCancelled) {
    std::cerr << coordinator->failure().to_string() << '\n';
    return 9;
  }
  std::cout << "CANCELLED " << static_cast<unsigned int>(invocation.query_seed) << '\n'
            << std::flush;
  return 0;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] int run_reducer(const std::uint16_t coordinator_port,
                              const std::uint16_t refused_port, const std::uint32_t partition_id,
                              const std::string& value, const std::uint64_t count) {
  Proofs proofs;
  Authenticator authenticator{92U};
  Authorizer authorizer;
  auto context = network::TlsClientContext::create(client_tls());
  if (!context.has_value()) {
    std::cerr << context.error().to_string() << '\n';
    return 2;
  }
  const auto source = proofs.authority.destination_node(partition_id);
  if (!source.has_value())
    return 3;
  std::vector<std::vector<std::byte>> batches;
  batches.push_back(batch(value, count));
  auto retry = cluster::DistributedVectorGroupedAggregateShuffleResultRetry::create(
      proofs.authority, proofs.finalization.result_schema(),
      {.partition_id = partition_id,
       .source_node_id = *source,
       .coordinator_node_id = kCoordinatorNode},
      std::move(batches),
      {.retry = {.maximum_attempts = 8U,
                 .initial_backoff = std::chrono::milliseconds{20},
                 .maximum_backoff = std::chrono::milliseconds{50}},
       .stream = carrier_limits().stream});
  if (!retry.has_value()) {
    std::cerr << retry.error().to_string() << '\n';
    return 4;
  }
  std::vector<network::Ipv4Endpoint> endpoints;
  if (refused_port != 0U)
    endpoints.push_back({{127U, 0U, 0U, 1U}, refused_port});
  endpoints.push_back({{127U, 0U, 0U, 1U}, coordinator_port});
  std::vector<cluster::DistributedVectorGroupedAggregateShuffleResultRetry> retries;
  retries.push_back(std::move(*retry));
  auto execution = cluster::DistributedVectorGroupedAggregateShuffleResultTcpExecution::create(
      proofs.authority, proofs.finalization.result_schema(), std::move(retries),
      {.authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .routes = {{.node_id = kCoordinatorNode,
                   .endpoints = std::move(endpoints),
                   .tls_context = &*context}},
       .carrier_limits = carrier_limits(),
       .connect_timeout = std::chrono::milliseconds{500},
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}});
  if (!execution.has_value()) {
    std::cerr << execution.error().to_string() << '\n';
    return 5;
  }
  while (execution->state() ==
         cluster::DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kRunning) {
    const common::Status status = execution->poll_once(std::chrono::milliseconds{10});
    if (!status.is_ok()) {
      std::cerr << status.to_string() << '\n';
      return 6;
    }
  }
  if (execution->state() !=
      cluster::DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kComplete) {
    std::cerr << execution->failure().to_string() << '\n';
    return 7;
  }
  const auto metrics = execution->metrics();
  std::cout << "SENT " << partition_id << " attempts=" << metrics.attempts_started
            << " retries=" << metrics.retries_started << '\n'
            << std::flush;
  return 0;
}

[[nodiscard]] int run_stalled_reducer(const std::uint32_t partition_id) {
  Proofs proofs;
  if (!proofs.authority.destination_node(partition_id).has_value())
    return 2;
  std::cout << "STALLING " << partition_id << '\n' << std::flush;
  for (;;)
    std::this_thread::sleep_for(std::chrono::seconds{1});
}

} // namespace
} // namespace chronos::integration

int main(const int argc, char** argv) {
  try {
    using namespace chronos::integration;
    if (argc == 4 && std::string_view{argv[1]} == "job-reducer") {
      const auto local_node_id = parse_integer<chronos::raft::NodeId>(argv[2]);
      const std::string_view boundary{argv[3]};
      if (!local_node_id.has_value() || (*local_node_id != 2U && *local_node_id != 3U))
        return 64;
      if (boundary == "none")
        return run_job_reducer(*local_node_id, JobReducerPauseBoundary::kNone);
      if (boundary == "before-control")
        return run_job_reducer(*local_node_id, JobReducerPauseBoundary::kBeforeControl);
      if (boundary == "after-prepare")
        return run_job_reducer(*local_node_id, JobReducerPauseBoundary::kAfterPrepare);
      if (boundary == "after-routes")
        return run_job_reducer(*local_node_id, JobReducerPauseBoundary::kAfterRoutes);
      if (boundary == "after-activation")
        return run_job_reducer(*local_node_id, JobReducerPauseBoundary::kAfterActivation);
    }
    if (argc == 7 && std::string_view{argv[1]} == "job-coordinator") {
      const auto port = parse_integer<std::uint16_t>(argv[2]);
      const auto query_seed = parse_integer<std::uint32_t>(argv[3]);
      const auto execution = parse_integer<std::uint64_t>(argv[4]);
      const auto lease = parse_integer<std::uint64_t>(argv[5]);
      const std::string_view mode{argv[6]};
      if (port.has_value() && query_seed.has_value() && *query_seed != 0U && *query_seed <= 255U &&
          execution.has_value() && *execution != 0U && lease.has_value() && *lease != 0U &&
          (mode == "hold" || mode == "cancel")) {
        return run_job_coordinator(
            {.reducers = {{.node_id = 2U, .port = *port}},
             .query_seed = static_cast<std::uint8_t>(*query_seed),
             .reducer_execution_timeout = std::chrono::milliseconds{*execution},
             .lease_duration = std::chrono::milliseconds{*lease},
             .cancel_after_renewal = mode == "cancel"});
      }
    }
    if (argc == 8 && std::string_view{argv[1]} == "job-coordinator-two") {
      const auto first_port = parse_integer<std::uint16_t>(argv[2]);
      const auto second_port = parse_integer<std::uint16_t>(argv[3]);
      const auto query_seed = parse_integer<std::uint32_t>(argv[4]);
      const auto execution = parse_integer<std::uint64_t>(argv[5]);
      const auto lease = parse_integer<std::uint64_t>(argv[6]);
      const std::string_view mode{argv[7]};
      if (first_port.has_value() && second_port.has_value() && query_seed.has_value() &&
          *query_seed != 0U && *query_seed <= 255U && execution.has_value() && *execution != 0U &&
          lease.has_value() && *lease != 0U && (mode == "hold" || mode == "cancel")) {
        return run_job_coordinator(
            {.reducers = {{.node_id = 2U, .port = *first_port},
                          {.node_id = 3U, .port = *second_port}},
             .query_seed = static_cast<std::uint8_t>(*query_seed),
             .reducer_execution_timeout = std::chrono::milliseconds{*execution},
             .lease_duration = std::chrono::milliseconds{*lease},
             .cancel_after_renewal = mode == "cancel"});
      }
    }
    if (argc == 3 && std::string_view{argv[1]} == "stall-reducer") {
      const auto partition = parse_integer<std::uint32_t>(argv[2]);
      if (partition.has_value())
        return run_stalled_reducer(*partition);
    }
    if (argc == 4 && std::string_view{argv[1]} == "coordinator") {
      const auto port = parse_integer<std::uint16_t>(argv[2]);
      const auto timeout = parse_integer<std::uint64_t>(argv[3]);
      if (port.has_value() && timeout.has_value())
        return run_coordinator(*port, std::chrono::milliseconds{*timeout});
    }
    if (argc == 7 && std::string_view{argv[1]} == "reducer") {
      const auto coordinator_port = parse_integer<std::uint16_t>(argv[2]);
      const auto refused_port = parse_integer<std::uint16_t>(argv[3]);
      const auto partition = parse_integer<std::uint32_t>(argv[4]);
      const auto count = parse_integer<std::uint64_t>(argv[6]);
      if (coordinator_port.has_value() && refused_port.has_value() && partition.has_value() &&
          count.has_value()) {
        return run_reducer(*coordinator_port, *refused_port, *partition, argv[5], *count);
      }
    }
    std::cerr << "usage: grouped_shuffle_result_process_child "
                 "coordinator PORT TIMEOUT_MS | reducer PORT REFUSED_PORT PARTITION VALUE COUNT | "
                 "stall-reducer PARTITION | "
                 "job-reducer NODE none|before-control|after-prepare|after-routes|after-activation "
                 "| job-coordinator REDUCER_PORT QUERY_SEED EXECUTION_MS LEASE_MS hold|cancel | "
                 "job-coordinator-two REDUCER2_PORT REDUCER3_PORT QUERY_SEED EXECUTION_MS "
                 "LEASE_MS hold|cancel\n";
    return 64;
  } catch (const std::exception& error) {
    std::cerr << "grouped shuffle process child failed: " << error.what() << '\n';
    return 70;
  } catch (...) {
    std::cerr << "grouped shuffle process child failed with an unknown exception\n";
    return 71;
  }
}
