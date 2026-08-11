#include "chronos/cluster/distributed_query_execution.hpp"
#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_query_tcp_server.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-query-execution-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
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

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

void write_file(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  for (const std::byte value : bytes)
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
}

[[nodiscard]] schema::TableSchema schema_value() {
  const auto event = id<schema::ColumnId>(5U);
  const auto value = id<schema::ColumnId>(6U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(
                        value, "value",
                        schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(),
                        true)
                        .value());
  return schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
                                     schema::SchemaVersion::initial(), std::nullopt,
                                     std::move(columns),
                                     {.event_time_column = event,
                                      .physical_ordering_key = {event},
                                      .partition_columns = {event},
                                      .shard_key = {event},
                                      .deduplication_key = {event}})
      .value();
}

struct ExecutionInput {
  query::DistributedAggregatePlan plan;
  std::vector<query::DistributedReadAdmission> admissions;
  query::CompatibleDistributedAggregateSnapshot snapshot;
};

[[nodiscard]] common::Result<ExecutionInput> make_input(const TemporaryDirectory& directory) {
  const schema::TableSchema schema = schema_value();
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema).value();
  const std::array tablets{id<schema::TabletId>(3U), id<schema::TabletId>(9U)};
  const std::array groups{uuid(8U), uuid(10U)};
  const std::array positions{10U, 20U};
  std::vector<manifest::TemporalTabletDescriptor> descriptors;
  for (std::size_t index = 0U; index < tablets.size(); ++index) {
    descriptors.push_back({.table_id = schema.table_id(),
                           .tablet_id = tablets[index],
                           .recovery_schema_id = schema.schema_id(),
                           .recovery_schema_version = schema.version(),
                           .source_id = groups[index],
                           .durable_position = positions[index],
                           .reclaim_position = 0U,
                           .first_part_index = 0U,
                           .part_count = 0U,
                           .durable_version_count = 0U,
                           .commit_source = manifest::ManifestCommitSource::kRaft});
  }
  auto encoded = manifest::encode_manifest_v2_temporal({.generation = 1U,
                                                        .database_id = id<manifest::DatabaseId>(1U),
                                                        .wal_reclaim_checkpoint = std::nullopt,
                                                        .tablets = descriptors,
                                                        .parts = {},
                                                        .retries = {}});
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  if (!std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName) ||
      !std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "cannot create execution fixture"});
  }
  write_file(directory.path() / manifest::kManifestDirectoryName / manifest::kManifestLockFileName,
             {});
  write_file(directory.path() / manifest::kManifestDirectoryName /
                 *manifest::manifest_file_name(1U),
             encoded->bytes());
  auto storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  if (!storage.has_value())
    return common::make_unexpected(storage.error());
  std::vector<manifest::TabletSchemaBinding> schema_bindings;
  std::vector<manifest::TemporalTabletSourceBinding> source_bindings;
  for (std::size_t index = 0U; index < tablets.size(); ++index) {
    schema_bindings.push_back({tablets[index], std::cref(lineage)});
    source_bindings.push_back(
        {tablets[index], manifest::ManifestCommitSource::kRaft, groups[index]});
  }
  auto loaded = storage->load_selected_temporal_manifest(
      {.expected_database_id = id<manifest::DatabaseId>(1U),
       .schema_bindings = schema_bindings,
       .source_bindings = source_bindings});
  if (!loaded.has_value())
    return common::make_unexpected(loaded.error());
  auto owner =
      std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
  auto publisher = manifest::TemporalDatabaseStoragePublisher::create(owner, schema_bindings);
  if (!publisher.has_value())
    return common::make_unexpected(publisher.error());
  auto snapshot = publisher->snapshot();
  if (!snapshot.has_value())
    return common::make_unexpected(snapshot.error());

  query::DistributedAggregatePlan plan{
      .query_id = uuid(7U),
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .fragments = {{tablets[0], 0, 100, 11U, 10U, 10U}, {tablets[1], 101, 200, 12U, 20U, 20U}}};
  std::vector<query::DistributedReadAdmission> admissions{
      {tablets[0], 11U, 10U, 10U, raft::ReadBarrier{2U, 3U, 10U}},
      {tablets[1], 12U, 20U, 20U, raft::ReadBarrier{2U, 4U, 20U}}};
  const std::array placements{
      raft::TabletPlacementMetadata{schema.table_id(), tablets[0], 12U, {11U, 13U}, 11U},
      raft::TabletPlacementMetadata{schema.table_id(), tablets[1], 13U, {12U, 14U}, 12U}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};
  const std::array bindings{query::DistributedAggregateSnapshotFragmentBinding{
                                std::cref(admissions[0]), std::cref(schema), groups[0],
                                std::cref(placements[0]), projection, 1U, std::nullopt},
                            query::DistributedAggregateSnapshotFragmentBinding{
                                std::cref(admissions[1]), std::cref(schema), groups[1],
                                std::cref(placements[1]), projection, 1U, std::nullopt}};
  auto compatible = query::bind_compatible_distributed_aggregate_snapshot(
      plan, std::move(*snapshot), bindings,
      {.maximum_fragments = 2U, .maximum_total_projection_ordinals = 4U});
  if (!compatible.has_value())
    return common::make_unexpected(compatible.error());
  return ExecutionInput{std::move(plan), std::move(admissions), std::move(*compatible)};
}

[[nodiscard]] query::ExchangeMessage message(const schema::TabletId& tablet, const double value) {
  query::MergeableAggregateState partial;
  EXPECT_TRUE(partial.add(value).is_ok());
  return {uuid(7U), tablet, 1U, partial, true};
}

[[nodiscard]] std::filesystem::path tls_fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig execution_tls_server_config() {
  return {.certificate_chain_file = tls_fixture("server.pem").string(),
          .private_key_file = tls_fixture("server-key.pem").string(),
          .trust_store_file = tls_fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig execution_tls_client_config() {
  return {.certificate_chain_file = tls_fixture("client.pem").string(),
          .private_key_file = tls_fixture("client-key.pem").string(),
          .trust_store_file = tls_fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

class ExecutionAuthenticator final : public network::ConnectionAuthenticator {
public:
  explicit ExecutionAuthenticator(const std::uint64_t principal_id) : principal_id_(principal_id) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_id_};
  }

private:
  std::uint64_t principal_id_{};
};

class ExecutionNodeAuthorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId node_id) const override {
    return (principal_id == 91U && node_id == 1U) ||
           (principal_id == 92U && (node_id == 11U || node_id == 12U));
  }
};

class ExecutionWorker final : public DistributedQueryWorkerService {
public:
  ExecutionWorker(const double value, const bool fail_first) noexcept
      : value_(value), fail_first_(fail_first) {}

  common::Result<query::ExchangeMessage>
  execute(const query::DistributedAggregateFragmentDispatch& dispatch) override {
    ++calls;
    if (fail_first_ && calls == 1U) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnavailable, "injected retryable worker failure"});
    }
    query::MergeableAggregateState partial;
    const common::Status added = partial.add(value_);
    if (!added.is_ok())
      return common::make_unexpected(added);
    return query::ExchangeMessage{dispatch.fragment.query_id, dispatch.fragment.tablet_id, 1U,
                                  partial, true};
  }

  std::size_t calls{};

private:
  double value_{};
  bool fail_first_{};
};

[[nodiscard]] DistributedQueryTcpServerConfig
execution_server_config(ExecutionAuthenticator& authenticator, DistributedQueryReceiver& receiver) {
  return {.listener = {},
          .tls = execution_tls_server_config(),
          .authenticator = &authenticator,
          .receiver = &receiver,
          .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                             .exchange_timeout = std::chrono::milliseconds{1000}},
          .maximum_connections = 8U,
          .maximum_accepts_per_poll = 8U};
}

TEST(DistributedQueryExecutionTest, DeliversEveryTerminalResultExactlyOnceAndFinishes) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const auto tablets =
      std::array{input->plan.fragments[0].tablet_id, input->plan.fragments[1].tablet_id};
  auto execution = DistributedQueryExecution::create(
      1U, std::move(input->plan), std::move(input->admissions), std::move(input->snapshot));
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->snapshot().snapshot().generation(), 1U);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  const auto now = DistributedQueryExecution::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablets[0], now).has_value());
  ASSERT_TRUE(execution->begin_attempt(tablets[1], now).has_value());

  const auto first =
      encode_distributed_query_response_v1({11U, 1U, uuid(7U), tablets[0], common::StatusCode::kOk,
                                            message(tablets[0], 2.5), std::nullopt})
          .value();
  ASSERT_TRUE(execution->accept_response(tablets[0], first, now).is_ok());
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  const auto second =
      encode_distributed_query_response_v1({12U, 1U, uuid(7U), tablets[1], common::StatusCode::kOk,
                                            message(tablets[1], 3.5), std::nullopt})
          .value();
  ASSERT_TRUE(execution->accept_response(tablets[1], second, now).is_ok());
  const auto result = execution->finish();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->count, 2U);
  EXPECT_EQ(result->sum, 6.0);
  EXPECT_EQ(execution->accept_response(tablets[1], second, now).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(execution->begin_attempt(id<schema::TabletId>(99U), now).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedQueryExecutionTest, RetryBackoffDoesNotFailUntilSenderBecomesTerminal) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const schema::TabletId tablet = input->plan.fragments[0].tablet_id;
  auto execution = DistributedQueryExecution::create(
      1U, std::move(input->plan), std::move(input->admissions), std::move(input->snapshot),
      {.coordinator = {},
       .retry = {.maximum_attempts = 2U,
                 .initial_backoff = std::chrono::milliseconds{10},
                 .maximum_backoff = std::chrono::milliseconds{10}}});
  ASSERT_TRUE(execution.has_value());
  const auto now = DistributedQueryExecution::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablet, now).has_value());
  const auto retry = encode_distributed_query_response_v1(
                         {11U, 1U, uuid(7U), tablet, common::StatusCode::kUnavailable, std::nullopt,
                          DistributedQueryLeaderHint{13U, 14U}})
                         .value();
  ASSERT_TRUE(execution->accept_response(tablet, retry, now).is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kBackoff);
  EXPECT_EQ(*execution->next_attempt_not_before(tablet), now + std::chrono::milliseconds{10});
  EXPECT_EQ(*execution->suggested_leader(tablet), DistributedQueryLeaderHint(13U, 14U));
  ASSERT_TRUE(execution->begin_attempt(tablet, now + std::chrono::milliseconds{10}).has_value());
  ASSERT_TRUE(execution
                  ->record_transport_failure(tablet, common::StatusCode::kIoError,
                                             now + std::chrono::milliseconds{10})
                  .is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kFailed);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kIoError);
}

TEST(DistributedQueryTcpExecutionTest, SchedulesPlanOrderedTabletsAndRetriesWithoutRebinding) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  auto execution = DistributedQueryExecution::create(
      1U, std::move(input->plan), std::move(input->admissions), std::move(input->snapshot),
      {.coordinator = {},
       .retry = {.maximum_attempts = 2U,
                 .initial_backoff = std::chrono::milliseconds{1},
                 .maximum_backoff = std::chrono::milliseconds{1}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();

  ExecutionNodeAuthorizer authorizer;
  ExecutionWorker first_worker{2.5, true};
  ExecutionWorker second_worker{3.5, false};
  auto first_receiver = DistributedQueryReceiver::create(
      {.local_node_id = 11U, .authorizer = &authorizer, .worker = &first_worker});
  auto second_receiver = DistributedQueryReceiver::create(
      {.local_node_id = 12U, .authorizer = &authorizer, .worker = &second_worker});
  ASSERT_TRUE(first_receiver.has_value());
  ASSERT_TRUE(second_receiver.has_value());
  ExecutionAuthenticator client_authenticator{91U};
  auto first_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *first_receiver));
  auto second_server = DistributedQueryTcpServer::start(
      execution_server_config(client_authenticator, *second_receiver));
  ASSERT_TRUE(first_server.has_value()) << first_server.error().to_string();
  ASSERT_TRUE(second_server.has_value()) << second_server.error().to_string();

  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  ExecutionAuthenticator server_authenticator{92U};
  auto tcp_execution = DistributedQueryTcpExecution::create(
      std::move(*execution),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{11U, first_server->bound_endpoint(), &*tls_context},
                  {12U, second_server->bound_endpoint(), &*tls_context}},
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000}},
       .connect_timeout = std::chrono::milliseconds{1000}});
  ASSERT_TRUE(tcp_execution.has_value()) << tcp_execution.error().to_string();
  EXPECT_EQ(tcp_execution->snapshot().snapshot().generation(), 1U);

  for (std::size_t iteration = 0U;
       iteration < 2048U && tcp_execution->state() == DistributedQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(tcp_execution->poll_once(std::chrono::milliseconds{1}).is_ok())
        << tcp_execution->failure().to_string();
    ASSERT_TRUE(first_server->poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(second_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }

  ASSERT_EQ(tcp_execution->state(), DistributedQueryTcpExecutionState::kComplete)
      << tcp_execution->failure().to_string();
  auto result = tcp_execution->result();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->count, 2U);
  EXPECT_EQ(result->sum, 6.0);
  EXPECT_EQ(first_worker.calls, 2U);
  EXPECT_EQ(second_worker.calls, 1U);
  const auto metrics = tcp_execution->metrics();
  EXPECT_EQ(metrics.attempts_started, 3U);
  EXPECT_EQ(metrics.retries_started, 1U);
  EXPECT_EQ(metrics.transport_completed_attempts, 3U);
  EXPECT_EQ(metrics.transport_failed_attempts, 0U);
  EXPECT_EQ(metrics.active_attempts, 0U);
}

TEST(DistributedQueryTcpExecutionTest, RejectsIncompleteRoutesBeforeOpeningAttempts) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  auto execution = DistributedQueryExecution::create(
      1U, std::move(input->plan), std::move(input->admissions), std::move(input->snapshot));
  ASSERT_TRUE(execution.has_value());
  ExecutionNodeAuthorizer authorizer;
  ExecutionAuthenticator authenticator{92U};
  auto tls_context = network::TlsClientContext::create(execution_tls_client_config());
  ASSERT_TRUE(tls_context.has_value());
  EXPECT_EQ(DistributedQueryTcpExecution::create(
                std::move(*execution), {.authenticator = &authenticator,
                                        .node_authorizer = &authorizer,
                                        .routes = {{11U, {{127U, 0U, 0U, 1U}, 1U}, &*tls_context}}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedQueryExecutionTest, RejectsAdmissionOrderThatDiffersFromPinnedDispatches) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  std::swap(input->admissions[0], input->admissions[1]);
  EXPECT_EQ(DistributedQueryExecution::create(1U, std::move(input->plan),
                                              std::move(input->admissions),
                                              std::move(input->snapshot))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
