#include "chronos/common/byte_reader.hpp"
#include "chronos/ingest/columnar_append_executor.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/service/native_protocol_service.hpp"
#include "chronos/service/single_node_database.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-single-node-XXXXXX").string();
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

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

constexpr std::string_view kCreateSql =
    "CREATE TABLE trades (ts TIMESTAMP_NS NOT NULL, symbol SYMBOL NOT NULL, price "
    "DECIMAL(20, 8) NOT NULL, note STRING) EVENT TIME ts ORDER KEY (symbol, ts) "
    "PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (symbol) DEDUP KEY "
    "(symbol, ts) RETENTION INTERVAL '30 days' SYSTEM HISTORY RETENTION INTERVAL "
    "'7 days' ALLOWED LATENESS INTERVAL '0 seconds'";

[[nodiscard]] runtime::DatabaseBootstrapDescriptor descriptor() {
  return {.database_id = uuid(1U),
          .metadata_group_id = uuid(2U),
          .local_node_id = 1U,
          .mutable_head_rows = 4U,
          .maximum_sealed_generations = 4U,
          .variable_column_bytes = 64U,
          .maximum_retry_entries = 128U,
          .wal_segment_target_bytes = wal::kSegmentSizeLimit,
          .raft_segment_target_bytes = 1U * 1024U * 1024U};
}

[[nodiscard]] SingleNodeDatabaseConfig config(const TemporaryDirectory& directory) {
  return {.bootstrap = {.database_root = directory.path().string(), .new_database = descriptor()},
          .wal_recovery = {.repair_incomplete_final_tail = false},
          .raft_recovery = {.repair_incomplete_final_tail = false}};
}

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(70U);
}

[[nodiscard]] raft::ProposeOperation metadata_proposal(raft::MetadataCommand command) {
  return {raft::kRaftMetadataCommandEntryType,
          raft::encode_metadata_command_v1(std::move(command)).value()};
}

void seed_catalog(const TemporaryDirectory& directory) {
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(config(directory).bootstrap);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  const auto schema = columnar::test::batch_schema();
  const raft::CatalogTableDefinition definition{
      .name = "events", .quoted = false, .schema = schema};
  const raft::GroupId group = descriptor().metadata_group_id;
  const raft::RaftPersistentLogConfig raft_config{
      .directory_path = bootstrap->raft_directory_path(),
      .target_segment_size = descriptor().raft_segment_target_bytes};
  ASSERT_TRUE(bootstrap->close().is_ok());
  auto runtime = raft::DurableMultiRaftRuntime::create_new(1U, raft_config, {{group, {1U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(runtime->execute_batch({{group, raft::StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(runtime
                  ->execute_batch({{group,
                                    raft::ProposeOperation{
                                        raft::kRaftSchemaDefinitionEntryType,
                                        raft::encode_schema_definition_v1(definition).value()}}})
                  .has_value());
  ASSERT_TRUE(runtime
                  ->execute_batch({{group, metadata_proposal(raft::TablePolicyMetadata{
                                               schema->table_id(), 100, 1000, 500, 10, 128U})}})
                  .has_value());
  ASSERT_TRUE(runtime
                  ->execute_batch({{group, metadata_proposal(raft::TabletPlacementMetadata{
                                               schema->table_id(), tablet_id(), 1U, {1U}, 1U})}})
                  .has_value());
  ASSERT_TRUE(runtime->close().is_ok());
}

void seed_schema_prefix(const TemporaryDirectory& directory) {
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(config(directory).bootstrap);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto parsed = query::parse_sql_v1_create_table(kCreateSql);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  auto empty = std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, {}).value());
  auto bound = query::bind_sql_v1_create_table(std::move(*parsed), std::move(empty));
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  const std::vector columns{id<schema::ColumnId>(43U), id<schema::ColumnId>(44U),
                            id<schema::ColumnId>(45U), id<schema::ColumnId>(46U)};
  auto table = query::materialize_sql_v1_table_schema(*bound, id<schema::TableId>(41U),
                                                      id<schema::SchemaId>(42U), columns);
  ASSERT_TRUE(table.has_value()) << table.error().status().to_string();
  const raft::CatalogTableDefinition definition{
      .name = "trades",
      .quoted = false,
      .schema = std::make_shared<const schema::TableSchema>(std::move(*table))};
  const raft::GroupId group = descriptor().metadata_group_id;
  const raft::RaftPersistentLogConfig raft_config{
      .directory_path = bootstrap->raft_directory_path(),
      .target_segment_size = descriptor().raft_segment_target_bytes};
  ASSERT_TRUE(bootstrap->close().is_ok());
  auto raft_runtime = raft::DurableMultiRaftRuntime::create_new(1U, raft_config, {{group, {1U}}});
  ASSERT_TRUE(raft_runtime.has_value()) << raft_runtime.error().to_string();
  ASSERT_TRUE(raft_runtime->execute_batch({{group, raft::StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(raft_runtime
                  ->execute_batch({{group,
                                    raft::ProposeOperation{
                                        raft::kRaftSchemaDefinitionEntryType,
                                        raft::encode_schema_definition_v1(definition).value()}}})
                  .has_value());
  ASSERT_TRUE(raft_runtime->close().is_ok());
}

[[nodiscard]] query::SqlResult<query::BoundSqlCreateTable>
bind_create(SingleNodeDatabase& database) {
  auto parsed = query::parse_sql_v1_create_table(kCreateSql);
  if (!parsed.has_value())
    return std::unexpected(parsed.error());
  return query::bind_sql_v1_create_table(std::move(*parsed), database.query_catalog());
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                           columnar::test::batch_columns())
          .value());
}

[[nodiscard]] network::NetworkTask ingest_task(const std::uint8_t seed,
                                               const network::DurabilityMode durability) {
  const auto input = batch();
  const auto encoded_batch = columnar::encode_columnar_batch_v1(*input).value();
  const auto append =
      ingest::encode_columnar_append_v1(
          {.client_id = ingest::test::request_id<ingest::ClientId>(seed),
           .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(seed + 32U),
           .tablet_id = tablet_id()},
          encoded_batch)
          .value();
  auto payload = network::encode_ingest_request(durability, append.bytes()).value();
  return {.connection_id = 9U,
          .principal_id = 7U,
          .frame = {.header = {.message_type = network::MessageType::kIngestRequest,
                               .request_id = seed,
                               .payload_size = static_cast<std::uint32_t>(payload.size())},
                    .payload = std::move(payload)}};
}

[[nodiscard]] network::NetworkTask query_task(const std::uint64_t request_id,
                                              const std::string_view sql) {
  auto payload = network::encode_query_request(sql).value();
  return {.connection_id = 9U,
          .principal_id = 7U,
          .frame = {.header = {.message_type = network::MessageType::kQueryRequest,
                               .request_id = request_id,
                               .payload_size = static_cast<std::uint32_t>(payload.size())},
                    .payload = std::move(payload)}};
}

[[nodiscard]] std::int64_t query_count(SingleNodeDatabase& database) {
  auto parsed = query::parse_sql_v1_select("SELECT count(*) AS rows FROM events");
  EXPECT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  auto bound = query::bind_sql_v1_select(std::move(*parsed), database.query_catalog());
  EXPECT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto lowered = query::lower_bound_sql_select(*bound);
  EXPECT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  const auto schema = columnar::test::batch_schema();
  const auto* lineage = database.find_lineage(schema->table_id());
  EXPECT_NE(lineage, nullptr);
  auto* tablet = database.find_tablet(tablet_id());
  EXPECT_NE(tablet, nullptr);
  auto snapshot = tablet->snapshot();
  EXPECT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  query::QueryResourceContext resources =
      query::QueryResourceContext::create(8U * 1024U * 1024U).value();
  auto pipeline = query::instantiate_tablet_state_pipeline(resources, *snapshot, *lineage,
                                                           schema->schema_id(), *lowered);
  EXPECT_TRUE(pipeline.has_value()) << pipeline.error().to_string();
  auto step = (*pipeline)->next(resources);
  EXPECT_TRUE(step.has_value()) << step.error().to_string();
  EXPECT_EQ(step->kind(), query::PhysicalOperatorStepKind::kChunk);
  const auto cell = step->chunk()->chunk().cell({.column_ordinal = 0U, .selected_row = 0U});
  EXPECT_TRUE(cell.has_value());
  common::ByteReader reader{cell->bytes().value()};
  return reader.read_i64_le().value();
}

TEST(SingleNodeDatabaseTest, CreatesAndReopensAnEmptyDatabaseWithoutConfiguredTablets) {
  TemporaryDirectory directory;
  auto created = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  EXPECT_TRUE(created->query_catalog()->tables().empty());
  ASSERT_TRUE(created->shutdown().is_ok());
  auto reopened = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_TRUE(reopened->query_catalog()->tables().empty());
  EXPECT_TRUE(reopened->shutdown().is_ok());
}

TEST(SingleNodeDatabaseTest, RecoversCatalogWalRowsAndVectorQueryVisibility) {
  TemporaryDirectory directory;
  seed_catalog(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  ASSERT_EQ(database->query_catalog()->tables().size(), 1U);
  ASSERT_NE(database->find_tablet(tablet_id()), nullptr);
  const auto appended = ingest::execute_columnar_append(
      {.client_id = ingest::test::request_id<ingest::ClientId>(1U),
       .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(2U),
       .batch = batch(),
       .durability = wal::WalDurabilityMode::kLocalSync},
      database->retry_directory(), *database->find_tablet(tablet_id()),
      database->wal_coordinator());
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(query_count(*database), 2);
  ASSERT_TRUE(database->shutdown().is_ok());

  auto recovered = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->find_tablet(tablet_id())->snapshot()->visible_row_count(), 2U);
  EXPECT_EQ(query_count(*recovered), 2);
  EXPECT_TRUE(recovered->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, AppliesLocalSyncIngestAndReturnsPositionlessMatchingRetry) {
  TemporaryDirectory directory;
  seed_catalog(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  NativeProtocolService service{*database};

  auto applied = service.execute_ingest(ingest_task(3U, network::DurabilityMode::kLocalSync));
  ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
  EXPECT_EQ(applied->connection_id, 9U);
  EXPECT_EQ(applied->principal_id, 7U);
  EXPECT_EQ(applied->frame.header.message_type, network::MessageType::kIngestAcknowledgement);
  const auto applied_ack = network::decode_ingest_acknowledgement(applied->frame.payload);
  ASSERT_TRUE(applied_ack.has_value()) << applied_ack.error().to_string();
  EXPECT_EQ(applied_ack->requested_durability, network::DurabilityMode::kLocalSync);
  EXPECT_EQ(applied_ack->effective_durability, network::DurabilityMode::kLocalSync);
  EXPECT_EQ(applied_ack->outcome, network::IngestOutcome::kApplied);
  EXPECT_NE(applied_ack->record_sequence, 0U);
  EXPECT_NE(applied_ack->segment_number, 0U);

  auto matching = service.execute_ingest(ingest_task(3U, network::DurabilityMode::kAsync));
  ASSERT_TRUE(matching.has_value()) << matching.error().to_string();
  const auto matching_ack = network::decode_ingest_acknowledgement(matching->frame.payload);
  ASSERT_TRUE(matching_ack.has_value()) << matching_ack.error().to_string();
  EXPECT_EQ(matching_ack->requested_durability, network::DurabilityMode::kAsync);
  EXPECT_EQ(matching_ack->effective_durability, network::DurabilityMode::kAsync);
  EXPECT_EQ(matching_ack->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(matching_ack->record_sequence, 0U);
  EXPECT_EQ(matching_ack->segment_number, 0U);
  EXPECT_EQ(matching_ack->byte_offset, 0U);
  EXPECT_EQ(database->find_tablet(tablet_id())->snapshot()->visible_row_count(), 2U);

  auto query = service.execute_query(query_task(40U, "SELECT count(*) AS rows FROM events"));
  ASSERT_TRUE(query.has_value()) << query.error().to_string();
  ASSERT_EQ(query->responses.size(), 2U);
  EXPECT_EQ(query->result_rows, 1U);
  EXPECT_GT(query->payload_bytes, 0U);
  EXPECT_EQ(query->responses[0].frame.header.message_type, network::MessageType::kQueryResult);
  EXPECT_EQ(query->responses[1].frame.header.message_type, network::MessageType::kQueryEnd);
  EXPECT_EQ(query->responses[0].frame.header.request_id, 40U);
  const auto result = network::decode_query_result_batch(query->responses[0].frame.payload);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->row_count(), 1U);
  ASSERT_EQ(result->columns().size(), 1U);
  EXPECT_EQ(result->columns()[0].name, "rows");
  const network::QueryResultCell* const cell = result->cell(0U, 0U);
  ASSERT_NE(cell, nullptr);
  ASSERT_FALSE(cell->is_null);
  common::ByteReader count{cell->value};
  EXPECT_EQ(count.read_i64_le().value(), 2);
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, RejectsMalformedIngestWithProtocolError) {
  TemporaryDirectory directory;
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  NativeProtocolService service{*database};
  auto task = ingest_task(4U, network::DurabilityMode::kAsync);
  task.frame.payload.resize(3U);

  auto response = service.execute_ingest(std::move(task));
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  EXPECT_EQ(response->frame.header.message_type, network::MessageType::kError);
  const auto error = network::decode_error_message(response->frame.payload);
  ASSERT_TRUE(error.has_value()) << error.error().to_string();
  EXPECT_EQ(error->code, network::ProtocolErrorCode::kInvalidRequest);
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, ConvertsBoundedQueryOverflowToOneTerminalError) {
  TemporaryDirectory directory;
  seed_catalog(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  ASSERT_TRUE(ingest::execute_columnar_append(
                  {.client_id = ingest::test::request_id<ingest::ClientId>(1U),
                   .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(2U),
                   .batch = batch()},
                  database->retry_directory(), *database->find_tablet(tablet_id()),
                  database->wal_coordinator())
                  .has_value());
  NativeProtocolServiceLimits limits;
  limits.maximum_result_rows = 1U;
  NativeProtocolService service{*database, limits};

  auto query = service.execute_query(query_task(41U, "SELECT * FROM events"));
  ASSERT_TRUE(query.has_value()) << query.error().to_string();
  ASSERT_EQ(query->responses.size(), 1U);
  EXPECT_EQ(query->result_rows, 0U);
  EXPECT_EQ(query->responses[0].frame.header.message_type, network::MessageType::kError);
  const auto error = network::decode_error_message(query->responses[0].frame.payload);
  ASSERT_TRUE(error.has_value()) << error.error().to_string();
  EXPECT_EQ(error->code, network::ProtocolErrorCode::kOverloaded);
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, EmitsDescribedZeroRowResultBeforeQueryEnd) {
  TemporaryDirectory directory;
  seed_catalog(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  NativeProtocolService service{*database};

  auto query = service.execute_query(query_task(42U, "SELECT * FROM events WHERE false"));
  ASSERT_TRUE(query.has_value()) << query.error().to_string();
  ASSERT_EQ(query->responses.size(), 2U);
  EXPECT_EQ(query->result_rows, 0U);
  EXPECT_EQ(query->responses[0].frame.header.message_type, network::MessageType::kQueryResult);
  EXPECT_EQ(query->responses[1].frame.header.message_type, network::MessageType::kQueryEnd);
  const auto result = network::decode_query_result_batch(query->responses[0].frame.payload);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->row_count(), 0U);
  EXPECT_EQ(result->columns().size(), 3U);
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(SingleNodeDatabaseTest, CreatesACompleteTableAndReopensItsRuntimeCatalog) {
  TemporaryDirectory directory;
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  auto bound = bind_create(*database);
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto created =
      database->create_table(*bound,
                             {.table_id = id<schema::TableId>(11U),
                              .schema_id = id<schema::SchemaId>(12U),
                              .column_ids = {id<schema::ColumnId>(13U), id<schema::ColumnId>(14U),
                                             id<schema::ColumnId>(15U), id<schema::ColumnId>(16U)},
                              .tablet_id = id<schema::TabletId>(17U)},
                             64U);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  EXPECT_FALSE(created->resumed_incomplete_creation);
  EXPECT_EQ(database->query_catalog()->tables().size(), 1U);
  EXPECT_NE(database->find_tablet(created->tablet_id), nullptr);
  EXPECT_NE(database->find_lineage(created->table_id), nullptr);
  EXPECT_EQ(bind_create(*database).error().status().code(), common::StatusCode::kAlreadyExists);
  ASSERT_TRUE(database->shutdown().is_ok());

  auto reopened = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->query_catalog()->tables().size(), 1U);
  EXPECT_NE(reopened->find_tablet(created->tablet_id), nullptr);
  EXPECT_EQ(reopened->metadata_catalog().table_policies.front().retry_retention_positions, 64U);
  EXPECT_TRUE(reopened->shutdown().is_ok());
}

TEST(SingleNodeDatabaseTest, ResumesAnIncompleteSchemaPrefixUsingItsDurableIdentities) {
  TemporaryDirectory directory;
  seed_schema_prefix(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  EXPECT_TRUE(database->query_catalog()->tables().empty());
  auto bound = bind_create(*database);
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto created =
      database->create_table(*bound,
                             {.table_id = id<schema::TableId>(71U),
                              .schema_id = id<schema::SchemaId>(72U),
                              .column_ids = {id<schema::ColumnId>(73U), id<schema::ColumnId>(74U),
                                             id<schema::ColumnId>(75U), id<schema::ColumnId>(76U)},
                              .tablet_id = id<schema::TabletId>(77U)},
                             32U);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  EXPECT_TRUE(created->resumed_incomplete_creation);
  EXPECT_EQ(created->table_id, id<schema::TableId>(41U));
  EXPECT_EQ(created->schema_id, id<schema::SchemaId>(42U));
  EXPECT_EQ(created->tablet_id, id<schema::TabletId>(77U));
  EXPECT_EQ(database->query_catalog()->tables().size(), 1U);
  EXPECT_TRUE(database->shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
