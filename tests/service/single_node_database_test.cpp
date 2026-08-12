#include "chronos/common/byte_reader.hpp"
#include "chronos/ingest/columnar_append_executor.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
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

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                           columnar::test::batch_columns())
          .value());
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

} // namespace
} // namespace chronos::service
