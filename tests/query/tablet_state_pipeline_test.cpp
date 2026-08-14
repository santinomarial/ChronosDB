#include "chronos/common/byte_reader.hpp"
#include "chronos/ingest/identity.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(70U);
}

[[nodiscard]] ingest::Sha256Digest digest(const std::uint8_t seed) {
  ingest::Sha256Digest::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return ingest::Sha256Digest{bytes};
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                           columnar::test::batch_columns())
          .value());
}

struct AppendVersion {
  std::uint8_t seed{};
  std::uint64_t sequence{};
};

void append(ingest::TabletState& target, const schema::TabletId& target_id,
            const AppendVersion version) {
  const ingest::RetryIdentity retry{
      .client_id = ingest::test::request_id<ingest::ClientId>(version.seed),
      .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(version.seed + 32U)};
  const ingest::ColumnarAppendMutationIdentity mutation{
      .table_id = columnar::test::batch_schema()->table_id(),
      .tablet_id = target_id,
      .request_digest = digest(version.seed)};
  auto prepared = target.prepare_append(retry, mutation, batch());
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  ASSERT_TRUE(prepared->mark_wal_started().is_ok());
  wal::WalId wal_id;
  wal_id.bytes.front() = std::byte{1U};
  auto published = prepared->publish({.wal_id = wal_id, .record_sequence = version.sequence});
  ASSERT_TRUE(published.has_value()) << published.error().to_string();
}

[[nodiscard]] PhysicalPipelinePlan lower(const std::string_view sql) {
  const std::vector<QueryCatalogTableInput> tables{
      {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()}};
  auto catalog = std::make_shared<const QueryCatalogSnapshot>(
      QueryCatalogSnapshot::create(1U, tables).value());
  auto parsed = parse_sql_v1_select(sql);
  EXPECT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  auto bound = bind_sql_v1_select(std::move(*parsed), std::move(catalog));
  EXPECT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto lowered = lower_bound_sql_select(*bound);
  EXPECT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  return std::move(*lowered);
}

TEST(TabletStatePipelineTest, DefaultsToAnEightMebibyteSourceConfigurationLimit) {
  EXPECT_EQ(TabletStatePipelineLimits{}.maximum_source_configuration_bytes,
            std::size_t{8U} * 1024U * 1024U);
}

TEST(TabletStatePipelineTest, ExecutesOneGlobalVectorPipelineAcrossSealedAndActiveHeads) {
  auto schema = columnar::test::batch_schema();
  auto target = ingest::TabletState::create(
      schema, tablet_id(),
      {.head_capacity = {.row_capacity = 2U, .variable_value_bytes = {0U, 2U, 0U}},
       .maximum_schema_versions = 1U,
       .maximum_sealed_generations = 2U,
       .maximum_retry_entries = 4U,
       .flush_queue = nullptr});
  ASSERT_TRUE(target.has_value()) << target.error().to_string();
  append(*target, tablet_id(), {.seed = 1U, .sequence = 1U});
  append(*target, tablet_id(), {.seed = 2U, .sequence = 2U});
  auto snapshot = target->snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  ASSERT_EQ(snapshot->sealed_generations().size(), 1U);
  ASSERT_EQ(snapshot->active_generation().row_count(), 2U);

  auto lineage = schema::SchemaLineage::create(*schema);
  ASSERT_TRUE(lineage.has_value());
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
  auto pipeline =
      instantiate_tablet_state_pipeline(resources, *snapshot, *lineage, schema->schema_id(),
                                        lower("SELECT count(*) AS rows FROM events"));
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().to_string();

  auto step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  ASSERT_EQ(step->chunk()->chunk().selected_row_count(), 1U);
  const auto cell = step->chunk()->chunk().cell({.column_ordinal = 0U, .selected_row = 0U});
  ASSERT_TRUE(cell.has_value());
  common::ByteReader count{cell->bytes().value()};
  EXPECT_EQ(count.read_i64_le().value(), 4);
  step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value());
  EXPECT_EQ(step->kind(), PhysicalOperatorStepKind::kEnd);
  pipeline->reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(TabletStatePipelineTest, ExecutesOneGlobalAggregateAcrossMultipleTablets) {
  auto schema = columnar::test::batch_schema();
  const schema::TabletId first_id = tablet_id();
  const schema::TabletId second_id = columnar::test::id<schema::TabletId>(71U);
  const ingest::TabletStateConfig config{
      .head_capacity = {.row_capacity = 4U, .variable_value_bytes = {0U, 2U, 0U}},
      .maximum_schema_versions = 1U,
      .maximum_sealed_generations = 1U,
      .maximum_retry_entries = 2U,
      .flush_queue = nullptr};
  auto first = ingest::TabletState::create(schema, first_id, config).value();
  auto second = ingest::TabletState::create(schema, second_id, config).value();
  append(first, first_id, {.seed = 1U, .sequence = 1U});
  append(second, second_id, {.seed = 2U, .sequence = 2U});
  const std::vector snapshots{first.snapshot().value(), second.snapshot().value()};
  auto lineage = schema::SchemaLineage::create(*schema).value();
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();

  auto pipeline =
      instantiate_tablet_states_pipeline(resources, snapshots, lineage, schema->schema_id(),
                                         lower("SELECT count(*) AS rows FROM events"));
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().to_string();
  auto step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  const auto cell = step->chunk()->chunk().cell({.column_ordinal = 0U, .selected_row = 0U});
  ASSERT_TRUE(cell.has_value());
  common::ByteReader count{cell->bytes().value()};
  EXPECT_EQ(count.read_i64_le().value(), 4);
  step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value());
  EXPECT_EQ(step->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(TabletStatePipelineTest, RejectsAPlanWhoseSourceShapeDisagreesWithTheSnapshotSchema) {
  auto schema = columnar::test::batch_schema();
  auto target = ingest::TabletState::create(
                    schema, tablet_id(),
                    {.head_capacity = {.row_capacity = 2U, .variable_value_bytes = {0U, 2U, 0U}},
                     .maximum_schema_versions = 1U,
                     .maximum_sealed_generations = 1U,
                     .maximum_retry_entries = 1U,
                     .flush_queue = nullptr})
                    .value();
  auto lineage = schema::SchemaLineage::create(*schema).value();
  auto wrong = PhysicalPipelinePlan::create(
                   {{.type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
                     .nullable = false}},
                   {})
                   .value();
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto pipeline = instantiate_tablet_state_pipeline(resources, target.snapshot().value(), lineage,
                                                    schema->schema_id(), wrong);
  ASSERT_FALSE(pipeline.has_value());
  EXPECT_EQ(pipeline.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(TabletStatePipelineTest, RejectsDuplicateTabletSnapshots) {
  auto schema = columnar::test::batch_schema();
  auto target = ingest::TabletState::create(
                    schema, tablet_id(),
                    {.head_capacity = {.row_capacity = 2U, .variable_value_bytes = {0U, 2U, 0U}},
                     .maximum_schema_versions = 1U,
                     .maximum_sealed_generations = 1U,
                     .maximum_retry_entries = 1U,
                     .flush_queue = nullptr})
                    .value();
  const auto snapshot = target.snapshot().value();
  const std::vector snapshots{snapshot, snapshot};
  auto lineage = schema::SchemaLineage::create(*schema).value();
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();

  auto pipeline =
      instantiate_tablet_states_pipeline(resources, snapshots, lineage, schema->schema_id(),
                                         lower("SELECT count(*) AS rows FROM events"));
  ASSERT_FALSE(pipeline.has_value());
  EXPECT_EQ(pipeline.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
