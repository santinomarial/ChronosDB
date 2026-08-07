#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/snapshot_pipeline.hpp"
#include "snapshot_tablet_scan_test_fixture.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] PhysicalPipelinePlan lower(const test::SnapshotTabletScanFixture& fixture,
                                         const std::string_view sql) {
  const std::vector<QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = fixture.schema_ptr()}};
  auto catalog = std::make_shared<const QueryCatalogSnapshot>(
      QueryCatalogSnapshot::create(1U, tables).value());
  SqlResult<ParsedSqlSelect> parsed = parse_sql_v1_select(sql);
  if (!parsed.has_value())
    throw std::runtime_error{parsed.error().status().to_string()};
  SqlResult<BoundSqlSelect> bound = bind_sql_v1_select(std::move(*parsed), std::move(catalog));
  if (!bound.has_value())
    throw std::runtime_error{bound.error().status().to_string()};
  SqlResult<PhysicalPipelinePlan> lowered = lower_bound_sql_select(*bound);
  if (!lowered.has_value())
    throw std::runtime_error{lowered.error().status().to_string()};
  return std::move(*lowered);
}

[[nodiscard]] std::int64_t signed_cell(const VectorChunk& chunk, const std::size_t row) {
  const columnar::ColumnCellView cell =
      chunk.cell({.column_ordinal = 0U, .selected_row = row}).value();
  const common::ByteView bytes = cell.bytes().value();
  std::uint64_t encoded = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index)
    encoded |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[index]))
               << (index * 8U);
  return std::bit_cast<std::int64_t>(encoded);
}

[[nodiscard]] std::vector<std::int64_t> drain_signed(PhysicalOperator& pipeline,
                                                     const QueryResourceContext& resources) {
  std::vector<std::int64_t> values;
  for (;;) {
    common::Result<PhysicalOperatorStep> step = pipeline.next(resources);
    EXPECT_TRUE(step.has_value()) << step.error().to_string();
    if (!step.has_value() || step->kind() == PhysicalOperatorStepKind::kEnd)
      break;
    const VectorChunk& chunk = step->chunk()->chunk();
    EXPECT_EQ(chunk.column_count(), 1U);
    for (std::size_t row = 0U; row < chunk.selected_row_count(); ++row)
      values.push_back(signed_cell(chunk, row));
  }
  return values;
}

TEST(SnapshotPipelineTest, InstantiatesBoundWhereOrderLimitAndRemovesHiddenSuffix) {
  test::SnapshotTabletScanFixture fixture{6U};
  PhysicalPipelinePlan plan =
      lower(fixture, "SELECT event_time FROM metrics WHERE event_time >= "
                     "TIMESTAMP '1969-12-31 23:59:59.999999903Z' ORDER BY event_time DESC LIMIT 2");
  ASSERT_EQ(plan.input_columns().size(), 5U);
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  auto pipeline = instantiate_snapshot_tablet_pipeline(
      resources, fixture.storage(), fixture.snapshot(),
      test::SnapshotTabletScanFixture::tablet_id(), fixture.lineage(),
      fixture.schema_ptr()->schema_id(), plan);
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().to_string();
  EXPECT_EQ(drain_signed(**pipeline, resources), (std::vector<std::int64_t>{-95, -96}));
  pipeline->reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(SnapshotPipelineTest, InfersSuffixFreeAggregateInputAndExecutesExactSnapshot) {
  test::SnapshotTabletScanFixture fixture{7U};
  PhysicalPipelinePlan plan =
      lower(fixture, "SELECT count(*) AS rows FROM metrics ORDER BY rows DESC");
  ASSERT_EQ(plan.input_columns().size(), 1U);
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  auto pipeline = instantiate_snapshot_tablet_pipeline(
      resources, fixture.storage(), fixture.snapshot(),
      test::SnapshotTabletScanFixture::tablet_id(), fixture.lineage(),
      fixture.schema_ptr()->schema_id(), plan);
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().to_string();
  EXPECT_EQ(drain_signed(**pipeline, resources), (std::vector<std::int64_t>{7}));
  pipeline->reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(SnapshotPipelineTest, InfersLatestSuffixAndExecutesComputedTimestampBeforeOrder) {
  test::SnapshotTabletScanFixture fixture{6U};
  PhysicalPipelinePlan plan = lower(
      fixture, "SELECT event_time FROM metrics LATEST BY (event_time) ON "
               "time_bucket(INTERVAL '1 second', event_time) ORDER BY event_time DESC LIMIT 2");
  ASSERT_EQ(plan.input_columns().size(), 5U);
  ASSERT_TRUE(std::ranges::any_of(plan.stages(), [](const PhysicalPipelineStage& stage) {
    return std::holds_alternative<LatestByStage>(stage);
  }));
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  auto pipeline = instantiate_snapshot_tablet_pipeline(
      resources, fixture.storage(), fixture.snapshot(),
      test::SnapshotTabletScanFixture::tablet_id(), fixture.lineage(),
      fixture.schema_ptr()->schema_id(), plan);
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().to_string();
  EXPECT_EQ(drain_signed(**pipeline, resources), (std::vector<std::int64_t>{-95, -96}));
  pipeline->reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(SnapshotPipelineTest, RejectsForeignAndMalformedPipelineShapesBeforeSourceOwnership) {
  test::SnapshotTabletScanFixture fixture{3U};
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();

  auto wrong_type =
      PhysicalPipelinePlan::create(
          {{.type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
            .nullable = false}},
          {})
          .value();
  auto rejected = instantiate_snapshot_tablet_pipeline(
      resources, fixture.storage(), fixture.snapshot(),
      test::SnapshotTabletScanFixture::tablet_id(), fixture.lineage(),
      fixture.schema_ptr()->schema_id(), wrong_type);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);

  PhysicalPipelinePlan ordered =
      lower(fixture, "SELECT event_time FROM metrics ORDER BY event_time");
  std::vector<PhysicalColumnShape> bad_suffix{ordered.input_columns().begin(),
                                              ordered.input_columns().end()};
  bad_suffix.back().nullable = true;
  auto malformed = PhysicalPipelinePlan::create(std::move(bad_suffix), {}).value();
  rejected = instantiate_snapshot_tablet_pipeline(resources, fixture.storage(), fixture.snapshot(),
                                                  test::SnapshotTabletScanFixture::tablet_id(),
                                                  fixture.lineage(),
                                                  fixture.schema_ptr()->schema_id(), malformed);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);

  rejected = instantiate_snapshot_tablet_pipeline(
      resources, fixture.storage(), fixture.snapshot(),
      test::SnapshotTabletScanFixture::tablet_id(), fixture.lineage(),
      cseg::test::identifier<schema::SchemaId>(0x77U), ordered);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(SnapshotPipelineTest, PropagatesFinitePlanningAndSourceLimitsWithoutLeakingCredit) {
  test::SnapshotTabletScanFixture fixture{2U};
  PhysicalPipelinePlan plan = lower(fixture, "SELECT event_time FROM metrics");
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto rejected = instantiate_snapshot_tablet_pipeline(
      resources, fixture.storage(), fixture.snapshot(),
      test::SnapshotTabletScanFixture::tablet_id(), fixture.lineage(),
      fixture.schema_ptr()->schema_id(), plan,
      {.scan = {.maximum_heads = 0U, .maximum_retained_configuration_bytes = 4'096U}});
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::query
