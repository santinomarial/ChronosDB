#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/columnar_batch_scan.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "columnar/columnar_test_support.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <memory>

namespace chronos::query {
namespace {

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                           columnar::test::batch_columns())
          .value());
}

TEST(ColumnarBatchScanTest, EmitsBoundedOwnedCanonicalChunksAndReleasesSourceCredit) {
  auto source = ColumnarBatchScanOperator::create(
      batch(), {.maximum_rows_per_chunk = 1U,
                .chunk = {.maximum_rows = 1U,
                          .maximum_columns = 3U,
                          .maximum_buffer_bytes = 4096U,
                          .maximum_retained_buffer_bytes = 4096U}});
  ASSERT_TRUE(source.has_value()) << source.error().to_string();
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();

  auto first = (*source)->next(resources);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_EQ(first->kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& first_chunk = first->chunk()->chunk();
  EXPECT_EQ(first_chunk.physical_row_count(), 1U);
  auto text = first_chunk.cell({1U, 0U})->bytes();
  ASSERT_TRUE(text.has_value());
  const std::array expected{std::byte{'x'}};
  EXPECT_TRUE(std::ranges::equal(*text, expected));
  EXPECT_TRUE(first_chunk.cell({2U, 0U})->boolean().value());

  auto second = (*source)->next(resources);
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  ASSERT_EQ(second->kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& second_chunk = second->chunk()->chunk();
  EXPECT_EQ(second_chunk.physical_row_count(), 1U);
  EXPECT_TRUE(second_chunk.cell({1U, 0U})->is_null());
  EXPECT_FALSE(second_chunk.cell({2U, 0U})->boolean().value());

  EXPECT_EQ((*source)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ((*source)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  first = PhysicalOperatorStep::end();
  second = PhysicalOperatorStep::end();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(ColumnarBatchScanTest, RejectsForeignCreditAndRetriesAdmissionWithoutAdvancing) {
  auto source = ColumnarBatchScanOperator::create(batch());
  ASSERT_TRUE(source.has_value());
  QueryResourceContext source_resources = QueryResourceContext::create(1U << 20U).value();
  QueryResourceContext foreign = QueryResourceContext::create(1U << 20U).value();
  auto first = (*source)->next(source_resources);
  ASSERT_TRUE(first.has_value());
  auto rejected = (*source)->next(foreign);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);

  auto retry_source = ColumnarBatchScanOperator::create(batch());
  ASSERT_TRUE(retry_source.has_value());
  QueryResourceContext tiny = QueryResourceContext::create(1U).value();
  rejected = (*retry_source)->next(tiny);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  QueryResourceContext admitted = QueryResourceContext::create(1U << 20U).value();
  auto retried = (*retry_source)->next(admitted);
  ASSERT_TRUE(retried.has_value()) << retried.error().to_string();
  EXPECT_EQ(retried->chunk()->chunk().physical_row_count(), 2U);
}

TEST(ColumnarBatchScanTest, InstantiatesARowPreservingSqlPipelineOverCommittedInput) {
  const auto input = batch();
  const std::array tables{
      QueryCatalogTableInput{.name = "events", .quoted = false, .schema = input->schema_ptr()}};
  auto catalog = std::make_shared<const QueryCatalogSnapshot>(
      QueryCatalogSnapshot::create(1U, tables).value());
  auto parsed = parse_sql_v1_select("SELECT tag FROM events WHERE enabled");
  ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  auto bound = bind_sql_v1_select(std::move(*parsed), std::move(catalog));
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto plan = lower_bound_sql_select(*bound);
  ASSERT_TRUE(plan.has_value()) << plan.error().status().to_string();
  auto source = ColumnarBatchScanOperator::create(input);
  ASSERT_TRUE(source.has_value()) << source.error().to_string();
  auto pipeline = plan->instantiate(std::move(*source));
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().to_string();
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();

  auto output = (*pipeline)->next(resources);
  ASSERT_TRUE(output.has_value()) << output.error().to_string();
  ASSERT_EQ(output->kind(), PhysicalOperatorStepKind::kChunk);
  ASSERT_EQ(output->chunk()->chunk().selected_row_count(), 1U);
  auto text = output->chunk()->chunk().cell({0U, 0U})->bytes();
  ASSERT_TRUE(text.has_value());
  const std::array expected{std::byte{'x'}};
  EXPECT_TRUE(std::ranges::equal(*text, expected));
  EXPECT_EQ((*pipeline)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

} // namespace
} // namespace chronos::query
