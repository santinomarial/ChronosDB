#include "chronos/cluster/distributed_vector_physical_rows_finalization_v2.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] std::string bytes_as_string(const std::span<const std::byte> bytes) {
  std::string result;
  result.reserve(bytes.size());
  for (const std::byte byte : bytes)
    result.push_back(static_cast<char>(byte));
  return result;
}

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema> events_schema() {
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(3U), "ts",
                                                     type(schema::LogicalTypeKind::kTimestampNs),
                                                     false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(4U), "label",
                                                     type(schema::LogicalTypeKind::kString), true)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(5U), "enabled",
                                                     type(schema::LogicalTypeKind::kBool), false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(6U), "score",
                                                     type(schema::LogicalTypeKind::kInt64), false)
                        .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = id<schema::ColumnId>(3U),
                                   .physical_ordering_key = {id<schema::ColumnId>(3U)},
                                   .partition_columns = {id<schema::ColumnId>(3U)},
                                   .shard_key = {id<schema::ColumnId>(3U)},
                                   .deduplication_key = {id<schema::ColumnId>(3U)}})
          .value());
}

[[nodiscard]] query::BoundSqlSelect bind(const std::string_view sql) {
  const std::vector<query::QueryCatalogTableInput> tables{
      {.name = "events", .quoted = false, .schema = events_schema()}};
  auto catalog = std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
  return query::bind_sql_v1_select(query::parse_sql_v1_select(sql).value(), std::move(catalog))
      .value();
}

struct Row {
  std::int64_t timestamp;
  std::optional<std::string_view> label;
  bool enabled;
  std::int64_t score;
};

[[nodiscard]] std::array<std::byte, 8U> signed_bytes(const std::int64_t value) {
  const std::uint64_t bits = std::bit_cast<std::uint64_t>(value);
  std::array<std::byte, 8U> bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index)
    bytes[index] = static_cast<std::byte>((bits >> (index * 8U)) & 0xffU);
  return bytes;
}

[[nodiscard]] std::vector<std::byte>
encode_rows(const std::span<const Row> rows,
            const query::DistributedVectorResultSchema& expected_schema) {
  std::vector<network::QueryResultColumn> columns;
  columns.reserve(expected_schema.columns.size());
  for (const auto& column : expected_schema.columns)
    columns.push_back({.name = column.name, .type = column.type, .nullable = column.nullable});
  std::vector<std::array<std::byte, 8U>> timestamps(rows.size());
  std::vector<std::array<std::byte, 8U>> scores(rows.size());
  std::vector<std::byte> booleans(rows.size());
  std::vector<network::QueryResultCell> cells;
  cells.reserve(rows.size() * columns.size());
  for (std::size_t ordinal = 0U; ordinal < rows.size(); ++ordinal) {
    timestamps[ordinal] = signed_bytes(rows[ordinal].timestamp);
    scores[ordinal] = signed_bytes(rows[ordinal].score);
    booleans[ordinal] = rows[ordinal].enabled ? std::byte{1U} : std::byte{};
    cells.push_back({.value = timestamps[ordinal]});
    if (rows[ordinal].label.has_value()) {
      const std::string_view label = rows[ordinal].label.value_or(std::string_view{});
      cells.push_back({.value = std::as_bytes(std::span{label.data(), label.size()})});
    } else {
      cells.push_back({.is_null = true, .value = {}});
    }
    cells.push_back({.value = {&booleans[ordinal], 1U}});
    cells.push_back({.value = scores[ordinal]});
  }
  return network::encode_query_result_batch(static_cast<std::uint32_t>(rows.size()), columns, cells)
      .value();
}

[[nodiscard]] DistributedVectorResultExchangeMessage message(const schema::TabletId tablet,
                                                             std::vector<std::byte> batch) {
  return {.query_id = uuid(20U),
          .tablet_id = tablet,
          .sequence = 1U,
          .terminal = true,
          .encoded_result_batch = std::move(batch)};
}

[[nodiscard]] std::int64_t int64_cell(const network::QueryResultBatchView& batch,
                                      const std::uint32_t row, const std::size_t column) {
  const network::QueryResultCell* cell = batch.cell(row, column);
  EXPECT_NE(cell, nullptr);
  EXPECT_EQ(cell->value.size(), 8U);
  std::array<std::byte, 8U> bytes{};
  std::ranges::copy(cell->value, bytes.begin());
  return std::bit_cast<std::int64_t>(bytes);
}

TEST(DistributedVectorPhysicalRowsFinalizationV2Test,
     ExecutesCompleteMultiKeyGroupedPhysicalPipelineAcrossTablets) {
  auto lowered = query::lower_bound_sql_select_to_distributed_vector_grouped(
      bind("SELECT lower(label) AS bucket, enabled AS active, sum(score + 2) AS total, "
           "count(*) AS n FROM events WHERE score >= 2 GROUP BY lower(label), enabled "
           "ORDER BY total DESC, bucket ASC LIMIT 2"));
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  const std::array first{Row{1, "A", true, 2}, Row{2, "b", false, 4}, Row{3, "a", true, 3}};
  const std::array second{Row{4, "B", false, 1}, Row{5, "c", true, 5}, Row{6, "b", false, 2}};
  std::vector<DistributedVectorResultExchangeMessage> messages;
  messages.push_back(
      message(id<schema::TabletId>(30U), encode_rows(first, lowered->input_rows.result_schema)));
  messages.push_back(
      message(id<schema::TabletId>(31U), encode_rows(second, lowered->input_rows.result_schema)));
  auto result = finalize_distributed_vector_physical_rows_v2(
      {.plan = lowered->input_rows.intent,
       .result = {.result_schema = lowered->input_rows.result_schema,
                  .messages = std::move(messages)}},
      lowered->coordinator_pipeline, std::move(lowered->result_schema));
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->row_count, 2U);
  ASSERT_EQ(result->encoded_batches.size(), 1U);

  auto first_group = network::decode_query_result_batch(result->encoded_batches[0]);
  ASSERT_TRUE(first_group.has_value()) << first_group.error().to_string();
  ASSERT_EQ(first_group->row_count(), 2U);
  ASSERT_EQ(first_group->columns().size(), 4U);
  EXPECT_EQ(first_group->columns()[0].name, "bucket");
  EXPECT_EQ(bytes_as_string(first_group->cell(0U, 0U)->value), "b");
  EXPECT_FALSE(first_group->cell(0U, 1U)->value.front() == std::byte{1U});
  EXPECT_EQ(int64_cell(*first_group, 0U, 2U), 10);
  EXPECT_EQ(int64_cell(*first_group, 0U, 3U), 2);
  EXPECT_EQ(bytes_as_string(first_group->cell(1U, 0U)->value), "a");
  EXPECT_EQ(int64_cell(*first_group, 1U, 2U), 9);
  EXPECT_EQ(int64_cell(*first_group, 1U, 3U), 2);
}

TEST(DistributedVectorPhysicalRowsFinalizationV2Test,
     PreservesEmptyGroupedSchemaAndRejectsWorkingExhaustion) {
  auto lowered = query::lower_bound_sql_select_to_distributed_vector_grouped(
      bind("SELECT label, count(*) AS n FROM events GROUP BY label"));
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  std::vector<DistributedVectorResultExchangeMessage> empty_messages;
  empty_messages.push_back(message(id<schema::TabletId>(30U), {}));
  auto empty = finalize_distributed_vector_physical_rows_v2(
      {.plan = lowered->input_rows.intent,
       .result = {.result_schema = lowered->input_rows.result_schema,
                  .messages = std::move(empty_messages)}},
      lowered->coordinator_pipeline, query::DistributedVectorResultSchema{lowered->result_schema});
  ASSERT_TRUE(empty.has_value()) << empty.error().to_string();
  EXPECT_EQ(empty->row_count, 0U);
  ASSERT_EQ(empty->encoded_batches.size(), 1U);
  auto decoded = network::decode_query_result_batch(empty->encoded_batches.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->row_count(), 0U);
  EXPECT_EQ(decoded->columns().size(), 2U);

  const std::array rows{Row{1, "payload", true, 2}};
  auto mismatched_schema = lowered->input_rows.result_schema;
  mismatched_schema.columns[0].name = "wrong";
  std::vector<DistributedVectorResultExchangeMessage> mismatched_messages;
  mismatched_messages.push_back(
      message(id<schema::TabletId>(30U), encode_rows(rows, lowered->input_rows.result_schema)));
  auto mismatched = finalize_distributed_vector_physical_rows_v2(
      {.plan = lowered->input_rows.intent,
       .result = {.result_schema = std::move(mismatched_schema),
                  .messages = std::move(mismatched_messages)}},
      lowered->coordinator_pipeline, query::DistributedVectorResultSchema{lowered->result_schema});
  ASSERT_FALSE(mismatched.has_value());
  EXPECT_EQ(mismatched.error().code(), common::StatusCode::kCorruption);

  std::vector<DistributedVectorResultExchangeMessage> bounded_messages;
  bounded_messages.push_back(
      message(id<schema::TabletId>(30U), encode_rows(rows, lowered->input_rows.result_schema)));
  auto bounded = finalize_distributed_vector_physical_rows_v2(
      {.plan = lowered->input_rows.intent,
       .result = {.result_schema = lowered->input_rows.result_schema,
                  .messages = std::move(bounded_messages)}},
      lowered->coordinator_pipeline, std::move(lowered->result_schema),
      {.maximum_batch_working_bytes = 1U});
  ASSERT_FALSE(bounded.has_value());
  EXPECT_EQ(bounded.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::cluster
