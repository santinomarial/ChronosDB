#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

TEST(DistributedVectorResultSchemaTest, RoundTripsOwnedDescriptorsAndRejectsDamage) {
  const auto decimal = schema::LogicalType::decimal(18U, 3U);
  ASSERT_TRUE(decimal.has_value());
  const DistributedVectorResultSchema schema_value{
      .columns = {{"label", type(schema::LogicalTypeKind::kString), true},
                  {"total", *decimal, false},
                  {"label", type(schema::LogicalTypeKind::kString), true}}};
  const auto encoded = encode_distributed_vector_result_schema(schema_value);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const auto decoded = decode_distributed_vector_result_schema_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, schema_value);

  EXPECT_EQ(decode_distributed_vector_result_schema_exact(encoded->bytes(), {.maximum_columns = 2U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(decode_distributed_vector_result_schema_exact(
                encoded->bytes(), {.maximum_frame_length = encoded->bytes().size() - 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(
      decode_distributed_vector_result_schema_exact(encoded->bytes(), {.maximum_name_length = 4U})
          .error()
          .code(),
      common::StatusCode::kResourceExhausted);
  std::vector<std::byte> damaged(encoded->bytes().begin(), encoded->bytes().end());
  damaged.back() ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_vector_result_schema_exact(damaged).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> future(encoded->bytes().begin(), encoded->bytes().end());
  future[8U] = std::byte{2U};
  store_u32_le(future, 32U, common::crc32c(common::ByteView{future}.first(32U)));
  store_u32_le(future, future.size() - 4U,
               common::crc32c(common::ByteView{future}.first(future.size() - 4U)));
  EXPECT_EQ(decode_distributed_vector_result_schema_exact(future).error().code(),
            common::StatusCode::kNotSupported);

  DistributedVectorResultSchema invalid = schema_value;
  invalid.columns.front().name = std::string(1U, static_cast<char>(0xff));
  EXPECT_EQ(encode_distributed_vector_result_schema(invalid).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorResultSchemaTest, BindsRowsAndAggregateShapesWithoutInventingNames) {
  const std::array inputs{PhysicalColumnShape{type(schema::LogicalTypeKind::kTimestampNs), false},
                          PhysicalColumnShape{type(schema::LogicalTypeKind::kString), true},
                          PhysicalColumnShape{type(schema::LogicalTypeKind::kFloat64), true}};
  const DistributedVectorPlanIntent rows{.mode = DistributedVectorPlanMode::kRows,
                                         .row_output_indices = {1U, 0U, 1U}};
  const DistributedVectorResultSchema row_schema{.columns = {{"alias", inputs[1].type, true},
                                                             {"event_time", inputs[0].type, false},
                                                             {"alias", inputs[1].type, true}}};
  EXPECT_TRUE(validate_distributed_vector_result_schema(rows, inputs, row_schema).is_ok());

  const DistributedVectorPlanIntent grouped{
      .mode = DistributedVectorPlanMode::kGroupedAggregate,
      .group_key_input_indices = {0U},
      .aggregates = {{VectorAggregateOperation::kCountStar, std::nullopt},
                     {VectorAggregateOperation::kSum, 2U}}};
  const DistributedVectorResultSchema grouped_schema{
      .columns = {{"bucket", inputs[0].type, false},
                  {"rows", type(schema::LogicalTypeKind::kInt64), false},
                  {"sum_value", inputs[2].type, true}}};
  EXPECT_TRUE(validate_distributed_vector_result_schema(grouped, inputs, grouped_schema).is_ok());

  DistributedVectorResultSchema mismatch = grouped_schema;
  mismatch.columns.back().nullable = false;
  EXPECT_EQ(validate_distributed_vector_result_schema(grouped, inputs, mismatch).code(),
            common::StatusCode::kInvalidArgument);
  mismatch = grouped_schema;
  mismatch.columns.pop_back();
  EXPECT_EQ(validate_distributed_vector_result_schema(grouped, inputs, mismatch).code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
