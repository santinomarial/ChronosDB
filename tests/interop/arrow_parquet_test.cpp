#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/status.hpp"
#include "chronos/interop/arrow_parquet.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "columnar/columnar_test_support.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::interop {
namespace {

class TemporaryPath {
public:
  explicit TemporaryPath(const std::string& extension) {
    static std::atomic<std::uint64_t> sequence{0U};
    path_ = std::filesystem::temp_directory_path() /
            ("chronos-interop-test-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1U, std::memory_order_relaxed)) + extension);
  }
  TemporaryPath(const TemporaryPath&) = delete;
  TemporaryPath& operator=(const TemporaryPath&) = delete;
  ~TemporaryPath() {
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] schema::LogicalType logical_type(const schema::LogicalTypeKind kind) {
  if (kind == schema::LogicalTypeKind::kDecimal) {
    return schema::LogicalType::decimal(20U, 4U).value();
  }
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::size_t width(const schema::LogicalTypeKind kind) {
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kUInt8:
    return 1U;
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kUInt16:
    return 2U;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kDate:
    return 4U;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kTimestampNs:
    return 8U;
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kUuid:
    return 16U;
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return 0U;
  }
  return 0U;
}

[[nodiscard]] columnar::OwnedColumnVector make_vector(const std::uint16_t identifier,
                                                      const schema::LogicalTypeKind kind,
                                                      const bool nullable) {
  columnar::ColumnVectorBuffers buffers;
  if (nullable) {
    buffers.validity = {std::byte{0x01}};
  }
  if (kind == schema::LogicalTypeKind::kBool) {
    buffers.values = {std::byte{0x01}};
  } else if (kind == schema::LogicalTypeKind::kSymbol || kind == schema::LogicalTypeKind::kString ||
             kind == schema::LogicalTypeKind::kBinary) {
    columnar::test::append_u32(buffers.offsets, 0U);
    columnar::test::append_u32(buffers.offsets, 1U);
    columnar::test::append_u32(buffers.offsets, 1U);
    buffers.values = {std::byte{'x'}};
  } else {
    buffers.values.resize(width(kind) * 2U, std::byte{0});
    buffers.values[0] = std::byte{0x01};
  }
  return columnar::OwnedColumnVector::create(
             columnar::ColumnVectorMetadata{.column_id =
                                                columnar::test::id<schema::ColumnId>(identifier),
                                            .type = logical_type(kind),
                                            .nullable = nullable,
                                            .row_count = 2U,
                                            .null_count = nullable ? 1U : 0U},
             std::move(buffers))
      .value();
}

struct FixtureBatch {
  std::shared_ptr<const schema::TableSchema> schema;
  columnar::OwnedColumnarBatch batch;
};

[[nodiscard]] FixtureBatch every_type_batch() {
  std::vector<schema::ColumnDefinition> definitions;
  std::vector<columnar::OwnedColumnVector> vectors;
  for (std::uint16_t code = static_cast<std::uint16_t>(schema::LogicalTypeKind::kBool);
       code <= static_cast<std::uint16_t>(schema::LogicalTypeKind::kUuid); ++code) {
    const schema::LogicalTypeKind kind = static_cast<schema::LogicalTypeKind>(code);
    const bool nullable = kind != schema::LogicalTypeKind::kTimestampNs;
    definitions.push_back(schema::ColumnDefinition::create(
                              columnar::test::id<schema::ColumnId>(code),
                              "column_" + std::to_string(code), logical_type(kind), nullable)
                              .value());
    vectors.push_back(make_vector(code, kind, nullable));
  }
  const schema::ColumnId event_time = columnar::test::id<schema::ColumnId>(
      static_cast<std::uint16_t>(schema::LogicalTypeKind::kTimestampNs));
  schema::TableSchemaRoles roles{.event_time_column = event_time,
                                 .physical_ordering_key = {event_time},
                                 .partition_columns = {event_time},
                                 .shard_key = {event_time},
                                 .deduplication_key = {}};
  auto target = std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(
          columnar::test::id<schema::TableId>(100U), columnar::test::id<schema::SchemaId>(101U),
          schema::SchemaVersion::initial(), std::nullopt, std::move(definitions), std::move(roles))
          .value());
  columnar::OwnedColumnarBatch batch =
      columnar::OwnedColumnarBatch::create(target, std::move(vectors)).value();
  return FixtureBatch{.schema = std::move(target), .batch = std::move(batch)};
}

void expect_same_batch(const columnar::OwnedColumnarBatch& expected,
                       const columnar::OwnedColumnarBatch& actual) {
  ASSERT_EQ(actual.schema(), expected.schema());
  ASSERT_EQ(actual.row_count(), expected.row_count());
  ASSERT_EQ(actual.columns().size(), expected.columns().size());
  for (std::size_t ordinal = 0; ordinal < expected.columns().size(); ++ordinal) {
    const columnar::ColumnVectorView left = expected.columns()[ordinal].view();
    const columnar::ColumnVectorView right = actual.columns()[ordinal].view();
    EXPECT_EQ(right.column_id(), left.column_id());
    EXPECT_EQ(right.type(), left.type());
    EXPECT_EQ(right.nullable(), left.nullable());
    EXPECT_EQ(right.null_count(), left.null_count());
    EXPECT_TRUE(std::ranges::equal(right.validity(), left.validity()));
    EXPECT_TRUE(std::ranges::equal(right.offsets(), left.offsets()));
    EXPECT_TRUE(std::ranges::equal(right.values(), left.values()));
  }
}

TEST(ArrowParquetInteropTest, ArrowIpcRoundTripsEveryLogicalType) {
  FixtureBatch fixture = every_type_batch();
  TemporaryPath file{".arrow"};
  ASSERT_TRUE(write_arrow_ipc_file(fixture.batch, file.path()).is_ok());
  common::Result<columnar::OwnedColumnarBatch> imported =
      read_arrow_ipc_file(file.path(), fixture.schema);
  ASSERT_TRUE(imported.has_value()) << imported.error().to_string();
  expect_same_batch(fixture.batch, *imported);
}

TEST(ArrowParquetInteropTest, ParquetRoundTripsEveryLogicalType) {
  FixtureBatch fixture = every_type_batch();
  TemporaryPath file{".parquet"};
  ASSERT_TRUE(write_parquet_file(fixture.batch, file.path()).is_ok());
  common::Result<columnar::OwnedColumnarBatch> imported =
      read_parquet_file(file.path(), fixture.schema);
  ASSERT_TRUE(imported.has_value()) << imported.error().to_string();
  expect_same_batch(fixture.batch, *imported);
}

TEST(ArrowParquetInteropTest, RejectsMismatchedTargetSchema) {
  const std::shared_ptr<const schema::TableSchema> source = columnar::test::batch_schema();
  columnar::OwnedColumnarBatch batch =
      columnar::OwnedColumnarBatch::create(source, columnar::test::batch_columns()).value();
  TemporaryPath file{".arrow"};
  ASSERT_TRUE(write_arrow_ipc_file(batch, file.path()).is_ok());
  common::Result<columnar::OwnedColumnarBatch> imported =
      read_arrow_ipc_file(file.path(), columnar::test::successor_batch_schema());
  ASSERT_FALSE(imported.has_value());
  EXPECT_EQ(imported.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(ArrowParquetInteropTest, EnforcesInputFileLimitBeforeDecode) {
  FixtureBatch fixture = every_type_batch();
  TemporaryPath file{".parquet"};
  ASSERT_TRUE(write_parquet_file(fixture.batch, file.path()).is_ok());
  ImportLimits limits;
  limits.max_file_bytes = 1U;
  common::Result<columnar::OwnedColumnarBatch> imported =
      read_parquet_file(file.path(), fixture.schema, limits);
  ASSERT_FALSE(imported.has_value());
  EXPECT_EQ(imported.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(ArrowParquetInteropTest, EnforcesDecodedBufferLimitBeforeCanonicalConversion) {
  FixtureBatch fixture = every_type_batch();
  TemporaryPath file{".arrow"};
  ASSERT_TRUE(write_arrow_ipc_file(fixture.batch, file.path()).is_ok());
  ImportLimits limits;
  limits.batch.max_buffer_bytes = 1U;
  limits.batch.max_retained_buffer_bytes = 1U;
  common::Result<columnar::OwnedColumnarBatch> imported =
      read_arrow_ipc_file(file.path(), fixture.schema, limits);
  ASSERT_FALSE(imported.has_value());
  EXPECT_EQ(imported.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(ArrowParquetInteropTest, RejectsCorruptFiles) {
  TemporaryPath arrow_file{".arrow"};
  TemporaryPath parquet_file{".parquet"};
  {
    std::ofstream output{arrow_file.path(), std::ios::binary};
    output << "not an Arrow file";
  }
  {
    std::ofstream output{parquet_file.path(), std::ios::binary};
    output << "not an Arrow file";
  }
  common::Result<columnar::OwnedColumnarBatch> arrow =
      read_arrow_ipc_file(arrow_file.path(), columnar::test::batch_schema());
  ASSERT_FALSE(arrow.has_value());
  EXPECT_EQ(arrow.error().code(), common::StatusCode::kInvalidArgument);
  common::Result<columnar::OwnedColumnarBatch> parquet =
      read_parquet_file(parquet_file.path(), columnar::test::batch_schema());
  ASSERT_FALSE(parquet.has_value());
  EXPECT_EQ(parquet.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::interop
