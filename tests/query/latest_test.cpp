#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/latest.hpp"
#include "chronos/schema/logical_type.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

class EmptySource final : public PhysicalOperator {
public:
  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

class ChunkSource final : public PhysicalOperator {
public:
  explicit ChunkSource(std::vector<AccountedVectorChunk> chunks) : chunks_(std::move(chunks)) {}

  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (cursor_ == chunks_.size())
      return PhysicalOperatorStep::end();
    return PhysicalOperatorStep::chunk(std::move(chunks_[cursor_++]));
  }

private:
  std::vector<AccountedVectorChunk> chunks_;
  std::size_t cursor_{};
};

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

void set_bit(std::vector<std::byte>& bytes, const std::uint32_t row) {
  bytes[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

template <typename Value>
[[nodiscard]] columnar::OwnedPhysicalColumn
fixed_column(const schema::LogicalTypeKind kind,
             const std::span<const std::optional<Value>> values) {
  static_assert(std::is_integral_v<Value>);
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  buffers.values.resize(values.size() * sizeof(Value));
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
      continue;
    }
    set_bit(buffers.validity, static_cast<std::uint32_t>(row));
    using Unsigned = std::make_unsigned_t<Value>;
    const Unsigned bits =
        std::bit_cast<Unsigned>(values[row].value()); // NOLINT(bugprone-unchecked-optional-access)
    for (std::size_t byte = 0U; byte < sizeof(Value); ++byte) {
      buffers.values[row * sizeof(Value) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & Unsigned{0xffU});
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(kind),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

template <typename Value>
[[nodiscard]] columnar::OwnedPhysicalColumn unsigned_column(const schema::LogicalTypeKind kind,
                                                            const std::span<const Value> values) {
  static_assert(std::is_unsigned_v<Value>);
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(values.size() * sizeof(Value));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    for (std::size_t byte = 0U; byte < sizeof(Value); ++byte) {
      buffers.values[row * sizeof(Value) + byte] =
          static_cast<std::byte>((values[row] >> (byte * 8U)) & Value{0xffU});
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(kind),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
string_column(const std::span<const std::optional<std::string>> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  buffers.offsets.resize((values.size() + 1U) * sizeof(std::uint32_t));
  const auto store_offset = [&buffers](const std::size_t row, const std::uint32_t value) {
    for (std::size_t byte = 0U; byte < sizeof(value); ++byte) {
      buffers.offsets[row * sizeof(value) + byte] =
          static_cast<std::byte>((value >> (byte * 8U)) & 0xffU);
    }
  };
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
    } else {
      set_bit(buffers.validity, static_cast<std::uint32_t>(row));
      for (const char byte : values[row].value()) // NOLINT(bugprone-unchecked-optional-access)
        buffers.values.push_back(static_cast<std::byte>(byte));
    }
    store_offset(row + 1U, static_cast<std::uint32_t>(buffers.values.size()));
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kString),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
uuid_column(const std::span<const common::Uuid> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.reserve(values.size() * common::Uuid::kSize);
  for (const common::Uuid& value : values)
    buffers.values.insert(buffers.values.end(), value.bytes().begin(), value.bytes().end());
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kUuid),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] AccountedVectorChunk latest_input(const QueryResourceContext& resources) {
  const std::array<std::optional<std::string>, 6U> groups{"A", "A", "A", "B", "B", std::nullopt};
  const std::array<std::optional<std::int64_t>, 6U> timestamps{std::nullopt, 10,           10,
                                                               std::nullopt, std::nullopt, 5};
  const std::array<std::optional<std::int64_t>, 6U> physical{1, 1, 1, 2, std::nullopt, 1};
  const std::array<std::optional<std::int64_t>, 6U> payload{10, 11, 12, 20, 21, 30};
  common::Uuid::Bytes wal_bytes{};
  wal_bytes.front() = std::byte{1U};
  const common::Uuid wal{wal_bytes};
  const std::array<common::Uuid, 6U> wal_ids{wal, wal, wal, wal, wal, wal};
  constexpr std::array<std::uint64_t, 6U> kSequences{1, 1, 2, 1, 1, 1};
  constexpr std::array<std::uint32_t, 6U> kRows{0, 0, 0, 0, 0, 0};
  constexpr std::array<std::uint8_t, 6U> kOperations{1, 1, 1, 1, 1, 1};
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(string_column(groups));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kTimestampNs,
                                 std::span<const std::optional<std::int64_t>>{timestamps}));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64,
                                 std::span<const std::optional<std::int64_t>>{physical}));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64,
                                 std::span<const std::optional<std::int64_t>>{payload}));
  columns.push_back(uuid_column(wal_ids));
  columns.push_back(unsigned_column(schema::LogicalTypeKind::kUInt64,
                                    std::span<const std::uint64_t>{kSequences}));
  columns.push_back(
      unsigned_column(schema::LogicalTypeKind::kUInt32, std::span<const std::uint32_t>{kRows}));
  columns.push_back(
      unsigned_column(schema::LogicalTypeKind::kUInt8, std::span<const std::uint8_t>{kOperations}));
  VectorChunk chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(6U).value()).value();
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(16'384U).value(),
                                      resources)
      .value();
}

[[nodiscard]] std::int64_t read_i64(const VectorChunk& chunk, const std::size_t column,
                                    const std::size_t row) {
  const common::ByteView bytes = chunk.cell({column, row}).value().bytes().value();
  std::uint64_t bits = 0U;
  for (std::size_t byte = 0U; byte < bytes.size(); ++byte)
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[byte])) << (byte * 8U);
  return std::bit_cast<std::int64_t>(bits);
}

TEST(LatestByOperatorTest, RejectsMissingGroupOrPhysicalIdentity) {
  auto missing_group =
      LatestByOperator::create(std::make_unique<EmptySource>(),
                               VectorLatestByDefinition{.key_column_ordinals = {},
                                                        .timestamp_column_ordinal = 1U,
                                                        .physical_ordering_key_ordinals = {1U},
                                                        .row_version_first_column_ordinal = 2U});
  ASSERT_FALSE(missing_group.has_value());
  EXPECT_EQ(missing_group.error().code(), common::StatusCode::kInvalidArgument);

  auto missing_physical =
      LatestByOperator::create(std::make_unique<EmptySource>(),
                               VectorLatestByDefinition{.key_column_ordinals = {0U},
                                                        .timestamp_column_ordinal = 1U,
                                                        .physical_ordering_key_ordinals = {},
                                                        .row_version_first_column_ordinal = 2U});
  ASSERT_FALSE(missing_physical.has_value());
  EXPECT_EQ(missing_physical.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(LatestByOperatorTest, UsesTimestampPhysicalIdentityVersionAndNullGroupSemantics) {
  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(latest_input(resources));
  auto latest =
      LatestByOperator::create(std::make_unique<ChunkSource>(std::move(chunks)),
                               VectorLatestByDefinition{.key_column_ordinals = {0U},
                                                        .timestamp_column_ordinal = 1U,
                                                        .physical_ordering_key_ordinals = {2U},
                                                        .row_version_first_column_ordinal = 4U});
  ASSERT_TRUE(latest.has_value()) << latest.error().message();
  auto step = (*latest)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().message();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& output = step->chunk()->chunk();
  ASSERT_EQ(output.selected_row_count(), 3U);
  EXPECT_EQ(read_i64(output, 3U, 0U), 12);
  EXPECT_EQ(read_i64(output, 3U, 1U), 21);
  EXPECT_EQ(read_i64(output, 3U, 2U), 30);
  step = (*latest)->next(resources);
  ASSERT_TRUE(step.has_value());
  EXPECT_EQ(step->kind(), PhysicalOperatorStepKind::kEnd);
  step = PhysicalOperatorStep::end();
  (*latest).reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(LatestByOperatorTest, RejectsRuntimeSuffixShapeCancelsAndReleasesCredit) {
  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(latest_input(resources));
  auto latest =
      LatestByOperator::create(std::make_unique<ChunkSource>(std::move(chunks)),
                               VectorLatestByDefinition{.key_column_ordinals = {0U},
                                                        .timestamp_column_ordinal = 1U,
                                                        .physical_ordering_key_ordinals = {2U},
                                                        .row_version_first_column_ordinal = 3U})
          .value();
  auto step = latest->next(resources);
  ASSERT_FALSE(step.has_value());
  EXPECT_EQ(step.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(resources.is_cancelled());
  latest.reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::query
