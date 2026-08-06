#include "chronos/ingest/tablet_state.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

enum class KeyPattern : std::uint8_t {
  kDuplicateZero,
  kUnique,
  kUniqueSecond,
  kSignedZeros,
  kNegativeZeroAndTwo,
  kRepeatedNan,
};

[[nodiscard]] std::size_t fixed_width(const schema::LogicalTypeKind kind) {
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

[[nodiscard]] schema::LogicalType logical_type(const schema::LogicalTypeKind kind) {
  return kind == schema::LogicalTypeKind::kDecimal ? schema::LogicalType::decimal(38U, 0U).value()
                                                   : schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema>
key_schema(const schema::LogicalTypeKind kind, const std::uint8_t seed) {
  const schema::ColumnId event_time = columnar::test::id<schema::ColumnId>(1U);
  const schema::ColumnId key = columnar::test::id<schema::ColumnId>(2U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_time, "event_time",
                        columnar::test::type(schema::LogicalTypeKind::kTimestampNs), false)
                        .value());
  columns.push_back(
      schema::ColumnDefinition::create(key, "logical_key", logical_type(kind), false).value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(
          columnar::test::id<schema::TableId>(static_cast<std::uint8_t>(100U + seed)),
          columnar::test::id<schema::SchemaId>(static_cast<std::uint8_t>(150U + seed)),
          schema::SchemaVersion::initial(), std::nullopt, std::move(columns),
          schema::TableSchemaRoles{.event_time_column = event_time,
                                   .physical_ordering_key = {event_time},
                                   .partition_columns = {event_time, key},
                                   .shard_key = {key},
                                   .deduplication_key = {key}})
          .value());
}

[[nodiscard]] columnar::OwnedColumnVector key_vector(const schema::LogicalTypeKind kind,
                                                     const KeyPattern pattern) {
  constexpr std::uint32_t kRows = 2U;
  const schema::LogicalType type = logical_type(kind);
  columnar::ColumnVectorBuffers buffers;
  if (kind == schema::LogicalTypeKind::kBool) {
    buffers.values = {pattern == KeyPattern::kDuplicateZero ? std::byte{0x00} : std::byte{0x02}};
  } else if (type.is_variable_width()) {
    columnar::test::append_u32(buffers.offsets, 0U);
    if (pattern == KeyPattern::kUnique || pattern == KeyPattern::kUniqueSecond) {
      columnar::test::append_u32(buffers.offsets, 1U);
      columnar::test::append_u32(buffers.offsets, 2U);
      buffers.values = pattern == KeyPattern::kUnique
                           ? std::vector<std::byte>{std::byte{'a'}, std::byte{'b'}}
                           : std::vector<std::byte>{std::byte{'c'}, std::byte{'d'}};
    } else {
      columnar::test::append_u32(buffers.offsets, 0U);
      columnar::test::append_u32(buffers.offsets, 0U);
    }
  } else {
    const std::size_t width = fixed_width(kind);
    buffers.values.resize(width * kRows);
    if (pattern == KeyPattern::kUnique) {
      buffers.values[width] = std::byte{1U};
    } else if (pattern == KeyPattern::kUniqueSecond) {
      buffers.values[0U] = std::byte{2U};
      buffers.values[width] = std::byte{3U};
    } else if (pattern == KeyPattern::kSignedZeros) {
      buffers.values[(width * 2U) - 1U] = std::byte{0x80};
    } else if (pattern == KeyPattern::kNegativeZeroAndTwo) {
      buffers.values[width - 1U] = std::byte{0x80};
      buffers.values[width] = std::byte{2U};
    } else if (pattern == KeyPattern::kRepeatedNan) {
      if (kind == schema::LogicalTypeKind::kFloat32) {
        buffers.values = {std::byte{0x00}, std::byte{0x00}, std::byte{0xc0}, std::byte{0x7f},
                          std::byte{0x00}, std::byte{0x00}, std::byte{0xc0}, std::byte{0x7f}};
      } else {
        buffers.values = {
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x00}, std::byte{0x00}, std::byte{0xf8}, std::byte{0x7f},
            std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
            std::byte{0x00}, std::byte{0x00}, std::byte{0xf8}, std::byte{0x7f},
        };
      }
    }
  }
  return columnar::OwnedColumnVector::create(
             columnar::ColumnVectorMetadata{.column_id = columnar::test::id<schema::ColumnId>(2U),
                                            .type = type,
                                            .nullable = false,
                                            .row_count = kRows,
                                            .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
key_batch(const std::shared_ptr<const schema::TableSchema>& schema,
          const schema::LogicalTypeKind kind, const KeyPattern pattern) {
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(
      columnar::test::fixed_vector(1U, columnar::test::type(schema::LogicalTypeKind::kTimestampNs),
                                   false, 2U, {}, 0U, std::vector<std::byte>(16U)));
  columns.push_back(key_vector(kind, pattern));
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(schema, std::move(columns)).value());
}

[[nodiscard]] TabletState tablet(const std::shared_ptr<const schema::TableSchema>& schema,
                                 const schema::LogicalTypeKind kind,
                                 const std::uint32_t row_capacity = 8U) {
  return TabletState::create(
             schema, columnar::test::id<schema::TabletId>(70U),
             TabletStateConfig{
                 .head_capacity =
                     head::MutableHeadCapacity{
                         .row_capacity = row_capacity,
                         .variable_value_bytes = {0U, logical_type(kind).is_variable_width() ? 16U
                                                                                             : 0U}},
                 .maximum_schema_versions = 1U,
                 .maximum_sealed_generations = 2U,
                 .maximum_retry_entries = 8U})
      .value();
}

[[nodiscard]] RetryIdentity retry_identity(const std::uint8_t seed) {
  return RetryIdentity{.client_id = test::request_id<ClientId>(seed),
                       .client_batch_id =
                           test::request_id<ClientBatchId>(static_cast<std::uint8_t>(seed + 32U))};
}

[[nodiscard]] ColumnarAppendMutationIdentity
mutation(const std::shared_ptr<const schema::TableSchema>& schema, const std::uint8_t seed) {
  Sha256Digest::Bytes digest{};
  digest.back() = static_cast<std::byte>(seed);
  return ColumnarAppendMutationIdentity{.table_id = schema->table_id(),
                                        .tablet_id = columnar::test::id<schema::TabletId>(70U),
                                        .request_digest = Sha256Digest{digest}};
}

[[nodiscard]] head::HeadCommitPosition position(const std::uint64_t sequence) {
  wal::WalId id;
  id.bytes.back() = std::byte{1U};
  return head::HeadCommitPosition{.wal_id = id, .record_sequence = sequence};
}

[[nodiscard]] common::Result<PreparedTabletAppend>
prepare(TabletState& target, const std::shared_ptr<const schema::TableSchema>& schema,
        const std::uint8_t seed, const std::shared_ptr<const columnar::OwnedColumnarBatch>& batch) {
  return target.prepare_append(retry_identity(seed), mutation(schema, seed), batch);
}

void publish(PreparedTabletAppend& prepared, const std::uint64_t sequence) {
  ASSERT_TRUE(prepared.mark_wal_started().is_ok());
  ASSERT_TRUE(prepared.publish(position(sequence)).has_value());
}

TEST(DeduplicationKeyTest, RejectsDuplicateKeysForEveryFrozenLogicalType) {
  for (std::uint16_t code = 1U; code <= 18U; ++code) {
    SCOPED_TRACE(code);
    const schema::LogicalTypeKind kind = schema::logical_type_kind_from_code(code).value();
    const auto schema = key_schema(kind, static_cast<std::uint8_t>(code));
    TabletState target = tablet(schema, kind);
    const auto duplicate = key_batch(schema, kind, KeyPattern::kDuplicateZero);
    const auto rejected = prepare(target, schema, 1U, duplicate);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
    EXPECT_EQ(target.snapshot()->visible_row_count(), 0U);

    const auto unique = key_batch(schema, kind, KeyPattern::kUnique);
    auto accepted = prepare(target, schema, 2U, unique);
    ASSERT_TRUE(accepted.has_value()) << accepted.error().to_string();
    EXPECT_TRUE(accepted->cancel_before_wal().is_ok());
  }
}

TEST(DeduplicationKeyTest, UsesIeeeEqualityForSignedZeroAndNan) {
  for (const schema::LogicalTypeKind kind :
       {schema::LogicalTypeKind::kFloat32, schema::LogicalTypeKind::kFloat64}) {
    const auto schema = key_schema(kind, static_cast<std::uint8_t>(kind));
    TabletState target = tablet(schema, kind);
    const auto signed_zeros = key_batch(schema, kind, KeyPattern::kSignedZeros);
    EXPECT_EQ(prepare(target, schema, 1U, signed_zeros).error().code(),
              common::StatusCode::kInvalidArgument);

    TabletState visible_zero = tablet(schema, kind);
    auto positive_zero =
        prepare(visible_zero, schema, 2U, key_batch(schema, kind, KeyPattern::kUnique));
    ASSERT_TRUE(positive_zero.has_value()) << positive_zero.error().to_string();
    publish(*positive_zero, 1U);
    const auto negative_zero =
        prepare(visible_zero, schema, 3U, key_batch(schema, kind, KeyPattern::kNegativeZeroAndTwo));
    ASSERT_FALSE(negative_zero.has_value());
    EXPECT_EQ(negative_zero.error().code(), common::StatusCode::kInvalidArgument);

    TabletState visible_nan = tablet(schema, kind);
    const auto nan = key_batch(schema, kind, KeyPattern::kRepeatedNan);
    auto first_nan = prepare(visible_nan, schema, 4U, nan);
    ASSERT_TRUE(first_nan.has_value()) << first_nan.error().to_string();
    publish(*first_nan, 1U);
    auto second_nan = prepare(visible_nan, schema, 5U, nan);
    ASSERT_TRUE(second_nan.has_value()) << second_nan.error().to_string();
    EXPECT_TRUE(second_nan->cancel_before_wal().is_ok());
  }
}

TEST(DeduplicationKeyTest, RejectsConflictsInActiveAndSealedGenerationsBeforeWal) {
  const schema::LogicalTypeKind kind = schema::LogicalTypeKind::kString;
  const auto schema = key_schema(kind, 20U);
  TabletState target = tablet(schema, kind, 2U);
  const auto first = key_batch(schema, kind, KeyPattern::kUnique);
  auto first_prepared = prepare(target, schema, 1U, first);
  ASSERT_TRUE(first_prepared.has_value()) << first_prepared.error().to_string();
  publish(*first_prepared, 1U);

  const auto active_conflict = prepare(target, schema, 2U, first);
  ASSERT_FALSE(active_conflict.has_value());
  EXPECT_EQ(active_conflict.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 2U);

  const auto second = key_batch(schema, kind, KeyPattern::kUniqueSecond);
  auto second_prepared = prepare(target, schema, 3U, second);
  ASSERT_TRUE(second_prepared.has_value()) << second_prepared.error().to_string();
  publish(*second_prepared, 2U);
  ASSERT_EQ(target.snapshot()->sealed_generations().size(), 1U);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 4U);

  const auto sealed_conflict = prepare(target, schema, 4U, first);
  ASSERT_FALSE(sealed_conflict.has_value());
  EXPECT_EQ(sealed_conflict.error().code(), common::StatusCode::kInvalidArgument);

  const auto other = key_batch(schema, kind, KeyPattern::kDuplicateZero);
  const auto intra_batch_rejected = prepare(target, schema, 5U, other);
  ASSERT_FALSE(intra_batch_rejected.has_value());
  EXPECT_EQ(intra_batch_rejected.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 4U);
}

TEST(DeduplicationKeyPropertyTest, DeterministicGeneratedKeysMatchAReferenceUniquenessSet) {
  constexpr std::uint32_t kRows = 257U;
  constexpr std::uint32_t kSeed = 0x44454455U;
  const schema::LogicalTypeKind kind = schema::LogicalTypeKind::kUInt32;
  const auto schema = key_schema(kind, 21U);

  std::vector<std::uint32_t> values(kRows);
  std::iota(values.begin(), values.end(), 0U);
  // NOLINTNEXTLINE(bugprone-random-generator-seed)
  std::mt19937 generator{kSeed};
  std::shuffle(values.begin(), values.end(), generator);
  const auto make_generated_batch = [&](const std::vector<std::uint32_t>& keys) {
    std::vector<std::byte> encoded;
    encoded.reserve(keys.size() * sizeof(std::uint32_t));
    for (const std::uint32_t key : keys) {
      columnar::test::append_u32(encoded, key);
    }
    std::vector<columnar::OwnedColumnVector> columns;
    columns.push_back(columnar::test::fixed_vector(
        1U, columnar::test::type(schema::LogicalTypeKind::kTimestampNs), false, kRows, {}, 0U,
        std::vector<std::byte>(static_cast<std::size_t>(kRows) * 8U)));
    columns.push_back(columnar::test::fixed_vector(2U, logical_type(kind), false, kRows, {}, 0U,
                                                   std::move(encoded)));
    return std::make_shared<const columnar::OwnedColumnarBatch>(
        columnar::OwnedColumnarBatch::create(schema, std::move(columns)).value());
  };

  TabletState target = tablet(schema, kind, kRows);
  auto unique = prepare(target, schema, 1U, make_generated_batch(values));
  ASSERT_TRUE(unique.has_value()) << unique.error().to_string();
  EXPECT_TRUE(unique->cancel_before_wal().is_ok());

  values.back() = values.front();
  const auto duplicate = prepare(target, schema, 2U, make_generated_batch(values));
  ASSERT_FALSE(duplicate.has_value());
  EXPECT_EQ(duplicate.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::ingest
