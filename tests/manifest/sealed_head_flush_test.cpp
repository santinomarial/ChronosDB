#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/cseg/validator.hpp"
#include "chronos/head/mutable_head.hpp"
#include "chronos/manifest/sealed_head_flush.hpp"
#include "chronos/manifest/storage.hpp"
#include "columnar/columnar_test_support.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint16_t value) {
  return columnar::test::id<Identifier>(value);
}

[[nodiscard]] wal::WalId wal_id() {
  wal::WalId value{};
  value.bytes.back() = std::byte{0x71U};
  return value;
}

template <typename Integer> void append_le(std::vector<std::byte>& bytes, const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

template <typename Integer> [[nodiscard]] Integer load_le(const common::ByteView bytes) {
  using Unsigned = std::make_unsigned_t<Integer>;
  Unsigned value{};
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    value |= static_cast<Unsigned>(std::to_integer<std::uint8_t>(bytes[index])) << (index * 8U);
  }
  return std::bit_cast<Integer>(value);
}

[[nodiscard]] std::uint32_t independent_crc32c(const common::ByteView bytes) {
  std::uint32_t crc = 0xffffffffU;
  for (const std::byte byte : bytes) {
    crc ^= std::to_integer<std::uint8_t>(byte);
    for (std::size_t bit = 0U; bit < 8U; ++bit) {
      const std::uint32_t mask = 0U - (crc & 1U);
      crc = (crc >> 1U) ^ (0x82f63b78U & mask);
    }
  }
  return ~crc;
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
batch(const std::shared_ptr<const schema::TableSchema>& schema,
      const std::array<std::int64_t, 2>& event_times,
      const std::array<std::optional<char>, 2>& tags, const std::array<bool, 2>& booleans) {
  std::vector<std::byte> events;
  for (const std::int64_t value : event_times) {
    append_le(events, value);
  }
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(
      columnar::test::fixed_vector(1U, columnar::test::type(schema::LogicalTypeKind::kTimestampNs),
                                   false, 2U, {}, 0U, std::move(events)));

  std::vector<std::byte> validity(1U, std::byte{0});
  std::vector<std::byte> offsets;
  std::vector<std::byte> values;
  append_le(offsets, std::uint32_t{0U});
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < tags.size(); ++row) {
    if (tags[row].has_value()) {
      validity.front() |= static_cast<std::byte>(1U << row);
      values.push_back(static_cast<std::byte>(*tags[row]));
    } else {
      ++null_count;
    }
    append_le(offsets, static_cast<std::uint32_t>(values.size()));
  }
  columns.push_back(columnar::OwnedColumnVector::create(
                        {.column_id = id<schema::ColumnId>(2U),
                         .type = columnar::test::type(schema::LogicalTypeKind::kString),
                         .nullable = true,
                         .row_count = 2U,
                         .null_count = null_count},
                        {.validity = std::move(validity),
                         .offsets = std::move(offsets),
                         .values = std::move(values)})
                        .value());
  std::vector<std::byte> bool_values(1U, std::byte{0});
  for (std::size_t row = 0U; row < booleans.size(); ++row) {
    if (booleans[row]) {
      bool_values.front() |= static_cast<std::byte>(1U << row);
    }
  }
  columns.push_back(
      columnar::test::fixed_vector(3U, columnar::test::type(schema::LogicalTypeKind::kBool), false,
                                   2U, {}, 0U, std::move(bool_values)));
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(schema, std::move(columns)).value());
}

[[nodiscard]] head::MutableHead populated_head() {
  const std::shared_ptr<const schema::TableSchema> schema = columnar::test::batch_schema();
  head::MutableHead target =
      head::MutableHead::create(schema, id<schema::TabletId>(70U), 3U,
                                {.row_capacity = 4U, .variable_value_bytes = {0U, 4U, 0U}})
          .value();
  const std::array batches{
      batch(schema, {30, 10}, {'c', 'a'}, {true, false}),
      batch(schema, {20, 10}, {std::nullopt, 'b'}, {false, true}),
  };
  std::uint64_t sequence = 7U;
  for (const auto& input : batches) {
    head::PreparedHeadAppend prepared = target.prepare_append(input).value();
    EXPECT_TRUE(prepared.mark_wal_started().is_ok());
    EXPECT_TRUE(prepared.publish({.wal_id = wal_id(), .record_sequence = sequence}).has_value());
    sequence += 2U;
  }
  return target;
}

[[nodiscard]] cseg::PartId part_id() {
  return id<cseg::PartId>(90U);
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-sealed-flush-XXXXXX").string();
    char* const created = ::mkdtemp(pattern.data());
    if (created != nullptr) {
      path_ = created;
    }
  }

  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

TEST(SealedHeadFlushTest, RejectsActiveAndEmptySealedGenerations) {
  head::MutableHead active = populated_head();
  const head::HeadSnapshot active_snapshot = active.snapshot().value();
  EXPECT_EQ(
      encode_sealed_head_v1({.snapshot = active_snapshot, .part_id = part_id()}).error().code(),
      common::StatusCode::kInvalidArgument);

  const auto schema = columnar::test::batch_schema();
  head::MutableHead empty =
      head::MutableHead::create(schema, id<schema::TabletId>(70U), 4U,
                                {.row_capacity = 1U, .variable_value_bytes = {0U, 0U, 0U}})
          .value();
  const head::HeadSnapshot empty_snapshot = empty.seal().value();
  EXPECT_EQ(
      encode_sealed_head_v1({.snapshot = empty_snapshot, .part_id = part_id()}).error().code(),
      common::StatusCode::kInvalidArgument);
}

TEST(SealedHeadFlushTest, DeterministicallySortsMaterializesAndDescribesOneGeneration) {
  head::MutableHead target = populated_head();
  const head::HeadSnapshot snapshot = target.seal().value();
  const auto first = encode_sealed_head_v1(
      {.snapshot = snapshot, .part_id = part_id(), .compression = cseg::PageCompression::kNone});
  const auto second = encode_sealed_head_v1(
      {.snapshot = snapshot, .part_id = part_id(), .compression = cseg::PageCompression::kNone});
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  EXPECT_TRUE(std::ranges::equal(first->encoded_part.bytes(), second->encoded_part.bytes()));
  EXPECT_EQ(first->descriptor, second->descriptor);
  EXPECT_EQ(first->descriptor.part_id, part_id());
  EXPECT_EQ(first->descriptor.table_id, snapshot.table_id());
  EXPECT_EQ(first->descriptor.tablet_id, snapshot.tablet_id());
  EXPECT_EQ(first->descriptor.schema_id, snapshot.schema_ptr()->schema_id());
  EXPECT_EQ(first->descriptor.schema_version, snapshot.schema_ptr()->version());
  EXPECT_EQ(first->descriptor.file_length, first->encoded_part.size());
  EXPECT_EQ(first->descriptor.row_count, 4U);
  EXPECT_EQ(first->descriptor.minimum_record_sequence, 7U);
  EXPECT_EQ(first->descriptor.maximum_record_sequence, 9U);
  EXPECT_EQ(first->descriptor.minimum_event_time, 10);
  EXPECT_EQ(first->descriptor.maximum_event_time, 30);
  EXPECT_EQ(first->wal_id, wal_id());
  // Fingerprints the complete converter output using a tableless Castagnoli implementation that
  // is independent of the production CRC provider.
  EXPECT_EQ(independent_crc32c(first->encoded_part.bytes()), 0xf1ef9280U);
  EXPECT_EQ(common::crc32c(first->encoded_part.bytes()), 0xf1ef9280U);

  const cseg::CsegPartDecodeResult decoded =
      cseg::decode_cseg_v1_part_exact(first->encoded_part.bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().status().to_string();
  EXPECT_TRUE(
      cseg::validate_cseg_v1_part(*decoded, *snapshot.schema_ptr(), snapshot.tablet_id()).is_ok());
  ASSERT_EQ(decoded->metadata().granules().size(), 1U);
  EXPECT_EQ(decoded->metadata().granules().front().row_count, 4U);
  ASSERT_EQ(decoded->metadata().columns().size(), 7U);
  ASSERT_EQ(decoded->metadata().pages().size(), 7U);

  const cseg::DecodedCsegPage events = decoded->decode_page(0U).value();
  const cseg::DecodedCsegPage tags = decoded->decode_page(1U).value();
  const cseg::DecodedCsegPage booleans = decoded->decode_page(2U).value();
  const cseg::DecodedCsegPage sequences = decoded->decode_page(4U).value();
  const cseg::DecodedCsegPage ordinals = decoded->decode_page(5U).value();
  const cseg::DecodedCsegPage operations = decoded->decode_page(6U).value();
  constexpr std::array<std::int64_t, 4> expected_events{10, 10, 20, 30};
  constexpr std::array<std::uint64_t, 4> expected_sequences{7U, 9U, 9U, 7U};
  constexpr std::array<std::uint32_t, 4> expected_ordinals{1U, 1U, 0U, 0U};
  constexpr std::array<bool, 4> expected_booleans{false, true, false, true};
  const std::array<std::optional<char>, 4> expected_tags{'a', 'b', std::nullopt, 'c'};
  for (std::uint32_t row = 0U; row < expected_events.size(); ++row) {
    EXPECT_EQ(load_le<std::int64_t>(*events.physical().cell(row)->bytes()), expected_events[row]);
    EXPECT_EQ(load_le<std::uint64_t>(*sequences.physical().cell(row)->bytes()),
              expected_sequences[row]);
    EXPECT_EQ(load_le<std::uint32_t>(*ordinals.physical().cell(row)->bytes()),
              expected_ordinals[row]);
    EXPECT_EQ(booleans.physical().cell(row)->boolean().value(), expected_booleans[row]);
    EXPECT_EQ(std::to_integer<std::uint8_t>(operations.physical().cell(row)->bytes()->front()),
              cseg::format::kAppendRowsOperation);
    const auto tag = tags.physical().cell(row).value();
    EXPECT_EQ(tag.is_null(), !expected_tags[row].has_value());
    if (expected_tags[row].has_value()) {
      EXPECT_EQ(static_cast<char>(std::to_integer<std::uint8_t>(tag.bytes()->front())),
                expected_tags[row].value_or('\0'));
    }
  }
}

TEST(SealedHeadFlushTest, ExplicitCompressionPolicyIsDeterministicAndFullyValidated) {
  head::MutableHead target = populated_head();
  const head::HeadSnapshot snapshot = target.seal().value();
  const auto first = encode_sealed_head_v1(
      {.snapshot = snapshot, .part_id = part_id(), .compression = cseg::PageCompression::kZstd});
  const auto second = encode_sealed_head_v1(
      {.snapshot = snapshot, .part_id = part_id(), .compression = cseg::PageCompression::kZstd});
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  EXPECT_TRUE(std::ranges::equal(first->encoded_part.bytes(), second->encoded_part.bytes()));
  const auto decoded = cseg::decode_cseg_v1_part_exact(first->encoded_part.bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_TRUE(
      cseg::validate_cseg_v1_part(*decoded, *snapshot.schema_ptr(), snapshot.tablet_id()).is_ok());
  bool compressed_page = false;
  for (const cseg::CsegPageDescriptor& page : decoded->metadata().pages()) {
    EXPECT_TRUE(page.compression == cseg::PageCompression::kNone ||
                page.compression == cseg::PageCompression::kZstd);
    compressed_page = compressed_page || page.compression == cseg::PageCompression::kZstd;
  }
  EXPECT_TRUE(compressed_page);
}

TEST(SealedHeadFlushTest, ResultInstallsThroughTheDurablePartBoundaryWithoutTranslation) {
  head::MutableHead target = populated_head();
  const head::HeadSnapshot snapshot = target.seal().value();
  const auto encoded = encode_sealed_head_v1(
      {.snapshot = snapshot, .part_id = part_id(), .compression = cseg::PageCompression::kZstd});
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();

  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / kPartsDirectoryName));
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / kManifestDirectoryName));
  {
    std::ofstream lock{directory.path() / kManifestDirectoryName /
                       std::string{kManifestLockFileName}};
    ASSERT_TRUE(lock.good());
  }
  auto storage = ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  common::Uuid::Bytes nonce_bytes{};
  nonce_bytes.front() = std::byte{0xa1U};
  const auto installed = storage->install_part({.encoded_part = std::cref(encoded->encoded_part),
                                                .descriptor = encoded->descriptor,
                                                .wal_id = encoded->wal_id,
                                                .schema = std::cref(*snapshot.schema_ptr()),
                                                .nonce = common::Uuid{nonce_bytes},
                                                .validation_limits = {}});
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(installed->descriptor, encoded->descriptor);
  EXPECT_TRUE(std::filesystem::is_regular_file(directory.path() / kPartsDirectoryName /
                                               installed->file_name));
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema> one_column_schema() {
  const schema::ColumnId event_id = id<schema::ColumnId>(11U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_id, "event_time",
                        columnar::test::type(schema::LogicalTypeKind::kTimestampNs), false)
                        .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(12U), id<schema::SchemaId>(13U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = event_id,
                                   .physical_ordering_key = {event_id},
                                   .partition_columns = {event_id},
                                   .shard_key = {event_id},
                                   .deduplication_key = {}})
          .value());
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
one_column_batch(const std::shared_ptr<const schema::TableSchema>& schema,
                 const std::span<const std::int64_t> event_times) {
  std::vector<std::byte> values;
  values.reserve(event_times.size() * sizeof(std::int64_t));
  for (const std::int64_t event_time : event_times) {
    append_le(values, event_time);
  }
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(columnar::test::fixed_vector(
      11U, columnar::test::type(schema::LogicalTypeKind::kTimestampNs), false,
      static_cast<std::uint32_t>(event_times.size()), {}, 0U, std::move(values)));
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(schema, std::move(columns)).value());
}

TEST(SealedHeadFlushPropertyTest, GeneratedHeadsEncodeDeterministicallyAndPreserveExtrema) {
  constexpr std::uint64_t multiplier = 6'364'136'223'846'793'005ULL;
  constexpr std::uint64_t increment = 1'442'695'040'888'963'407ULL;
  const auto schema = one_column_schema();
  for (std::uint64_t seed = 1U; seed <= 32U; ++seed) {
    SCOPED_TRACE(seed);
    std::uint64_t state = seed * multiplier + increment;
    const std::uint32_t row_count = 1U + static_cast<std::uint32_t>(state % 257U);
    std::vector<std::int64_t> values;
    values.reserve(row_count);
    for (std::uint32_t row = 0U; row < row_count; ++row) {
      state = state * multiplier + increment;
      values.push_back(std::bit_cast<std::int64_t>(state));
    }
    const auto input = one_column_batch(schema, values);
    head::MutableHead target =
        head::MutableHead::create(schema, id<schema::TabletId>(14U), seed,
                                  {.row_capacity = row_count, .variable_value_bytes = {0U}})
            .value();
    head::PreparedHeadAppend prepared = target.prepare_append(input).value();
    ASSERT_TRUE(prepared.mark_wal_started().is_ok());
    ASSERT_TRUE(prepared.publish({.wal_id = wal_id(), .record_sequence = seed}).has_value());
    const head::HeadSnapshot snapshot = target.seal().value();
    const cseg::PageCompression compression =
        seed % 2U == 0U ? cseg::PageCompression::kNone : cseg::PageCompression::kZstd;
    const cseg::PartId generated_part = id<cseg::PartId>(static_cast<std::uint16_t>(100U + seed));
    const auto first = encode_sealed_head_v1(
        {.snapshot = snapshot, .part_id = generated_part, .compression = compression});
    const auto second = encode_sealed_head_v1(
        {.snapshot = snapshot, .part_id = generated_part, .compression = compression});
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    ASSERT_TRUE(second.has_value()) << second.error().to_string();
    EXPECT_TRUE(std::ranges::equal(first->encoded_part.bytes(), second->encoded_part.bytes()));
    EXPECT_EQ(first->descriptor.minimum_event_time, *std::ranges::min_element(values));
    EXPECT_EQ(first->descriptor.maximum_event_time, *std::ranges::max_element(values));
    const auto decoded = cseg::decode_cseg_v1_part_exact(first->encoded_part.bytes());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(cseg::validate_cseg_v1_part(*decoded, *schema, snapshot.tablet_id()).is_ok());
  }
}

TEST(SealedHeadFlushTest, PlansTheCanonicalMaximumRowGranuleBoundary) {
  constexpr std::uint32_t row_count = cseg::format::kMaximumGranuleRowCount + 1U;
  const auto schema = one_column_schema();
  std::vector<std::byte> values;
  values.reserve(static_cast<std::size_t>(row_count) * sizeof(std::int64_t));
  for (std::uint32_t row = row_count; row > 0U; --row) {
    append_le(values, static_cast<std::int64_t>(row));
  }
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(
      columnar::test::fixed_vector(11U, columnar::test::type(schema::LogicalTypeKind::kTimestampNs),
                                   false, row_count, {}, 0U, std::move(values)));
  const auto input = std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(schema, std::move(columns)).value());
  head::MutableHead target =
      head::MutableHead::create(schema, id<schema::TabletId>(14U), 1U,
                                {.row_capacity = row_count, .variable_value_bytes = {0U}})
          .value();
  head::PreparedHeadAppend prepared = target.prepare_append(input).value();
  ASSERT_TRUE(prepared.mark_wal_started().is_ok());
  ASSERT_TRUE(prepared.publish({.wal_id = wal_id(), .record_sequence = 1U}).has_value());
  const head::HeadSnapshot snapshot = target.seal().value();
  const auto encoded = encode_sealed_head_v1({.snapshot = snapshot, .part_id = part_id()});
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const auto decoded = cseg::decode_cseg_v1_part_exact(encoded->encoded_part.bytes());
  ASSERT_TRUE(decoded.has_value());
  ASSERT_EQ(decoded->metadata().granules().size(), 2U);
  EXPECT_EQ(decoded->metadata().granules()[0].row_count, cseg::format::kMaximumGranuleRowCount);
  EXPECT_EQ(decoded->metadata().granules()[1].row_count, 1U);
  EXPECT_EQ(load_le<std::int64_t>(*decoded->decode_page(0U)->physical().cell(0U)->bytes()), 1);
  EXPECT_EQ(load_le<std::int64_t>(*decoded->decode_page(5U)->physical().cell(0U)->bytes()),
            static_cast<std::int64_t>(row_count));
}

} // namespace
} // namespace chronos::manifest
