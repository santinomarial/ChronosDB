#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/inspection.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/cseg/projected_reader.hpp"
#include "chronos/cseg/validator.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <span>
#include <vector>

namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] std::vector<std::byte> canonical_part() {
  using namespace chronos;
  const schema::LogicalType timestamp =
      schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value();
  const schema::LogicalType uuid =
      schema::LogicalType::create(schema::LogicalTypeKind::kUuid).value();
  const schema::LogicalType uint64 =
      schema::LogicalType::create(schema::LogicalTypeKind::kUInt64).value();
  const schema::LogicalType uint32 =
      schema::LogicalType::create(schema::LogicalTypeKind::kUInt32).value();
  const schema::LogicalType uint8 =
      schema::LogicalType::create(schema::LogicalTypeKind::kUInt8).value();
  const std::vector<cseg::CsegColumnDescriptor> columns{
      {.column_id = id<schema::ColumnId>(5U),
       .storage_kind = cseg::StorageKind::kUser,
       .logical_type = timestamp,
       .nullable = false,
       .event_time = true,
       .schema_ordinal = 0U,
       .ordering_ordinal = 0U},
      {.column_id = std::nullopt,
       .storage_kind = cseg::StorageKind::kWalId,
       .logical_type = uuid,
       .nullable = false,
       .event_time = false,
       .schema_ordinal = std::nullopt,
       .ordering_ordinal = std::nullopt},
      {.column_id = std::nullopt,
       .storage_kind = cseg::StorageKind::kRecordSequence,
       .logical_type = uint64,
       .nullable = false,
       .event_time = false,
       .schema_ordinal = std::nullopt,
       .ordering_ordinal = std::nullopt},
      {.column_id = std::nullopt,
       .storage_kind = cseg::StorageKind::kRowOrdinal,
       .logical_type = uint32,
       .nullable = false,
       .event_time = false,
       .schema_ordinal = std::nullopt,
       .ordering_ordinal = std::nullopt},
      {.column_id = std::nullopt,
       .storage_kind = cseg::StorageKind::kOperation,
       .logical_type = uint8,
       .nullable = false,
       .event_time = false,
       .schema_ordinal = std::nullopt,
       .ordering_ordinal = std::nullopt},
  };
  const std::vector<cseg::CsegGranuleDescriptor> granules{{.first_row = 0U,
                                                           .row_count = 1U,
                                                           .first_page_index = 0U,
                                                           .minimum_event_time = 0,
                                                           .maximum_event_time = 0}};
  const std::array<std::byte, 8U> event{};
  common::Uuid::Bytes wal{};
  wal.front() = std::byte{1U};
  const std::array<std::byte, 8U> sequence{std::byte{1U}};
  const std::array<std::byte, 4U> ordinal{};
  const std::array<std::byte, 1U> operation{std::byte{1U}};
  const auto encode_page = [](const schema::LogicalType type, const common::ByteView values) {
    const auto physical = columnar::PhysicalColumnView::create(
        {.type = type, .nullable = false, .row_count = 1U, .null_count = 0U},
        {.validity = {}, .offsets = {}, .values = values});
    return cseg::encode_cseg_v1_page(*physical, cseg::PageCompression::kNone).value();
  };
  std::vector<cseg::EncodedCsegPage> pages;
  pages.reserve(5U);
  pages.push_back(encode_page(timestamp, event));
  pages.push_back(encode_page(uuid, wal));
  pages.push_back(encode_page(uint64, sequence));
  pages.push_back(encode_page(uint32, ordinal));
  pages.push_back(encode_page(uint8, operation));
  auto encoded = cseg::encode_cseg_v1_part({.part_id = id<cseg::PartId>(1U),
                                            .table_id = id<schema::TableId>(2U),
                                            .tablet_id = id<schema::TabletId>(3U),
                                            .schema_id = id<schema::SchemaId>(4U),
                                            .schema_version = schema::SchemaVersion::initial(),
                                            .row_count = 1U,
                                            .event_time_column_ordinal = 0U,
                                            .ordering_column_count = 1U,
                                            .minimum_event_time = 0,
                                            .maximum_event_time = 0,
                                            .columns = columns,
                                            .granules = granules,
                                            .pages = pages})
                     .value();
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

[[nodiscard]] const chronos::schema::SchemaLineage& canonical_lineage() {
  using namespace chronos;
  // NOLINTNEXTLINE(bugprone-throwing-static-initialization)
  static const schema::SchemaLineage lineage = [] {
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            id<schema::ColumnId>(5U), "event_time",
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    schema::TableSchema table =
        schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
                                    schema::SchemaVersion::initial(), std::nullopt,
                                    std::move(columns),
                                    {.event_time_column = id<schema::ColumnId>(5U),
                                     .physical_ordering_key = {id<schema::ColumnId>(5U)},
                                     .partition_columns = {id<schema::ColumnId>(5U)},
                                     .shard_key = {id<schema::ColumnId>(5U)},
                                     .deduplication_key = {}})
            .value();
    return schema::SchemaLineage::create(std::move(table)).value();
  }();
  return lineage;
}

void exercise(const chronos::common::ByteView bytes) {
  [[maybe_unused]] const auto inspection = chronos::cseg::inspect_cseg_v1_part(bytes);
  const auto prefix = chronos::cseg::decode_cseg_v1_part_prefix(bytes);
  if (prefix.has_value()) {
    if (prefix->encoded_part().size() > bytes.size() ||
        prefix->metadata().pages().size() !=
            prefix->metadata().columns().size() * prefix->metadata().granules().size()) {
      std::abort();
    }
    for (std::size_t index = 0U; index < prefix->metadata().pages().size(); ++index) {
      if (!prefix->decode_page(index).has_value()) {
        std::abort();
      }
    }
  }
  const auto exact = chronos::cseg::decode_cseg_v1_part_exact(bytes);
  if (exact.has_value()) {
    if (exact->encoded_part().size() != bytes.size()) {
      std::abort();
    }
    static_cast<void>(chronos::cseg::validate_cseg_v1_part_contents(*exact));
  }
  const auto projected = chronos::cseg::open_cseg_v1_projected_reader_prefix(
      bytes, canonical_lineage(), id<chronos::schema::SchemaId>(4U),
      id<chronos::schema::TabletId>(3U));
  if (projected.has_value()) {
    const std::array<std::uint32_t, 1> selection{0U};
    const auto selected_plan = projected->plan_granule(0U, selection);
    if (selected_plan.has_value()) [[maybe_unused]]
      const auto selected = projected->read_granule(*selected_plan);
    const auto system_plan = projected->plan_granule(0U, {});
    if (system_plan.has_value()) [[maybe_unused]]
      const auto system_only = projected->read_granule(*system_plan);
  }
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

void exercise_authenticated_semantics(std::vector<std::byte> bytes, const std::uint8_t* data,
                                      const std::size_t size) {
  const auto canonical = chronos::cseg::decode_cseg_v1_part_exact(bytes);
  if (!canonical.has_value()) {
    std::abort();
  }
  const std::size_t page_index = size == 0U ? 0U : static_cast<std::size_t>(data[0]) % 5U;
  const chronos::cseg::CsegPageDescriptor& page = canonical->metadata().pages()[page_index];
  const std::size_t in_page =
      size < 2U ? 0U : static_cast<std::size_t>(data[1]) % page.stored_length;
  bytes[static_cast<std::size_t>(page.page_offset) + in_page] ^=
      std::byte{size < 3U ? std::uint8_t{1U} : static_cast<std::uint8_t>(data[2] | 1U)};

  constexpr std::size_t first_page_descriptor =
      chronos::cseg::format::kFileHeaderLength +
      5U * chronos::cseg::format::kColumnDescriptorLength +
      chronos::cseg::format::kGranuleDescriptorLength;
  const std::size_t descriptor =
      first_page_descriptor + page_index * chronos::cseg::format::kPageDescriptorLength;
  const chronos::common::ByteView stored = chronos::common::ByteView{bytes}.subspan(
      static_cast<std::size_t>(page.page_offset), static_cast<std::size_t>(page.stored_length));
  store_u32(bytes, descriptor + chronos::cseg::format::kPageCrc32cOffset,
            chronos::common::crc32c(stored));
  const std::size_t metadata_crc = canonical->metadata().encoded_metadata().size() -
                                   chronos::cseg::format::kMetadataCrc32cLength;
  store_u32(bytes, metadata_crc,
            chronos::common::crc32c(chronos::common::ByteView{bytes}.first(metadata_crc)));
  const auto decoded = chronos::cseg::decode_cseg_v1_part_exact(bytes);
  if (!decoded.has_value()) {
    std::abort();
  }
  static_cast<void>(chronos::cseg::validate_cseg_v1_part_contents(*decoded));
  const auto projected = chronos::cseg::open_cseg_v1_projected_reader_exact(
      bytes, canonical_lineage(), id<chronos::schema::SchemaId>(4U),
      id<chronos::schema::TabletId>(3U));
  if (!projected.has_value()) {
    std::abort();
  }
  const auto system_plan = projected->plan_granule(0U, {});
  if (system_plan.has_value()) [[maybe_unused]]
    const auto system_only = projected->read_granule(*system_plan);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  exercise(chronos::common::byte_view(std::span<const std::uint8_t>{data, size}));

  // Keep a structurally rich valid seed in every run so mutations reach page and padding checks.
  // NOLINTNEXTLINE(bugprone-throwing-static-initialization)
  static const std::vector<std::byte> canonical = canonical_part();
  std::vector<std::byte> mutated = canonical;
  if (size != 0U) {
    const std::size_t offset = static_cast<std::size_t>(data[0]) % mutated.size();
    mutated[offset] ^=
        std::byte{size > 1U ? static_cast<std::uint8_t>(data[1] | 1U) : std::uint8_t{1U}};
  }
  exercise(mutated);
  exercise_authenticated_semantics(canonical, data, size);
  return 0;
}
