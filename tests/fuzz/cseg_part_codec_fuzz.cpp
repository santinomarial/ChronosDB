#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

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

void exercise(const chronos::common::ByteView bytes) {
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
  if (exact.has_value() && exact->encoded_part().size() != bytes.size()) {
    std::abort();
  }
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
  return 0;
}
