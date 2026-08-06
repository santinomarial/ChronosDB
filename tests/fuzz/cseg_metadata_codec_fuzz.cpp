#include "chronos/common/bytes.hpp"
#include "chronos/cseg/metadata_codec.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>
#include <vector>

namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t value) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(value);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] chronos::schema::LogicalType type(const chronos::schema::LogicalTypeKind kind) {
  return chronos::schema::LogicalType::create(kind).value();
}

void exercise(const chronos::common::ByteView bytes) {
  using chronos::cseg::CsegMetadataDecodeErrorKind;
  const auto prefix = chronos::cseg::decode_cseg_v1_metadata_prefix(bytes);
  if (prefix.has_value()) {
    if (prefix->encoded_metadata().data() != bytes.data() ||
        prefix->encoded_metadata().size() > bytes.size() || prefix->row_count() == 0U ||
        prefix->columns().size() < chronos::cseg::format::kSystemColumnCount ||
        prefix->pages().empty()) {
      std::abort();
    }
  } else if (prefix.error().kind() == CsegMetadataDecodeErrorKind::kIncomplete &&
             (prefix.error().required_size() <= bytes.size() ||
              prefix.error().status().code() != chronos::common::StatusCode::kOutOfRange)) {
    std::abort();
  }

  const auto exact = chronos::cseg::decode_cseg_v1_metadata_exact(bytes);
  if (exact.has_value()) {
    if (!prefix.has_value() || exact->encoded_metadata().size() != bytes.size()) {
      std::abort();
    }
  } else if (prefix.has_value() && prefix->encoded_metadata().size() == bytes.size()) {
    std::abort();
  }
}

[[nodiscard]] std::vector<std::byte> structured_metadata() {
  using namespace chronos::cseg;
  using chronos::schema::LogicalTypeKind;
  const std::array<CsegColumnDescriptor, 5U> columns{
      CsegColumnDescriptor{.column_id = id<chronos::schema::ColumnId>(5U),
                           .storage_kind = StorageKind::kUser,
                           .logical_type = type(LogicalTypeKind::kTimestampNs),
                           .nullable = false,
                           .event_time = true,
                           .schema_ordinal = 0U,
                           .ordering_ordinal = 0U},
      CsegColumnDescriptor{.column_id = std::nullopt,
                           .storage_kind = StorageKind::kWalId,
                           .logical_type = type(LogicalTypeKind::kUuid),
                           .nullable = false,
                           .event_time = false,
                           .schema_ordinal = std::nullopt,
                           .ordering_ordinal = std::nullopt},
      CsegColumnDescriptor{.column_id = std::nullopt,
                           .storage_kind = StorageKind::kRecordSequence,
                           .logical_type = type(LogicalTypeKind::kUInt64),
                           .nullable = false,
                           .event_time = false,
                           .schema_ordinal = std::nullopt,
                           .ordering_ordinal = std::nullopt},
      CsegColumnDescriptor{.column_id = std::nullopt,
                           .storage_kind = StorageKind::kRowOrdinal,
                           .logical_type = type(LogicalTypeKind::kUInt32),
                           .nullable = false,
                           .event_time = false,
                           .schema_ordinal = std::nullopt,
                           .ordering_ordinal = std::nullopt},
      CsegColumnDescriptor{.column_id = std::nullopt,
                           .storage_kind = StorageKind::kOperation,
                           .logical_type = type(LogicalTypeKind::kUInt8),
                           .nullable = false,
                           .event_time = false,
                           .schema_ordinal = std::nullopt,
                           .ordering_ordinal = std::nullopt},
  };
  const std::array<CsegGranuleDescriptor, 1U> granules{CsegGranuleDescriptor{
      .first_row = 0U,
      .row_count = 1U,
      .first_page_index = 0U,
      .minimum_event_time = 1,
      .maximum_event_time = 1,
  }};
  const auto page = [](const std::uint64_t length) {
    return CsegPageMetadataInput{.compression = PageCompression::kNone,
                                 .row_count = 1U,
                                 .null_count = 0U,
                                 .stored_length = length,
                                 .uncompressed_length = length,
                                 .validity_length = 0U,
                                 .offsets_length = 0U,
                                 .values_length = length,
                                 .page_crc32c = 0U};
  };
  const std::array<CsegPageMetadataInput, 5U> pages{page(8U), page(16U), page(8U), page(4U),
                                                    page(1U)};
  const auto encoded =
      encode_cseg_v1_metadata({.part_id = id<PartId>(1U),
                               .table_id = id<chronos::schema::TableId>(2U),
                               .tablet_id = id<chronos::schema::TabletId>(3U),
                               .schema_id = id<chronos::schema::SchemaId>(4U),
                               .schema_version = chronos::schema::SchemaVersion::initial(),
                               .row_count = 1U,
                               .event_time_column_ordinal = 0U,
                               .ordering_column_count = 1U,
                               .minimum_event_time = 1,
                               .maximum_event_time = 1,
                               .columns = columns,
                               .granules = granules,
                               .pages = pages});
  if (!encoded.has_value()) {
    std::abort();
  }
  return {encoded->bytes().begin(), encoded->bytes().end()};
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes =
      chronos::common::byte_view(std::span<const std::uint8_t>{data, size});
  exercise(bytes);
  if (size != 0U) {
    std::vector<std::byte> structured = structured_metadata();
    const std::size_t offset = static_cast<std::size_t>(data[0]) % structured.size();
    structured[offset] ^= static_cast<std::byte>(size > 1U ? (data[1] | 1U) : 1U);
    exercise(structured);
  }
  return 0;
}
