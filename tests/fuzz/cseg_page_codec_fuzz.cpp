#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/page_codec.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <span>
#include <vector>

namespace {

[[nodiscard]] chronos::schema::LogicalType type(const std::uint8_t selector) {
  using chronos::schema::LogicalType;
  using chronos::schema::LogicalTypeKind;
  const auto kind = static_cast<LogicalTypeKind>(1U + (selector % 18U));
  return kind == LogicalTypeKind::kDecimal ? LogicalType::decimal(18U, 4U).value()
                                           : LogicalType::create(kind).value();
}

[[nodiscard]] chronos::cseg::CsegColumnDescriptor
column(const chronos::schema::LogicalType logical_type, const bool nullable) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.back() = std::byte{1U};
  return {.column_id = chronos::schema::ColumnId::from_bytes(bytes).value(),
          .storage_kind = chronos::cseg::StorageKind::kUser,
          .logical_type = logical_type,
          .nullable = nullable,
          .event_time = false,
          .schema_ordinal = 0U,
          .ordering_ordinal = std::nullopt};
}

struct Selectors {
  std::uint8_t type;
  std::uint8_t row;
  std::uint8_t split;
  std::uint8_t flags;
};

void exercise_raw(const chronos::common::ByteView stored, const Selectors selectors) {
  const bool nullable = (selectors.flags & 1U) != 0U;
  const std::uint32_t rows = 1U + selectors.row % 64U;
  const std::uint32_t nulls = nullable ? selectors.flags % (rows + 1U) : 0U;
  const std::size_t validity =
      stored.empty() ? 0U : static_cast<std::size_t>(selectors.split) % (stored.size() + 1U);
  const std::size_t remainder = stored.size() - validity;
  const std::size_t offsets =
      remainder == 0U ? 0U : static_cast<std::size_t>(selectors.flags) % (remainder + 1U);
  const chronos::cseg::CsegPageDescriptor page{
      .granule_ordinal = 0U,
      .stored_column_ordinal = 0U,
      .compression = chronos::cseg::PageCompression::kNone,
      .row_count = rows,
      .null_count = nulls,
      .page_offset = 0U,
      .stored_length = stored.size(),
      .uncompressed_length = stored.size(),
      .validity_length = validity,
      .offsets_length = offsets,
      .values_length = remainder - offsets,
      .page_crc32c = (selectors.flags & 2U) != 0U ? chronos::common::crc32c(stored) : 0U,
  };
  const auto decoded =
      chronos::cseg::decode_cseg_v1_page(stored, column(type(selectors.type), nullable), page);
  if (decoded.has_value()) {
    if (decoded->owns_uncompressed_bytes() ||
        decoded->uncompressed_bytes().data() != stored.data() ||
        decoded->physical().buffer_bytes() != stored.size()) {
      std::abort();
    }
  }
}

struct StructuredZstd {
  std::vector<std::byte> bytes;
  chronos::cseg::CsegColumnDescriptor column;
  chronos::cseg::CsegPageDescriptor page;
};

[[nodiscard]] StructuredZstd structured_zstd() {
  using namespace chronos;
  std::vector<std::byte> offsets{
      std::byte{0U}, std::byte{0U},  std::byte{0U}, std::byte{0U},
      std::byte{0U}, std::byte{16U}, std::byte{0U}, std::byte{0U},
  };
  std::vector<std::byte> values(4'096U, std::byte{'x'});
  const schema::LogicalType logical_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto physical = columnar::PhysicalColumnView::create(
      {.type = logical_type, .nullable = false, .row_count = 1U, .null_count = 0U},
      {.validity = {}, .offsets = offsets, .values = values});
  if (!physical.has_value()) {
    std::abort();
  }
  auto encoded = cseg::encode_cseg_v1_page(*physical, cseg::PageCompression::kZstd);
  if (!encoded.has_value() || encoded->compression() != cseg::PageCompression::kZstd) {
    std::abort();
  }
  const cseg::CsegPageMetadataInput metadata = encoded->metadata();
  return {.bytes = {encoded->bytes().begin(), encoded->bytes().end()},
          .column = column(logical_type, false),
          .page = {.granule_ordinal = 0U,
                   .stored_column_ordinal = 0U,
                   .compression = metadata.compression,
                   .row_count = metadata.row_count,
                   .null_count = metadata.null_count,
                   .page_offset = 0U,
                   .stored_length = metadata.stored_length,
                   .uncompressed_length = metadata.uncompressed_length,
                   .validity_length = metadata.validity_length,
                   .offsets_length = metadata.offsets_length,
                   .values_length = metadata.values_length,
                   .page_crc32c = metadata.page_crc32c}};
}

void exercise_structured(const std::uint8_t* data, const std::size_t size) {
  // The immutable provider fixture is built once; mutations below operate on their own byte copy.
  // NOLINTNEXTLINE(bugprone-throwing-static-initialization)
  static const StructuredZstd canonical = structured_zstd();
  std::vector<std::byte> bytes = canonical.bytes;
  chronos::cseg::CsegPageDescriptor page = canonical.page;
  if (size != 0U) {
    const std::size_t offset = static_cast<std::size_t>(data[0]) % bytes.size();
    bytes[offset] ^= static_cast<std::byte>(size > 1U ? (data[1] | 1U) : 1U);
    if (size > 2U && (data[2] & 1U) != 0U) {
      page.page_crc32c = chronos::common::crc32c(bytes);
    }
  }
  const auto decoded = chronos::cseg::decode_cseg_v1_page(bytes, canonical.column, page);
  if (decoded.has_value() &&
      (!decoded->owns_uncompressed_bytes() || decoded->physical().values().size() != 4'096U)) {
    std::abort();
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes =
      chronos::common::byte_view(std::span<const std::uint8_t>{data, size});
  const Selectors selectors{.type = size > 0U ? data[0] : std::uint8_t{0U},
                            .row = size > 1U ? data[1] : std::uint8_t{0U},
                            .split = size > 2U ? data[2] : std::uint8_t{0U},
                            .flags = size > 3U ? data[3] : std::uint8_t{0U}};
  exercise_raw(bytes, selectors);
  exercise_structured(data, size);
  return 0;
}
