#include "chronos/common/bytes.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/plain_page.hpp"
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

struct FuzzSelectors {
  std::uint8_t type;
  std::uint8_t row;
  std::uint8_t split;
  std::uint8_t flags;
};

void exercise(const chronos::common::ByteView payload, const FuzzSelectors selectors) {
  const bool nullable = (selectors.flags & 1U) != 0U;
  const std::uint32_t row_count = 1U + (selectors.row % 64U);
  const std::uint32_t null_count = nullable ? selectors.flags % (row_count + 1U) : 0U;
  const std::size_t validity_length =
      payload.empty() ? 0U : static_cast<std::size_t>(selectors.split) % (payload.size() + 1U);
  const std::size_t remaining = payload.size() - validity_length;
  const std::size_t offsets_length =
      remaining == 0U ? 0U : static_cast<std::size_t>(selectors.flags) % (remaining + 1U);
  const std::size_t values_length = remaining - offsets_length;
  const chronos::cseg::CsegPageDescriptor page{
      .granule_ordinal = 0U,
      .stored_column_ordinal = 0U,
      .compression = chronos::cseg::PageCompression::kNone,
      .row_count = row_count,
      .null_count = null_count,
      .page_offset = 0U,
      .stored_length = payload.size(),
      .uncompressed_length = payload.size(),
      .validity_length = validity_length,
      .offsets_length = offsets_length,
      .values_length = values_length,
      .page_crc32c = 0U,
  };
  const auto decoded = chronos::cseg::decode_cseg_v1_plain_page(
      payload, column(type(selectors.type), nullable), page);
  if (!decoded.has_value()) {
    return;
  }
  if (decoded->encoded_bytes().data() != payload.data() ||
      decoded->encoded_bytes().size() != payload.size() ||
      decoded->physical().row_count() != row_count ||
      decoded->physical().buffer_bytes() != payload.size()) {
    std::abort();
  }
  const auto encoded = chronos::cseg::encode_cseg_v1_plain_page(decoded->physical());
  if (!encoded.has_value() || !std::ranges::equal(encoded->bytes(), payload)) {
    std::abort();
  }
}

void exercise_structured(const std::uint8_t* data, const std::size_t size) {
  std::array<std::byte, 2U> payload{std::byte{0x05U}, std::byte{0x05U}};
  if (size != 0U) {
    payload[static_cast<std::size_t>(data[0]) % payload.size()] ^=
        static_cast<std::byte>(size > 1U ? (data[1] | 1U) : 1U);
  }
  const chronos::cseg::CsegPageDescriptor page{
      .granule_ordinal = 0U,
      .stored_column_ordinal = 0U,
      .compression = chronos::cseg::PageCompression::kNone,
      .row_count = 3U,
      .null_count = 1U,
      .page_offset = 0U,
      .stored_length = payload.size(),
      .uncompressed_length = payload.size(),
      .validity_length = 1U,
      .offsets_length = 0U,
      .values_length = 1U,
      .page_crc32c = 0U,
  };
  const auto decoded = chronos::cseg::decode_cseg_v1_plain_page(
      payload,
      column(type(static_cast<std::uint8_t>(chronos::schema::LogicalTypeKind::kBool) - 1U), true),
      page);
  if (decoded.has_value() && decoded->physical().null_count() != 1U) {
    std::abort();
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes =
      chronos::common::byte_view(std::span<const std::uint8_t>{data, size});
  const FuzzSelectors selectors{.type = size > 0U ? data[0] : std::uint8_t{0U},
                                .row = size > 1U ? data[1] : std::uint8_t{0U},
                                .split = size > 2U ? data[2] : std::uint8_t{0U},
                                .flags = size > 3U ? data[3] : std::uint8_t{0U}};
  exercise(bytes, selectors);
  exercise_structured(data, size);
  return 0;
}
