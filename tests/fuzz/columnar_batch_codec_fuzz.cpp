#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>

namespace {

void store_u16_le(const chronos::common::MutableByteView bytes, const std::size_t offset,
                  const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void store_u32_le(const chronos::common::MutableByteView bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

void store_u64_le(const chronos::common::MutableByteView bytes, const std::size_t offset,
                  const std::uint64_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

void refresh_header_crc(const chronos::common::MutableByteView bytes) {
  store_u32_le(bytes, chronos::columnar::format::kHeaderCrc32cOffset,
               chronos::common::crc32c(chronos::common::ByteView{bytes}.first(
                   chronos::columnar::format::kHeaderCrc32cOffset)));
}

void refresh_batch_crc(const chronos::common::MutableByteView bytes) {
  store_u32_le(bytes, bytes.size() - chronos::columnar::format::kBatchTrailerLength,
               chronos::common::crc32c(chronos::common::ByteView{bytes}.first(
                   bytes.size() - chronos::columnar::format::kBatchTrailerLength)));
}

void exercise(const chronos::common::ByteView bytes) {
  using chronos::columnar::ColumnarBatchDecodeErrorKind;

  const auto prefix = chronos::columnar::decode_columnar_batch_v1_prefix(bytes);
  if (prefix.has_value()) {
    if (prefix->encoded_bytes().data() != bytes.data() ||
        prefix->encoded_bytes().size() > bytes.size() || prefix->row_count() == 0U ||
        prefix->columns().empty() ||
        prefix->columns().size() > chronos::columnar::format::kMaximumColumnCount) {
      std::abort();
    }
    for (const chronos::columnar::ColumnVectorView& column : prefix->columns()) {
      if (column.row_count() != prefix->row_count()) {
        std::abort();
      }
    }
  } else if (prefix.error().kind() == ColumnarBatchDecodeErrorKind::kIncomplete) {
    if (prefix.error().required_size() <= bytes.size() ||
        prefix.error().status().code() != chronos::common::StatusCode::kOutOfRange) {
      std::abort();
    }
  }

  const auto exact = chronos::columnar::decode_columnar_batch_v1_exact(bytes);
  if (exact.has_value()) {
    if (!prefix.has_value() || exact->encoded_bytes().size() != bytes.size()) {
      std::abort();
    }
  } else if (prefix.has_value() && prefix->encoded_bytes().size() == bytes.size()) {
    std::abort();
  }
}

[[nodiscard]] std::array<std::byte, 192> structured_batch(const std::uint8_t value) {
  std::array<std::byte, 192> bytes{};
  const chronos::common::MutableByteView output{bytes};
  std::copy(chronos::columnar::format::kMagic.begin(), chronos::columnar::format::kMagic.end(),
            bytes.begin());
  store_u16_le(output, 8U, chronos::columnar::format::kFormatMajor);
  store_u16_le(output, 10U, chronos::columnar::format::kFormatMinor);
  store_u32_le(output, 12U,
               static_cast<std::uint32_t>(chronos::columnar::format::kBatchHeaderLength));
  store_u32_le(output, 20U, 1U);
  store_u32_le(output, 24U, 1U);
  store_u32_le(output, 28U,
               static_cast<std::uint32_t>(chronos::columnar::format::kColumnDescriptorLength));
  store_u64_le(output, 32U, bytes.size());
  bytes[55] = std::byte{1U};
  bytes[71] = std::byte{2U};
  store_u64_le(output, 72U, 1U);
  store_u64_le(output, 80U, chronos::columnar::format::kDescriptorsOffset);
  bytes[111] = std::byte{3U};
  store_u16_le(output, 112U, 1U);
  store_u16_le(output, 114U, chronos::columnar::format::kPlainPhysicalEncoding);
  store_u64_le(output, 160U, 176U);
  store_u64_le(output, 168U, 1U);
  bytes[176] = static_cast<std::byte>(value & 1U);
  refresh_header_crc(output);
  refresh_batch_crc(output);
  return bytes;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes =
      chronos::common::byte_view(std::span<const std::uint8_t>{data, size});
  exercise(bytes);

  if (size != 0U) {
    std::array<std::byte, 192> structured = structured_batch(data[0]);
    exercise(structured);
    if (size > 1U) {
      const std::size_t offset = static_cast<std::size_t>(data[0]) % structured.size();
      structured[offset] ^= static_cast<std::byte>(data[1] | 1U);
      const std::uint8_t checksum_mode = data[1] & 3U;
      if (checksum_mode >= 2U && offset < chronos::columnar::format::kHeaderCrc32cOffset) {
        refresh_header_crc(structured);
      }
      if (checksum_mode != 0U) {
        refresh_batch_crc(structured);
      }
      exercise(structured);
    }
  }
  return 0;
}
