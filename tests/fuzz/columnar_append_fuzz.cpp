#include "chronos/columnar/columnar_batch_format.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/ingest/columnar_append.hpp"

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

void exercise(const chronos::common::ByteView bytes) {
  using chronos::ingest::ColumnarAppendDecodeErrorKind;
  const auto prefix = chronos::ingest::decode_columnar_append_v1_prefix(bytes);
  if (prefix.has_value()) {
    if (prefix->encoded_payload().data() != bytes.data() ||
        prefix->encoded_payload().size() > bytes.size() || prefix->row_count() == 0U ||
        prefix->batch().encoded_bytes().data() != bytes.data() + 176U) {
      std::abort();
    }
  } else if (prefix.error().kind() == ColumnarAppendDecodeErrorKind::kIncomplete &&
             (prefix.error().required_size() <= bytes.size() ||
              prefix.error().status().code() != chronos::common::StatusCode::kOutOfRange)) {
    std::abort();
  }

  const auto exact = chronos::ingest::decode_columnar_append_v1_exact(bytes);
  if (exact.has_value()) {
    if (!prefix.has_value() || exact->encoded_payload().size() != bytes.size()) {
      std::abort();
    }
  } else if (prefix.has_value() && prefix->encoded_payload().size() == bytes.size()) {
    std::abort();
  }
}

[[nodiscard]] std::array<std::byte, 192U> structured_batch(const std::uint8_t value) {
  using namespace chronos::columnar;
  std::array<std::byte, 192U> bytes{};
  const chronos::common::MutableByteView output{bytes};
  std::copy(format::kMagic.begin(), format::kMagic.end(), bytes.begin());
  store_u16_le(output, 8U, format::kFormatMajor);
  store_u16_le(output, 10U, format::kFormatMinor);
  store_u32_le(output, 12U, static_cast<std::uint32_t>(format::kBatchHeaderLength));
  store_u32_le(output, 20U, 1U);
  store_u32_le(output, 24U, 1U);
  store_u32_le(output, 28U, static_cast<std::uint32_t>(format::kColumnDescriptorLength));
  store_u64_le(output, 32U, bytes.size());
  bytes[55] = std::byte{1U};
  bytes[71] = std::byte{2U};
  store_u64_le(output, 72U, 1U);
  store_u64_le(output, 80U, format::kDescriptorsOffset);
  bytes[111] = std::byte{3U};
  store_u16_le(output, 112U, 1U);
  store_u16_le(output, 114U, format::kPlainPhysicalEncoding);
  store_u64_le(output, 160U, 176U);
  store_u64_le(output, 168U, 1U);
  bytes[176] = static_cast<std::byte>(value & 1U);
  store_u32_le(output, format::kHeaderCrc32cOffset,
               chronos::common::crc32c(chronos::common::ByteView{bytes}.first(
                   format::kHeaderCrc32cOffset)));
  store_u32_le(output, bytes.size() - format::kBatchTrailerLength,
               chronos::common::crc32c(chronos::common::ByteView{bytes}.first(
                   bytes.size() - format::kBatchTrailerLength)));
  return bytes;
}

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t value) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(value);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] std::array<std::byte, 368U> structured_command(const std::uint8_t value) {
  using namespace chronos::ingest;
  using namespace chronos::ingest::columnar_append_v1;
  std::array<std::byte, 368U> bytes{};
  const chronos::common::MutableByteView output{bytes};
  store_u32_le(output, chronos::wal::kApplicationFormatOffset, kApplicationFormat);
  store_u32_le(output, chronos::wal::kApplicationKindOffset, kApplicationKind);
  const std::size_t command = chronos::wal::kApplicationEnvelopeSize;
  store_u32_le(output, command + kCommandHeaderLengthOffset,
               static_cast<std::uint32_t>(kCommandHeaderLength));
  store_u32_le(output, command + kMutationKindOffset, kMutationKindAppendRows);
  store_u32_le(output, command + kDigestAlgorithmOffset, kDigestAlgorithmSha256);
  bytes[command + kClientIdOffset + 15U] = std::byte{4U};
  bytes[command + kClientBatchIdOffset + 15U] = std::byte{5U};
  bytes[command + kTableIdOffset + 15U] = std::byte{1U};
  bytes[command + kTabletIdOffset + 15U] = std::byte{6U};
  bytes[command + kSchemaIdOffset + 15U] = std::byte{2U};
  store_u64_le(output, command + kSchemaVersionOffset, 1U);
  store_u32_le(output, command + kRowCountOffset, 1U);
  store_u32_le(output, command + kBatchLengthOffset, 192U);
  store_u32_le(output, command + kOutcomeCodeOffset, kOutcomeCodeApplied);
  store_u32_le(output, command + kOutcomeRowCountOffset, 1U);
  const std::array<std::byte, 192U> batch = structured_batch(value);
  std::copy(batch.begin(), batch.end(), bytes.begin() + 176U);
  const auto digest = compute_columnar_append_v1_request_digest(
      {.table_id = id<chronos::schema::TableId>(1U),
       .tablet_id = id<chronos::schema::TabletId>(6U),
       .schema_id = id<chronos::schema::SchemaId>(2U),
       .schema_version = chronos::schema::SchemaVersion::initial(),
       .encoded_batch = batch});
  if (!digest.has_value()) {
    std::abort();
  }
  std::copy(digest->bytes().begin(), digest->bytes().end(),
            bytes.begin() + static_cast<std::ptrdiff_t>(command + kRequestDigestOffset));
  return bytes;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes =
      chronos::common::byte_view(std::span<const std::uint8_t>{data, size});
  exercise(bytes);
  if (size != 0U) {
    std::array<std::byte, 368U> structured = structured_command(data[0]);
    exercise(structured);
    if (size > 1U) {
      const std::size_t offset = static_cast<std::size_t>(data[0]) % structured.size();
      structured[offset] ^= static_cast<std::byte>(data[1] | 1U);
      exercise(structured);
    }
  }
  return 0;
}
