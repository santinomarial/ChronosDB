#include "chronos/wal/application.hpp"

#include "chronos/wal/types.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

namespace chronos::wal {
namespace {

[[nodiscard]] common::Status invalid_argument(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

void store_u32_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint32_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

void store_u64_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint64_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

[[nodiscard]] std::uint32_t load_u32_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  std::uint32_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  std::uint64_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint64_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

} // namespace

EncodedApplicationPayload::EncodedApplicationPayload(std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedApplicationPayload::bytes() const noexcept {
  return bytes_;
}

std::size_t EncodedApplicationPayload::size() const noexcept {
  return bytes_.size();
}

common::Result<EncodedApplicationPayload>
encode_application_payload(const ApplicationEnvelopeInput& input) {
  if (input.application_format == 0U || input.application_kind == 0U) {
    return common::make_unexpected(
        invalid_argument("WAL application format and kind must be nonzero"));
  }
  if (input.application_body.size() > kMaximumPayloadLength - kApplicationEnvelopeSize) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kOutOfRange, "WAL application body exceeds the v1 payload limit"});
  }

  std::vector<std::byte> bytes(kApplicationEnvelopeSize + input.application_body.size(),
                               std::byte{0});
  const common::MutableByteView output{bytes};
  store_u32_le(output, kApplicationFormatOffset, input.application_format);
  store_u32_le(output, kApplicationKindOffset, input.application_kind);
  store_u64_le(output, kApplicationFlagsOffset, input.application_flags);
  if (!input.application_body.empty()) {
    std::memcpy(bytes.data() + kApplicationBodyOffset, input.application_body.data(),
                input.application_body.size());
  }
  return EncodedApplicationPayload{std::move(bytes)};
}

common::Result<DecodedApplicationEnvelope>
decode_application_payload(const common::ByteView encoded_bytes) {
  if (encoded_bytes.size() < kApplicationEnvelopeSize) {
    return common::make_unexpected(
        corruption("WAL application payload requires a complete 16-byte envelope"));
  }
  if (encoded_bytes.size() > kMaximumPayloadLength) {
    return common::make_unexpected(corruption("WAL application payload exceeds the v1 limit"));
  }
  const std::uint32_t application_format = load_u32_le(encoded_bytes, kApplicationFormatOffset);
  const std::uint32_t application_kind = load_u32_le(encoded_bytes, kApplicationKindOffset);
  if (application_format == 0U || application_kind == 0U) {
    return common::make_unexpected(corruption("WAL application format or kind zero is invalid"));
  }
  return DecodedApplicationEnvelope{
      .application_format = application_format,
      .application_kind = application_kind,
      .application_flags = load_u64_le(encoded_bytes, kApplicationFlagsOffset),
      .application_body = encoded_bytes.subspan(kApplicationBodyOffset)};
}

} // namespace chronos::wal
