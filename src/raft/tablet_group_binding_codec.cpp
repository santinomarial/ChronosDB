#include "chronos/raft/tablet_group_binding_codec.hpp"

#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <new>
#include <ranges>
#include <span>
#include <stdexcept>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                           std::byte{'N'}, std::byte{'T'}, std::byte{'G'},
                                           std::byte{'B'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kPayloadCrcOffset = 32U;
constexpr std::size_t kHeaderCrcOffset = 36U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

void store_u16(const std::span<std::byte> bytes, const std::size_t offset,
               const std::uint16_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void store_u32(const std::span<std::byte> bytes, const std::size_t offset,
               const std::uint32_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint16_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

[[nodiscard]] bool all_zero(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0U}; });
}

} // namespace

common::Result<std::vector<std::byte>>
encode_tablet_group_binding_v1(const TabletGroupBindingMetadata& binding) {
  if (binding.tablet_id.uuid().is_nil() || binding.group_id.is_nil())
    return common::make_unexpected(invalid("tablet group binding identity is nil"));
  try {
    std::vector<std::byte> bytes(kTabletGroupBindingSize, std::byte{0U});
    std::ranges::copy(kMagic, bytes.begin());
    store_u16(bytes, 8U, kMajor);
    store_u16(bytes, 10U, kMinor);
    store_u32(bytes, 12U, kTabletGroupBindingHeaderSize);
    store_u32(bytes, 16U, kTabletGroupBindingSize);
    store_u32(bytes, 20U, kTabletGroupBindingPayloadSize);
    std::ranges::copy(binding.tablet_id.uuid().bytes(),
                      bytes.begin() + kTabletGroupBindingHeaderSize);
    std::ranges::copy(binding.group_id.bytes(),
                      bytes.begin() + kTabletGroupBindingHeaderSize + common::Uuid::kSize);
    const common::ByteView payload{bytes.data() + kTabletGroupBindingHeaderSize,
                                   kTabletGroupBindingPayloadSize};
    store_u32(bytes, kPayloadCrcOffset, common::crc32c(payload));
    store_u32(bytes, kHeaderCrcOffset,
              common::crc32c(common::ByteView{bytes}.first(kTabletGroupBindingHeaderSize)));
    store_u32(bytes, bytes.size() - kTabletGroupBindingTrailerSize,
              common::crc32c(
                  common::ByteView{bytes}.first(bytes.size() - kTabletGroupBindingTrailerSize)));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("tablet group binding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("tablet group binding exceeds container limits"));
  }
}

common::Result<TabletGroupBindingMetadata>
decode_tablet_group_binding_v1(const common::ByteView bytes) {
  if (bytes.size() < kTabletGroupBindingHeaderSize)
    return common::make_unexpected(corruption("tablet group binding header is truncated"));
  std::array<std::byte, kTabletGroupBindingHeaderSize> header{};
  std::ranges::copy(bytes.first(header.size()), header.begin());
  const std::uint32_t header_crc = load_u32(bytes, kHeaderCrcOffset);
  store_u32(header, kHeaderCrcOffset, 0U);
  if (common::crc32c(header) != header_crc)
    return common::make_unexpected(corruption("tablet group binding header checksum mismatch"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic) || load_u16(bytes, 8U) != kMajor)
    return common::make_unexpected(unsupported("tablet group binding magic or major is unknown"));
  if (load_u16(bytes, 10U) != kMinor || load_u32(bytes, 12U) != kTabletGroupBindingHeaderSize) {
    return common::make_unexpected(unsupported("tablet group binding minor or header is unknown"));
  }
  if (bytes.size() != kTabletGroupBindingSize || load_u32(bytes, 16U) != kTabletGroupBindingSize ||
      load_u32(bytes, 20U) != kTabletGroupBindingPayloadSize || !all_zero(bytes.subspan(24U, 8U)) ||
      !all_zero(bytes.subspan(40U, 8U))) {
    return common::make_unexpected(corruption("tablet group binding framing is invalid"));
  }
  const common::ByteView payload =
      bytes.subspan(kTabletGroupBindingHeaderSize, kTabletGroupBindingPayloadSize);
  if (common::crc32c(payload) != load_u32(bytes, kPayloadCrcOffset) ||
      common::crc32c(bytes.first(bytes.size() - kTabletGroupBindingTrailerSize)) !=
          load_u32(bytes, bytes.size() - kTabletGroupBindingTrailerSize)) {
    return common::make_unexpected(corruption("tablet group binding checksum mismatch"));
  }
  common::Uuid::Bytes tablet_bytes{};
  common::Uuid::Bytes group_bytes{};
  std::ranges::copy(payload.first(common::Uuid::kSize), tablet_bytes.begin());
  std::ranges::copy(payload.subspan(common::Uuid::kSize), group_bytes.begin());
  auto tablet_id = schema::TabletId::from_bytes(tablet_bytes);
  const GroupId group_id{group_bytes};
  if (!tablet_id.has_value() || group_id.is_nil())
    return common::make_unexpected(corruption("tablet group binding identity is invalid"));
  return TabletGroupBindingMetadata{*tablet_id, group_id};
}

} // namespace chronos::raft
