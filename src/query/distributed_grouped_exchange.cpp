#include "chronos/query/distributed_grouped_exchange.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ranges>
#include <utility>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'X'}, std::byte{'G'}, std::byte{'R'},
                                                  std::byte{'P'}, std::byte{'1'}};
inline constexpr std::array<std::byte, 8U> kTerminalMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'X'},
    std::byte{'G'}, std::byte{'R'}, std::byte{'T'}, std::byte{'1'}};
inline constexpr std::uint32_t kTerminalFlag = 1U << 0U;
inline constexpr std::uint32_t kKeyPresentFlag = 1U << 1U;
inline constexpr std::uint32_t kMinimumFlag = 1U << 2U;
inline constexpr std::uint32_t kMaximumFlag = 1U << 3U;
inline constexpr std::uint32_t kKnownFlags =
    kTerminalFlag | kKeyPresentFlag | kMinimumFlag | kMaximumFlag;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status validate_partial(const MergeableAggregateState& partial) {
  if (partial.minimum.has_value() != partial.maximum.has_value())
    return invalid("grouped exchange aggregate extrema presence differs");
  if (partial.count == 0U) {
    if (partial.minimum.has_value() || std::bit_cast<std::uint64_t>(partial.sum) != 0U ||
        std::bit_cast<std::uint64_t>(partial.mean) != 0U ||
        std::bit_cast<std::uint64_t>(partial.m2) != 0U) {
      return invalid("empty grouped exchange aggregate state is not canonical");
    }
  } else if (!partial.minimum.has_value()) {
    return invalid("nonempty grouped exchange aggregate state has no extrema");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_message(const GroupedFloat64ExchangeMessage& message) {
  if (message.query_id.is_nil() || message.tablet_id.uuid().is_nil() || message.sequence == 0U)
    return invalid("grouped exchange identity or sequence is invalid");
  return validate_partial(message.partial);
}

[[nodiscard]] std::uint64_t canonical_group_key_bits(const double value) noexcept {
  if (value == 0.0)
    return 0U;
  if (std::isnan(value))
    return grouped_float64_exchange_format::kCanonicalQuietNanBits;
  return std::bit_cast<std::uint64_t>(value);
}

} // namespace

EncodedGroupedFloat64ExchangeMessage::EncodedGroupedFloat64ExchangeMessage(
    std::array<std::byte, grouped_float64_exchange_format::kFrameLength> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedGroupedFloat64ExchangeMessage::bytes() const noexcept {
  return bytes_;
}

EncodedGroupedExchangeTerminalMessage::EncodedGroupedExchangeTerminalMessage(
    std::array<std::byte, grouped_exchange_terminal_format::kFrameLength> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedGroupedExchangeTerminalMessage::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedGroupedFloat64ExchangeMessage>
encode_grouped_float64_exchange_message(const GroupedFloat64ExchangeMessage& message) {
  const common::Status validation = validate_message(message);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  std::array<std::byte, grouped_float64_exchange_format::kFrameLength> bytes{};
  common::ByteWriter writer{bytes};
  common::Status status = writer.write_exact(kMagic);
  if (status.is_ok())
    status = writer.write_u16_le(grouped_float64_exchange_format::kMajor);
  if (status.is_ok())
    status = writer.write_u16_le(grouped_float64_exchange_format::kMinor);
  if (status.is_ok())
    status = writer.write_u32_le(grouped_float64_exchange_format::kFrameLength);
  if (status.is_ok())
    status = writer.write_exact(message.query_id.bytes());
  if (status.is_ok())
    status = writer.write_exact(message.tablet_id.bytes());
  if (status.is_ok())
    status = writer.write_u64_le(message.sequence);
  const std::uint64_t group_key_bits =
      message.group_key.has_value() ? canonical_group_key_bits(*message.group_key) : 0U;
  if (status.is_ok())
    status = writer.write_float64_le(std::bit_cast<double>(group_key_bits));
  if (status.is_ok())
    status = writer.write_u64_le(message.partial.count);
  if (status.is_ok())
    status = writer.write_float64_le(message.partial.sum);
  if (status.is_ok())
    status = writer.write_float64_le(message.partial.minimum.value_or(0.0));
  if (status.is_ok())
    status = writer.write_float64_le(message.partial.maximum.value_or(0.0));
  if (status.is_ok())
    status = writer.write_float64_le(message.partial.mean);
  if (status.is_ok())
    status = writer.write_float64_le(message.partial.m2);
  std::uint32_t flags = message.terminal ? kTerminalFlag : 0U;
  if (message.group_key.has_value())
    flags |= kKeyPresentFlag;
  if (message.partial.minimum.has_value())
    flags |= kMinimumFlag | kMaximumFlag;
  if (status.is_ok())
    status = writer.write_u32_le(flags);
  if (status.is_ok())
    status = writer.zero_fill(16U);
  if (!status.is_ok() || writer.offset() != bytes.size() - 4U) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "grouped exchange frame layout differs from its length"});
  }
  status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
  if (!status.is_ok() || !writer.full()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "grouped exchange checksum does not fit its layout"});
  }
  return EncodedGroupedFloat64ExchangeMessage{std::move(bytes)};
}

common::Result<GroupedFloat64ExchangeMessage>
decode_grouped_float64_exchange_message_exact(const common::ByteView bytes) {
  if (bytes.size() != grouped_float64_exchange_format::kFrameLength)
    return common::make_unexpected(corruption("grouped exchange frame length is not exact"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("grouped exchange frame magic is invalid"));
  common::ByteReader checksum_reader{bytes.last(4U)};
  const auto stored_crc = checksum_reader.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("grouped exchange frame checksum is invalid"));

  common::ByteReader reader{bytes};
  if (!reader.skip(kMagic.size()).is_ok())
    return common::make_unexpected(corruption("grouped exchange header is truncated"));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto length = reader.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !length.has_value())
    return common::make_unexpected(corruption("grouped exchange header is truncated"));
  if (*major != grouped_float64_exchange_format::kMajor ||
      *minor != grouped_float64_exchange_format::kMinor) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                  "grouped exchange version is unsupported"});
  }
  if (*length != grouped_float64_exchange_format::kFrameLength)
    return common::make_unexpected(corruption("grouped exchange encoded length is invalid"));
  const auto query_bytes = reader.read_exact(common::Uuid::kSize);
  const auto tablet_bytes = reader.read_exact(common::Uuid::kSize);
  const auto sequence = reader.read_u64_le();
  const auto group_key = reader.read_float64_le();
  const auto count = reader.read_u64_le();
  const auto sum = reader.read_float64_le();
  const auto minimum = reader.read_float64_le();
  const auto maximum = reader.read_float64_le();
  const auto mean = reader.read_float64_le();
  const auto m2 = reader.read_float64_le();
  const auto flags = reader.read_u32_le();
  const auto reserved = reader.read_exact(16U);
  if (!query_bytes.has_value() || !tablet_bytes.has_value() || !sequence.has_value() ||
      !group_key.has_value() || !count.has_value() || !sum.has_value() || !minimum.has_value() ||
      !maximum.has_value() || !mean.has_value() || !m2.has_value() || !flags.has_value() ||
      !reserved.has_value() || reader.remaining() != 4U) {
    return common::make_unexpected(corruption("grouped exchange payload is truncated"));
  }
  if ((*flags & ~kKnownFlags) != 0U || std::ranges::any_of(*reserved, [](const std::byte value) {
        return value != std::byte{0U};
      })) {
    return common::make_unexpected(corruption("grouped exchange flags or reserved bytes differ"));
  }
  if (((*flags & kMinimumFlag) != 0U) != ((*flags & kMaximumFlag) != 0U))
    return common::make_unexpected(corruption("grouped exchange extrema flags disagree"));
  const bool has_key = (*flags & kKeyPresentFlag) != 0U;
  const std::uint64_t group_key_bits = std::bit_cast<std::uint64_t>(*group_key);
  if ((!has_key && group_key_bits != 0U) ||
      (has_key && group_key_bits != canonical_group_key_bits(*group_key))) {
    return common::make_unexpected(corruption("grouped exchange key is not canonical"));
  }
  const bool has_extrema = (*flags & kMinimumFlag) != 0U;
  if (!has_extrema && (std::bit_cast<std::uint64_t>(*minimum) != 0U ||
                       std::bit_cast<std::uint64_t>(*maximum) != 0U)) {
    return common::make_unexpected(corruption("grouped exchange absent extrema are not canonical"));
  }
  common::Uuid::Bytes query_id_bytes{};
  common::Uuid::Bytes tablet_id_bytes{};
  std::ranges::copy(*query_bytes, query_id_bytes.begin());
  std::ranges::copy(*tablet_bytes, tablet_id_bytes.begin());
  auto tablet_id = schema::TabletId::from_bytes(tablet_id_bytes);
  if (!tablet_id.has_value())
    return common::make_unexpected(corruption("grouped exchange tablet identity is invalid"));
  GroupedFloat64ExchangeMessage message{
      .query_id = common::Uuid{query_id_bytes},
      .tablet_id = *tablet_id,
      .sequence = *sequence,
      .group_key = has_key ? std::optional<double>{*group_key} : std::nullopt,
      .partial = {.count = *count,
                  .sum = *sum,
                  .minimum = has_extrema ? std::optional<double>{*minimum} : std::nullopt,
                  .maximum = has_extrema ? std::optional<double>{*maximum} : std::nullopt,
                  .mean = *mean,
                  .m2 = *m2},
      .terminal = (*flags & kTerminalFlag) != 0U};
  if (!validate_message(message).is_ok())
    return common::make_unexpected(corruption("grouped exchange aggregate state is invalid"));
  return message;
}

common::Result<EncodedGroupedExchangeTerminalMessage>
encode_grouped_exchange_terminal_message(const GroupedExchangeTerminalMessage& message) {
  if (message.query_id.is_nil() || message.tablet_id.uuid().is_nil() || message.sequence == 0U)
    return common::make_unexpected(invalid("grouped terminal identity or sequence is invalid"));
  std::array<std::byte, grouped_exchange_terminal_format::kFrameLength> bytes{};
  common::ByteWriter writer{bytes};
  common::Status status = writer.write_exact(kTerminalMagic);
  if (status.is_ok())
    status = writer.write_u16_le(grouped_exchange_terminal_format::kMajor);
  if (status.is_ok())
    status = writer.write_u16_le(grouped_exchange_terminal_format::kMinor);
  if (status.is_ok())
    status = writer.write_u32_le(grouped_exchange_terminal_format::kFrameLength);
  if (status.is_ok())
    status = writer.write_exact(message.query_id.bytes());
  if (status.is_ok())
    status = writer.write_exact(message.tablet_id.bytes());
  if (status.is_ok())
    status = writer.write_u64_le(message.sequence);
  if (status.is_ok())
    status = writer.zero_fill(4U);
  if (!status.is_ok() || writer.offset() != bytes.size() - 4U) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "grouped terminal layout differs from its length"});
  }
  status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
  if (!status.is_ok() || !writer.full()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "grouped terminal checksum does not fit its layout"});
  }
  return EncodedGroupedExchangeTerminalMessage{std::move(bytes)};
}

common::Result<GroupedExchangeTerminalMessage>
decode_grouped_exchange_terminal_message_exact(const common::ByteView bytes) {
  if (bytes.size() != grouped_exchange_terminal_format::kFrameLength)
    return common::make_unexpected(corruption("grouped terminal frame length is not exact"));
  if (!std::ranges::equal(bytes.first(kTerminalMagic.size()), kTerminalMagic))
    return common::make_unexpected(corruption("grouped terminal frame magic is invalid"));
  common::ByteReader checksum_reader{bytes.last(4U)};
  const auto stored_crc = checksum_reader.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("grouped terminal frame checksum is invalid"));
  common::ByteReader reader{bytes};
  if (!reader.skip(kTerminalMagic.size()).is_ok())
    return common::make_unexpected(corruption("grouped terminal header is truncated"));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto length = reader.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !length.has_value())
    return common::make_unexpected(corruption("grouped terminal header is truncated"));
  if (*major != grouped_exchange_terminal_format::kMajor ||
      *minor != grouped_exchange_terminal_format::kMinor) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                  "grouped terminal version is unsupported"});
  }
  if (*length != grouped_exchange_terminal_format::kFrameLength)
    return common::make_unexpected(corruption("grouped terminal encoded length is invalid"));
  const auto query_bytes = reader.read_exact(common::Uuid::kSize);
  const auto tablet_bytes = reader.read_exact(common::Uuid::kSize);
  const auto sequence = reader.read_u64_le();
  const auto reserved = reader.read_exact(4U);
  if (!query_bytes.has_value() || !tablet_bytes.has_value() || !sequence.has_value() ||
      !reserved.has_value() || reader.remaining() != 4U) {
    return common::make_unexpected(corruption("grouped terminal payload is truncated"));
  }
  if (std::ranges::any_of(*reserved, [](const std::byte value) { return value != std::byte{0U}; }))
    return common::make_unexpected(corruption("grouped terminal reserved bytes differ"));
  common::Uuid::Bytes query_id_bytes{};
  common::Uuid::Bytes tablet_id_bytes{};
  std::ranges::copy(*query_bytes, query_id_bytes.begin());
  std::ranges::copy(*tablet_bytes, tablet_id_bytes.begin());
  auto tablet_id = schema::TabletId::from_bytes(tablet_id_bytes);
  if (!tablet_id.has_value() || common::Uuid{query_id_bytes}.is_nil() || *sequence == 0U)
    return common::make_unexpected(corruption("grouped terminal identity or sequence is invalid"));
  return GroupedExchangeTerminalMessage{
      .query_id = common::Uuid{query_id_bytes}, .tablet_id = *tablet_id, .sequence = *sequence};
}

common::Result<GroupedFloat64ExchangeFrameReadStep>
GroupedFloat64ExchangeFrameReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  const std::size_t consumed =
      std::min(bytes.size(), grouped_float64_exchange_format::kFrameLength - buffered_bytes_);
  std::ranges::copy(bytes.first(consumed),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(buffered_bytes_));
  buffered_bytes_ += consumed;
  if (buffered_bytes_ != grouped_float64_exchange_format::kFrameLength)
    return GroupedFloat64ExchangeFrameReadStep{.consumed_bytes = consumed};
  auto decoded = decode_grouped_float64_exchange_message_exact(bytes_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  buffered_bytes_ = 0U;
  return GroupedFloat64ExchangeFrameReadStep{.consumed_bytes = consumed,
                                             .message = std::move(*decoded)};
}

std::size_t GroupedFloat64ExchangeFrameReader::buffered_bytes() const noexcept {
  return buffered_bytes_;
}

bool GroupedFloat64ExchangeFrameReader::failed() const noexcept {
  return failure_.has_value();
}

GroupedFloat64ExchangeFrameWriteCursor::GroupedFloat64ExchangeFrameWriteCursor(
    EncodedGroupedFloat64ExchangeMessage encoded) noexcept
    : encoded_(std::move(encoded)) {}

GroupedFloat64ExchangeFrameWriteCursor::GroupedFloat64ExchangeFrameWriteCursor(
    GroupedFloat64ExchangeFrameWriteCursor&& other) noexcept
    : encoded_(std::move(other.encoded_)),
      written_bytes_(
          std::exchange(other.written_bytes_, grouped_float64_exchange_format::kFrameLength)) {}

GroupedFloat64ExchangeFrameWriteCursor& GroupedFloat64ExchangeFrameWriteCursor::operator=(
    GroupedFloat64ExchangeFrameWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_ = std::move(other.encoded_);
    written_bytes_ =
        std::exchange(other.written_bytes_, grouped_float64_exchange_format::kFrameLength);
  }
  return *this;
}

common::Result<GroupedFloat64ExchangeFrameWriteCursor>
GroupedFloat64ExchangeFrameWriteCursor::create(const GroupedFloat64ExchangeMessage& message) {
  auto encoded = encode_grouped_float64_exchange_message(message);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return GroupedFloat64ExchangeFrameWriteCursor{std::move(*encoded)};
}

common::ByteView GroupedFloat64ExchangeFrameWriteCursor::pending_write() const noexcept {
  return encoded_.bytes().subspan(written_bytes_);
}

common::Status
GroupedFloat64ExchangeFrameWriteCursor::consume_written(const std::size_t bytes) noexcept {
  if (bytes > grouped_float64_exchange_format::kFrameLength - written_bytes_)
    return invalid("written byte count exceeds the grouped exchange frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t GroupedFloat64ExchangeFrameWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool GroupedFloat64ExchangeFrameWriteCursor::complete() const noexcept {
  return written_bytes_ == grouped_float64_exchange_format::kFrameLength;
}

} // namespace chronos::query
