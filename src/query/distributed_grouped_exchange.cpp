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
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <set>
#include <utility>
#include <variant>
#include <vector>

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

struct CanonicalGroupedFloat64Key {
  bool present{};
  std::uint64_t bits{};

  [[nodiscard]] bool operator<(const CanonicalGroupedFloat64Key& other) const noexcept {
    return present != other.present ? !present : bits < other.bits;
  }
};

using GroupedStreamMessage =
    std::variant<GroupedFloat64ExchangeMessage, GroupedExchangeTerminalMessage>;

[[nodiscard]] CanonicalGroupedFloat64Key
canonical_group_key(const std::optional<double>& key) noexcept {
  return {.present = key.has_value(),
          .bits = key.has_value() ? canonical_group_key_bits(*key) : 0U};
}

[[nodiscard]] GroupedFloat64ExchangeMessage
canonicalize_message(GroupedFloat64ExchangeMessage message) noexcept {
  if (message.group_key.has_value())
    message.group_key = std::bit_cast<double>(canonical_group_key_bits(*message.group_key));
  return message;
}

[[nodiscard]] bool same_float_bits(const double left, const double right) noexcept {
  return std::bit_cast<std::uint64_t>(left) == std::bit_cast<std::uint64_t>(right);
}

[[nodiscard]] bool same_optional_float_bits(const std::optional<double>& left,
                                            const std::optional<double>& right) noexcept {
  return left.has_value() == right.has_value() &&
         (!left.has_value() || same_float_bits(*left, *right));
}

[[nodiscard]] bool same_partial(const MergeableAggregateState& left,
                                const MergeableAggregateState& right) noexcept {
  return left.count == right.count && same_float_bits(left.sum, right.sum) &&
         same_optional_float_bits(left.minimum, right.minimum) &&
         same_optional_float_bits(left.maximum, right.maximum) &&
         same_float_bits(left.mean, right.mean) && same_float_bits(left.m2, right.m2);
}

[[nodiscard]] bool same_grouped_message(const GroupedFloat64ExchangeMessage& left,
                                        const GroupedFloat64ExchangeMessage& right) noexcept {
  return left.query_id == right.query_id && left.tablet_id == right.tablet_id &&
         left.sequence == right.sequence && left.terminal == right.terminal &&
         same_optional_float_bits(left.group_key, right.group_key) &&
         same_partial(left.partial, right.partial);
}

[[nodiscard]] bool same_stream_message(const GroupedStreamMessage& left,
                                       const GroupedStreamMessage& right) noexcept {
  if (left.index() != right.index())
    return false;
  if (const auto* left_grouped = std::get_if<GroupedFloat64ExchangeMessage>(&left)) {
    const auto* right_grouped = std::get_if<GroupedFloat64ExchangeMessage>(&right);
    return right_grouped != nullptr && same_grouped_message(*left_grouped, *right_grouped);
  }
  const auto* left_terminal = std::get_if<GroupedExchangeTerminalMessage>(&left);
  const auto* right_terminal = std::get_if<GroupedExchangeTerminalMessage>(&right);
  return left_terminal != nullptr && right_terminal != nullptr &&
         left_terminal->query_id == right_terminal->query_id &&
         left_terminal->tablet_id == right_terminal->tablet_id &&
         left_terminal->sequence == right_terminal->sequence;
}

[[nodiscard]] bool
valid_result_options(const DistributedGroupedFloat64ResultOptions& options) noexcept {
  return (options.order_key == DistributedGroupedFloat64ResultOrderKey::kGroupKey ||
          options.order_key == DistributedGroupedFloat64ResultOrderKey::kCount ||
          options.order_key == DistributedGroupedFloat64ResultOrderKey::kSum ||
          options.order_key == DistributedGroupedFloat64ResultOrderKey::kMinimum ||
          options.order_key == DistributedGroupedFloat64ResultOrderKey::kMaximum ||
          options.order_key == DistributedGroupedFloat64ResultOrderKey::kMean ||
          options.order_key == DistributedGroupedFloat64ResultOrderKey::kVariancePopulation) &&
         (options.direction == DistributedGroupedFloat64ResultDirection::kAscending ||
          options.direction == DistributedGroupedFloat64ResultDirection::kDescending) &&
         (options.null_placement == DistributedGroupedFloat64NullPlacement::kFirst ||
          options.null_placement == DistributedGroupedFloat64NullPlacement::kLast);
}

struct ResultComparison {
  int order{};
  bool compared_null{};
};

[[nodiscard]] int compare_nonnull_group_keys(const double left, const double right) noexcept {
  const bool left_nan = std::isnan(left);
  const bool right_nan = std::isnan(right);
  if (left_nan || right_nan)
    return left_nan == right_nan ? 0 : (left_nan ? 1 : -1);
  return left == right ? 0 : (left < right ? -1 : 1);
}

[[nodiscard]] ResultComparison
compare_optional_result(const std::optional<double>& left, const std::optional<double>& right,
                        const DistributedGroupedFloat64NullPlacement null_placement) noexcept {
  if (left.has_value() != right.has_value()) {
    const bool null_first = null_placement == DistributedGroupedFloat64NullPlacement::kFirst;
    return {.order = left.has_value() != null_first ? -1 : 1, .compared_null = true};
  }
  if (!left.has_value())
    return {.order = 0, .compared_null = true};
  return {.order = compare_nonnull_group_keys(*left, *right), .compared_null = false};
}

[[nodiscard]] ResultComparison
compare_result_key(const GroupedFloat64AggregateResult& left,
                   const GroupedFloat64AggregateResult& right,
                   const DistributedGroupedFloat64ResultOptions& options) noexcept {
  switch (options.order_key) {
  case DistributedGroupedFloat64ResultOrderKey::kGroupKey:
    return compare_optional_result(left.group_key, right.group_key, options.null_placement);
  case DistributedGroupedFloat64ResultOrderKey::kCount:
    return {.order = left.aggregate.count == right.aggregate.count
                         ? 0
                         : (left.aggregate.count < right.aggregate.count ? -1 : 1)};
  case DistributedGroupedFloat64ResultOrderKey::kSum:
    return {.order = compare_nonnull_group_keys(left.aggregate.sum, right.aggregate.sum)};
  case DistributedGroupedFloat64ResultOrderKey::kMinimum:
    return compare_optional_result(left.aggregate.minimum, right.aggregate.minimum,
                                   options.null_placement);
  case DistributedGroupedFloat64ResultOrderKey::kMaximum:
    return compare_optional_result(left.aggregate.maximum, right.aggregate.maximum,
                                   options.null_placement);
  case DistributedGroupedFloat64ResultOrderKey::kMean:
    return {.order = compare_nonnull_group_keys(left.aggregate.mean, right.aggregate.mean)};
  case DistributedGroupedFloat64ResultOrderKey::kVariancePopulation:
    return compare_optional_result(left.aggregate.variance_population(),
                                   right.aggregate.variance_population(), options.null_placement);
  }
  return {};
}

[[nodiscard]] bool result_precedes(const GroupedFloat64AggregateResult& left,
                                   const GroupedFloat64AggregateResult& right,
                                   const DistributedGroupedFloat64ResultOptions& options) noexcept {
  ResultComparison comparison = compare_result_key(left, right, options);
  if (!comparison.compared_null &&
      options.direction == DistributedGroupedFloat64ResultDirection::kDescending) {
    comparison.order = -comparison.order;
  }
  if (comparison.order != 0)
    return comparison.order < 0;
  const ResultComparison tie = compare_optional_result(
      left.group_key, right.group_key, DistributedGroupedFloat64NullPlacement::kFirst);
  return tie.order < 0;
}

[[nodiscard]] common::Status
validate_terminal_message(const GroupedExchangeTerminalMessage& message) {
  if (message.query_id.is_nil() || message.tablet_id.uuid().is_nil() || message.sequence == 0U)
    return invalid("grouped terminal identity or sequence is invalid");
  return common::Status::ok();
}

} // namespace

EncodedGroupedFloat64ExchangeMessage::EncodedGroupedFloat64ExchangeMessage(
    std::array<std::byte, grouped_float64_exchange_format::kFrameLength> bytes) noexcept
    : bytes_(bytes) {}

common::ByteView EncodedGroupedFloat64ExchangeMessage::bytes() const noexcept {
  return bytes_;
}

EncodedGroupedExchangeTerminalMessage::EncodedGroupedExchangeTerminalMessage(
    std::array<std::byte, grouped_exchange_terminal_format::kFrameLength> bytes) noexcept
    : bytes_(bytes) {}

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
  return EncodedGroupedFloat64ExchangeMessage{bytes};
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
  return EncodedGroupedExchangeTerminalMessage{bytes};
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

common::Result<GroupedExchangeTerminalFrameReadStep>
GroupedExchangeTerminalFrameReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  const std::size_t consumed =
      std::min(bytes.size(), grouped_exchange_terminal_format::kFrameLength - buffered_bytes_);
  std::ranges::copy(bytes.first(consumed),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(buffered_bytes_));
  buffered_bytes_ += consumed;
  if (buffered_bytes_ != grouped_exchange_terminal_format::kFrameLength)
    return GroupedExchangeTerminalFrameReadStep{.consumed_bytes = consumed};
  auto decoded = decode_grouped_exchange_terminal_message_exact(bytes_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  buffered_bytes_ = 0U;
  return GroupedExchangeTerminalFrameReadStep{.consumed_bytes = consumed, .message = *decoded};
}

std::size_t GroupedExchangeTerminalFrameReader::buffered_bytes() const noexcept {
  return buffered_bytes_;
}

bool GroupedExchangeTerminalFrameReader::failed() const noexcept {
  return failure_.has_value();
}

GroupedExchangeTerminalFrameWriteCursor::GroupedExchangeTerminalFrameWriteCursor(
    EncodedGroupedExchangeTerminalMessage encoded) noexcept
    : encoded_(encoded) {}

GroupedExchangeTerminalFrameWriteCursor::GroupedExchangeTerminalFrameWriteCursor(
    GroupedExchangeTerminalFrameWriteCursor&& other) noexcept
    : encoded_(other.encoded_),
      written_bytes_(
          std::exchange(other.written_bytes_, grouped_exchange_terminal_format::kFrameLength)) {}

GroupedExchangeTerminalFrameWriteCursor& GroupedExchangeTerminalFrameWriteCursor::operator=(
    GroupedExchangeTerminalFrameWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_ = other.encoded_;
    written_bytes_ =
        std::exchange(other.written_bytes_, grouped_exchange_terminal_format::kFrameLength);
  }
  return *this;
}

common::Result<GroupedExchangeTerminalFrameWriteCursor>
GroupedExchangeTerminalFrameWriteCursor::create(const GroupedExchangeTerminalMessage& message) {
  auto encoded = encode_grouped_exchange_terminal_message(message);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return GroupedExchangeTerminalFrameWriteCursor{*encoded};
}

common::ByteView GroupedExchangeTerminalFrameWriteCursor::pending_write() const noexcept {
  return encoded_.bytes().subspan(written_bytes_);
}

common::Status
GroupedExchangeTerminalFrameWriteCursor::consume_written(const std::size_t bytes) noexcept {
  if (bytes > grouped_exchange_terminal_format::kFrameLength - written_bytes_)
    return invalid("written byte count exceeds the grouped terminal frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t GroupedExchangeTerminalFrameWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool GroupedExchangeTerminalFrameWriteCursor::complete() const noexcept {
  return written_bytes_ == grouped_exchange_terminal_format::kFrameLength;
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
  return GroupedFloat64ExchangeFrameReadStep{.consumed_bytes = consumed, .message = *decoded};
}

std::size_t GroupedFloat64ExchangeFrameReader::buffered_bytes() const noexcept {
  return buffered_bytes_;
}

bool GroupedFloat64ExchangeFrameReader::failed() const noexcept {
  return failure_.has_value();
}

GroupedFloat64ExchangeFrameWriteCursor::GroupedFloat64ExchangeFrameWriteCursor(
    EncodedGroupedFloat64ExchangeMessage encoded) noexcept
    : encoded_(encoded) {}

GroupedFloat64ExchangeFrameWriteCursor::GroupedFloat64ExchangeFrameWriteCursor(
    GroupedFloat64ExchangeFrameWriteCursor&& other) noexcept
    : encoded_(other.encoded_),
      written_bytes_(
          std::exchange(other.written_bytes_, grouped_float64_exchange_format::kFrameLength)) {}

GroupedFloat64ExchangeFrameWriteCursor& GroupedFloat64ExchangeFrameWriteCursor::operator=(
    GroupedFloat64ExchangeFrameWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_ = other.encoded_;
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
  return GroupedFloat64ExchangeFrameWriteCursor{*encoded};
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

class DistributedGroupedFloat64Coordinator::Impl {
public:
  struct FragmentProgress {
    std::vector<GroupedStreamMessage> messages;
    std::map<CanonicalGroupedFloat64Key, MergeableAggregateState> groups;
    bool terminal{};
  };

  Impl(common::Uuid id, const std::vector<schema::TabletId>& tablets,
       const DistributedCoordinatorLimits configured,
       const DistributedGroupedFloat64ResultOptions configured_result)
      : query_id(id), limits(configured), result_options(configured_result) {
    for (const schema::TabletId& tablet_id : tablets)
      fragments.emplace(tablet_id, FragmentProgress{});
  }

  common::Uuid query_id;
  DistributedCoordinatorLimits limits;
  DistributedGroupedFloat64ResultOptions result_options;
  std::map<schema::TabletId, FragmentProgress> fragments;
  std::size_t retained_messages{};
  common::Status failure;
};

DistributedGroupedFloat64Coordinator::DistributedGroupedFloat64Coordinator(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

DistributedGroupedFloat64Coordinator::~DistributedGroupedFloat64Coordinator() = default;

DistributedGroupedFloat64Coordinator::DistributedGroupedFloat64Coordinator(
    DistributedGroupedFloat64Coordinator&&) noexcept = default;

DistributedGroupedFloat64Coordinator& DistributedGroupedFloat64Coordinator::operator=(
    DistributedGroupedFloat64Coordinator&&) noexcept = default;

common::Result<DistributedGroupedFloat64Coordinator> DistributedGroupedFloat64Coordinator::create(
    common::Uuid query_id,
    // Preserve the public ownership-transfer boundary.
    std::vector<schema::TabletId> tablets, // NOLINT(performance-unnecessary-value-param)
    const DistributedCoordinatorLimits limits,
    const DistributedGroupedFloat64ResultOptions result_options) {
  if (query_id.is_nil())
    return common::make_unexpected(invalid("grouped coordinator query identity is invalid"));
  if (limits.maximum_messages_per_fragment == 0U ||
      limits.maximum_messages_per_fragment > limits.maximum_total_messages ||
      limits.maximum_total_messages > kMaximumDistributedCoordinatorMessages ||
      limits.maximum_total_messages < tablets.size() || !valid_result_options(result_options)) {
    return common::make_unexpected(invalid("grouped coordinator limits are invalid"));
  }
  try {
    std::set<schema::TabletId> unique_tablets;
    for (const schema::TabletId& tablet_id : tablets) {
      if (tablet_id.uuid().is_nil() || !unique_tablets.insert(tablet_id).second)
        return common::make_unexpected(invalid("grouped coordinator tablet set is invalid"));
    }
    return DistributedGroupedFloat64Coordinator{
        std::make_unique<Impl>(query_id, tablets, limits, result_options)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "grouped coordinator allocation failed"});
  }
}

common::Status
DistributedGroupedFloat64Coordinator::accept(const GroupedFloat64ExchangeMessage& message) {
  if (!impl_->failure.is_ok())
    return impl_->failure;
  common::Status validation = validate_message(message);
  if (!validation.is_ok())
    return validation;
  GroupedFloat64ExchangeMessage canonical = canonicalize_message(message);
  auto fragment = impl_->fragments.find(canonical.tablet_id);
  if (canonical.query_id != impl_->query_id || fragment == impl_->fragments.end())
    return invalid("grouped fragment result does not belong to the coordinator");

  Impl::FragmentProgress& progress = fragment->second;
  const GroupedStreamMessage candidate{canonical};
  if (canonical.sequence <= progress.messages.size()) {
    return same_stream_message(candidate, progress.messages[canonical.sequence - 1U])
               ? common::Status::ok()
               : common::Status{common::StatusCode::kAlreadyExists,
                                "grouped fragment sequence conflicts with retained state"};
  }
  if (progress.terminal)
    return invalid("grouped fragment emitted after its terminal message");
  if (canonical.sequence != progress.messages.size() + 1U) {
    return common::Status{common::StatusCode::kUnavailable, "grouped fragment sequence has a gap"};
  }
  if (progress.messages.size() == impl_->limits.maximum_messages_per_fragment ||
      impl_->retained_messages == impl_->limits.maximum_total_messages) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "grouped coordinator message history is exhausted"};
  }

  const CanonicalGroupedFloat64Key key = canonical_group_key(canonical.group_key);
  auto existing = progress.groups.find(key);
  MergeableAggregateState merged = canonical.partial;
  if (existing != progress.groups.end()) {
    merged = existing->second;
    const common::Status merge_status = merged.merge(canonical.partial);
    if (!merge_status.is_ok())
      return merge_status;
  }
  try {
    progress.messages.emplace_back(canonical);
    if (existing == progress.groups.end()) {
      const auto [inserted, did_insert] = progress.groups.emplace(key, merged);
      static_cast<void>(inserted);
      if (!did_insert) {
        progress.messages.pop_back();
        return common::Status{common::StatusCode::kInternal,
                              "grouped coordinator key insertion raced serialized ownership"};
      }
    } else {
      existing->second = merged;
    }
  } catch (const std::bad_alloc&) {
    if (progress.messages.size() == canonical.sequence)
      progress.messages.pop_back();
    return common::Status{common::StatusCode::kResourceExhausted,
                          "grouped coordinator retention allocation failed"};
  }
  progress.terminal = canonical.terminal;
  ++impl_->retained_messages;
  return common::Status::ok();
}

common::Status DistributedGroupedFloat64Coordinator::accept_terminal(
    const GroupedExchangeTerminalMessage& message) {
  if (!impl_->failure.is_ok())
    return impl_->failure;
  common::Status validation = validate_terminal_message(message);
  if (!validation.is_ok())
    return validation;
  auto fragment = impl_->fragments.find(message.tablet_id);
  if (message.query_id != impl_->query_id || fragment == impl_->fragments.end())
    return invalid("grouped terminal does not belong to the coordinator");

  Impl::FragmentProgress& progress = fragment->second;
  const GroupedStreamMessage candidate{message};
  if (message.sequence <= progress.messages.size()) {
    return same_stream_message(candidate, progress.messages[message.sequence - 1U])
               ? common::Status::ok()
               : common::Status{common::StatusCode::kAlreadyExists,
                                "grouped terminal sequence conflicts with retained state"};
  }
  if (progress.terminal)
    return invalid("grouped terminal followed a terminal message");
  if (message.sequence != 1U || !progress.messages.empty())
    return invalid("grouped terminal-only frame may close only an empty sequence-one stream");
  if (progress.messages.size() == impl_->limits.maximum_messages_per_fragment ||
      impl_->retained_messages == impl_->limits.maximum_total_messages) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "grouped coordinator message history is exhausted"};
  }
  try {
    progress.messages.emplace_back(message);
  } catch (const std::bad_alloc&) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "grouped coordinator retention allocation failed"};
  }
  progress.terminal = true;
  ++impl_->retained_messages;
  return common::Status::ok();
}

common::Status
DistributedGroupedFloat64Coordinator::worker_failed(const schema::TabletId& tablet_id,
                                                    common::Status failure) {
  const auto fragment = impl_->fragments.find(tablet_id);
  if (fragment == impl_->fragments.end() || failure.is_ok())
    return invalid("grouped worker failure is invalid or belongs to another coordinator");
  if (fragment->second.terminal)
    return common::Status::ok();
  if (!impl_->failure.is_ok())
    return impl_->failure;
  impl_->failure = std::move(failure);
  return common::Status::ok();
}

common::Result<std::vector<GroupedFloat64AggregateResult>>
DistributedGroupedFloat64Coordinator::finish() const {
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  std::map<CanonicalGroupedFloat64Key, MergeableAggregateState> groups;
  try {
    for (const auto& [tablet_id, progress] : impl_->fragments) {
      static_cast<void>(tablet_id);
      if (!progress.terminal) {
        return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                      "grouped query has incomplete fragments"});
      }
      for (const auto& [key, partial] : progress.groups) {
        auto existing = groups.find(key);
        if (existing == groups.end()) {
          groups.emplace(key, partial);
          continue;
        }
        MergeableAggregateState merged = existing->second;
        const common::Status merge_status = merged.merge(partial);
        if (!merge_status.is_ok())
          return common::make_unexpected(merge_status);
        existing->second = merged;
      }
    }
    std::vector<GroupedFloat64AggregateResult> result;
    result.reserve(groups.size());
    for (const auto& [key, aggregate] : groups) {
      result.push_back({.group_key = key.present
                                         ? std::optional<double>{std::bit_cast<double>(key.bits)}
                                         : std::nullopt,
                        .aggregate = aggregate});
    }
    std::ranges::sort(result, [&](const GroupedFloat64AggregateResult& left,
                                  const GroupedFloat64AggregateResult& right) {
      return result_precedes(left, right, impl_->result_options);
    });
    const std::uint64_t limit = impl_->result_options.limit.value_or(result.size());
    if (limit < result.size())
      result.resize(static_cast<std::size_t>(limit));
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "grouped coordinator result allocation failed"});
  }
}

} // namespace chronos::query
