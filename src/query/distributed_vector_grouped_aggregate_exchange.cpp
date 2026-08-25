#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'G'}, std::byte{'E'},
                                                  std::byte{'X'}, std::byte{'1'}};
inline constexpr std::uint32_t kTerminalFlag = 1U << 0U;
inline constexpr std::uint32_t kEmptyFlag = 1U << 1U;
inline constexpr std::uint32_t kKnownFlags = kTerminalFlag | kEmptyFlag;
inline constexpr std::uint16_t kNullableKeyFlag = 1U << 0U;
inline constexpr std::uint16_t kNullKeyFlag = 1U << 1U;
inline constexpr std::uint16_t kKnownKeyFlags = kNullableKeyFlag | kNullKeyFlag;
inline constexpr std::size_t kPayloadCrcOffset = 92U;
inline constexpr std::size_t kHeaderCrcOffset = 96U;
inline constexpr std::size_t kConservativeAllocationOverheadBytes = 64U;

struct DecodedHeader {
  DistributedVectorGroupedAggregateExchangePosition position;
  std::size_t frame_length;
  std::size_t key_section_length;
  std::size_t state_section_length;
  std::uint32_t payload_crc;
};

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Result<std::size_t> add_size(const std::size_t left, const std::size_t right,
                                                   const char* message) {
  const std::optional<std::size_t> result = common::checked_add(left, right);
  if (!result.has_value())
    return common::make_unexpected(exhausted(message));
  return *result;
}

[[nodiscard]] common::Status
validate_limits(const DistributedVectorGroupedAggregateExchangeDecodeLimits& limits) {
  if (limits.maximum_frame_length <
          distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength ||
      limits.maximum_frame_length >
          distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength ||
      limits.maximum_key_payload_bytes == 0U ||
      limits.maximum_key_payload_bytes >
          distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes ||
      limits.maximum_groups == 0U ||
      limits.maximum_groups >
          distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups ||
      limits.maximum_group_keys == 0U ||
      limits.maximum_group_keys >
          distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys ||
      limits.maximum_aggregates >
          distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates ||
      limits.state.maximum_frame_length <
          distributed_vector_aggregate_state_format::kMinimumFrameLength ||
      limits.state.maximum_frame_length >
          distributed_vector_aggregate_state_format::kMaximumFrameLength ||
      limits.state.maximum_variable_extremum_bytes == 0U ||
      limits.state.maximum_variable_extremum_bytes >
          distributed_vector_aggregate_state_format::kMaximumExtremumBytes) {
    return invalid("distributed grouped aggregate exchange limits are invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_message(const DistributedVectorGroupedAggregateExchangeMessage& message,
                 const std::span<const VectorGroupKeyDefinition> expected_keys,
                 const std::span<const VectorAggregateDefinition> expected_aggregates) {
  common::Status authority =
      validate_distributed_vector_grouped_aggregate_authority(expected_keys, expected_aggregates);
  if (!authority.is_ok())
    return authority;
  const auto& position = message.position();
  if (position.query_id.is_nil() || position.tablet_id.uuid().is_nil())
    return invalid("distributed grouped aggregate exchange identity is invalid");
  if (position.empty) {
    if (position.group_count != 0U || position.group_ordinal != 0U || position.sequence != 1U ||
        !position.terminal || !message.keys().empty() || !message.states().empty()) {
      return invalid("distributed grouped aggregate empty terminal is noncanonical");
    }
    return common::Status::ok();
  }
  if (position.group_count == 0U ||
      position.group_count > distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups ||
      position.group_ordinal >= position.group_count ||
      position.sequence != static_cast<std::uint64_t>(position.group_ordinal) + 1U ||
      position.terminal != (position.group_ordinal + 1U == position.group_count)) {
    return invalid("distributed grouped aggregate exchange position is noncanonical");
  }
  if (message.keys().size() != expected_keys.size() ||
      message.states().size() != expected_aggregates.size()) {
    return invalid("distributed grouped aggregate exchange width differs from its fragment");
  }
  std::size_t key_payload_bytes = 0U;
  for (std::size_t index = 0U; index < expected_keys.size(); ++index) {
    const ScalarValue& key = message.keys()[index];
    if (key.type() != std::optional<schema::LogicalType>{expected_keys[index].type} ||
        (key.is_null() && !expected_keys[index].nullable)) {
      return invalid("distributed grouped aggregate key differs from its fragment");
    }
    auto size = canonical_scalar_value_size(key);
    if (!size.has_value())
      return size.error();
    auto next =
        add_size(key_payload_bytes, *size, "distributed grouped aggregate key size overflowed");
    if (!next.has_value())
      return next.error();
    key_payload_bytes = *next;
  }
  if (key_payload_bytes >
      distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes) {
    return exhausted("distributed grouped aggregate key payload exceeds its limit");
  }
  for (std::size_t index = 0U; index < expected_aggregates.size(); ++index) {
    if (message.states()[index].definition() != expected_aggregates[index])
      return invalid("distributed grouped aggregate state differs from its fragment");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<DecodedHeader>
decode_header(const common::ByteView bytes,
              const std::span<const VectorGroupKeyDefinition> expected_keys,
              const std::span<const VectorAggregateDefinition> expected_aggregates,
              const DistributedVectorGroupedAggregateExchangeDecodeLimits& limits,
              const bool require_complete_frame) {
  using namespace distributed_vector_grouped_aggregate_exchange_format;
  if (bytes.size() < kHeaderLength)
    return common::make_unexpected(corruption("distributed grouped aggregate header is truncated"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("distributed grouped aggregate magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto header_crc = header_crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(bytes.first(kHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("distributed grouped aggregate header checksum is invalid"));
  }
  common::ByteReader reader{bytes.first(kHeaderLength)};
  static_cast<void>(reader.skip(kMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto query_id = reader.read_exact(16U);
  const auto tablet_id = reader.read_exact(16U);
  const auto sequence = reader.read_u64_le();
  const auto group_ordinal = reader.read_u32_le();
  const auto group_count = reader.read_u32_le();
  const auto key_count = reader.read_u32_le();
  const auto aggregate_count = reader.read_u32_le();
  const auto flags = reader.read_u32_le();
  const auto key_section_length = reader.read_u32_le();
  const auto state_section_length = reader.read_u32_le();
  const auto payload_crc = reader.read_u32_le();
  static_cast<void>(reader.skip(4U));
  const auto reserved = reader.read_exact(28U);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !query_id.has_value() || !tablet_id.has_value() ||
      !sequence.has_value() || !group_ordinal.has_value() || !group_count.has_value() ||
      !key_count.has_value() || !aggregate_count.has_value() || !flags.has_value() ||
      !key_section_length.has_value() || !state_section_length.has_value() ||
      !payload_crc.has_value() || !reserved.has_value()) {
    return common::make_unexpected(
        corruption("distributed grouped aggregate header fields are truncated"));
  }
  if (*major != kMajor || *minor != kMinor) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported,
                       "distributed grouped aggregate exchange version is unsupported"});
  }
  const std::uint64_t payload_length =
      static_cast<std::uint64_t>(*key_section_length) + *state_section_length;
  if (*header_length != kHeaderLength || *frame_length < kMinimumFrameLength ||
      *frame_length > kMaximumFrameLength ||
      *frame_length != kHeaderLength + payload_length + kTrailerLength ||
      (require_complete_frame && *frame_length != bytes.size()) || (*flags & ~kKnownFlags) != 0U ||
      !std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{}; })) {
    return common::make_unexpected(
        corruption("distributed grouped aggregate header is noncanonical"));
  }
  if (*frame_length > limits.maximum_frame_length)
    return common::make_unexpected(
        exhausted("distributed grouped aggregate frame exceeds its limit"));
  const bool terminal = (*flags & kTerminalFlag) != 0U;
  const bool empty = (*flags & kEmptyFlag) != 0U;
  if (empty) {
    if (*group_count != 0U || *group_ordinal != 0U || *sequence != 1U || !terminal ||
        *key_count != 0U || *aggregate_count != 0U || *key_section_length != 0U ||
        *state_section_length != 0U) {
      return common::make_unexpected(
          corruption("distributed grouped aggregate empty terminal is noncanonical"));
    }
  } else {
    if (*group_count == 0U || *group_count > limits.maximum_groups ||
        *group_ordinal >= *group_count ||
        *sequence != static_cast<std::uint64_t>(*group_ordinal) + 1U ||
        terminal != (*group_ordinal + 1U == *group_count) || *key_count != expected_keys.size() ||
        *aggregate_count != expected_aggregates.size()) {
      return common::make_unexpected(
          corruption("distributed grouped aggregate position is noncanonical"));
    }
    const auto key_headers =
        common::checked_multiply(static_cast<std::size_t>(*key_count), kKeyHeaderLength);
    if (!key_headers.has_value() || *key_section_length < *key_headers ||
        *key_section_length - *key_headers > limits.maximum_key_payload_bytes) {
      return common::make_unexpected(
          exhausted("distributed grouped aggregate key section exceeds its limit"));
    }
  }
  common::Uuid::Bytes query_bytes{};
  common::Uuid::Bytes tablet_bytes{};
  std::ranges::copy(*query_id, query_bytes.begin());
  std::ranges::copy(*tablet_id, tablet_bytes.begin());
  const common::Uuid query{query_bytes};
  const auto tablet = schema::TabletId::from_bytes(tablet_bytes);
  if (query.is_nil() || !tablet.has_value())
    return common::make_unexpected(corruption("distributed grouped aggregate identity is invalid"));
  return DecodedHeader{.position = {.query_id = query,
                                    .tablet_id = *tablet,
                                    .sequence = *sequence,
                                    .group_ordinal = *group_ordinal,
                                    .group_count = *group_count,
                                    .terminal = terminal,
                                    .empty = empty},
                       .frame_length = static_cast<std::size_t>(*frame_length),
                       .key_section_length = *key_section_length,
                       .state_section_length = *state_section_length,
                       .payload_crc = *payload_crc};
}

} // namespace

// The adjacent maxima correspond positionally to the two preceding spans.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
common::Status validate_distributed_vector_grouped_aggregate_authority(
    const std::span<const VectorGroupKeyDefinition> key_definitions,
    const std::span<const VectorAggregateDefinition> aggregate_definitions,
    const std::uint32_t maximum_group_keys, const std::uint32_t maximum_aggregates) {
  using namespace distributed_vector_grouped_aggregate_exchange_format;
  if (key_definitions.empty())
    return invalid("distributed grouped aggregate key definition list is empty");
  if (key_definitions.size() > kMaximumGroupKeys || key_definitions.size() > maximum_group_keys) {
    return exhausted("distributed grouped aggregate key definition count exceeds its limit");
  }
  if (aggregate_definitions.size() > kMaximumAggregates ||
      aggregate_definitions.size() > maximum_aggregates) {
    return exhausted("distributed grouped aggregate definition count exceeds its limit");
  }
  for (std::size_t index = 0U; index < key_definitions.size(); ++index) {
    if (key_definitions[index].column_ordinal >
        distributed_vector_aggregate_state_format::kMaximumInputColumnOrdinal) {
      return invalid("distributed grouped aggregate key ordinal exceeds the wire limit");
    }
    for (std::size_t prior = 0U; prior < index; ++prior) {
      if (key_definitions[prior].column_ordinal == key_definitions[index].column_ordinal)
        return invalid("distributed grouped aggregate key ordinal is duplicated");
    }
  }
  for (const VectorAggregateDefinition& definition : aggregate_definitions) {
    const auto shape = vector_aggregate_output_shape(definition);
    if (!shape.has_value())
      return invalid("distributed grouped aggregate definition is invalid");
    if (definition.input.has_value() &&
        definition.input->column_ordinal >
            distributed_vector_aggregate_state_format::kMaximumInputColumnOrdinal) {
      return invalid("distributed grouped aggregate input ordinal exceeds the wire limit");
    }
  }
  return common::Status::ok();
}
// NOLINTEND(bugprone-easily-swappable-parameters)

DistributedVectorGroupedAggregateExchangeMessage::DistributedVectorGroupedAggregateExchangeMessage(
    const DistributedVectorGroupedAggregateExchangePosition position, std::vector<ScalarValue> keys,
    std::vector<MergeableVectorAggregateState> states) noexcept
    : position_(position), keys_(std::move(keys)), states_(std::move(states)) {}

DistributedVectorGroupedAggregateExchangeMessage::DistributedVectorGroupedAggregateExchangeMessage(
    const DistributedVectorGroupedAggregateExchangePosition position, std::vector<ScalarValue> keys,
    std::vector<MergeableVectorAggregateState> states,
    QueryMemoryReservation key_reservation) noexcept
    : position_(position), keys_(std::move(keys)), states_(std::move(states)),
      key_reservation_(std::move(key_reservation)) {}

const DistributedVectorGroupedAggregateExchangePosition&
DistributedVectorGroupedAggregateExchangeMessage::position() const noexcept {
  return position_;
}

std::span<const ScalarValue>
DistributedVectorGroupedAggregateExchangeMessage::keys() const noexcept {
  return keys_;
}

std::span<const MergeableVectorAggregateState>
DistributedVectorGroupedAggregateExchangeMessage::states() const noexcept {
  return states_;
}

std::vector<MergeableVectorAggregateState>
DistributedVectorGroupedAggregateExchangeMessage::take_states() && noexcept {
  return std::move(states_);
}

EncodedDistributedVectorGroupedAggregateExchangeMessage::
    EncodedDistributedVectorGroupedAggregateExchangeMessage(std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedVectorGroupedAggregateExchangeMessage::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedVectorGroupedAggregateExchangeMessage>
encode_distributed_vector_grouped_aggregate_exchange_message(
    const DistributedVectorGroupedAggregateExchangeMessage& message,
    const std::span<const VectorGroupKeyDefinition> expected_keys,
    const std::span<const VectorAggregateDefinition> expected_aggregates) {
  using namespace distributed_vector_grouped_aggregate_exchange_format;
  const common::Status validation = validate_message(message, expected_keys, expected_aggregates);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  try {
    std::vector<std::size_t> key_lengths;
    std::vector<EncodedMergeableVectorAggregateState> encoded_states;
    std::size_t key_section_length = 0U;
    std::size_t state_section_length = 0U;
    if (!message.position().empty) {
      key_lengths.reserve(message.keys().size());
      for (const ScalarValue& key : message.keys()) {
        auto length = canonical_scalar_value_size(key);
        if (!length.has_value())
          return common::make_unexpected(length.error());
        key_lengths.push_back(*length);
        auto next = add_size(key_section_length, kKeyHeaderLength + *length,
                             "distributed grouped aggregate key section overflowed");
        if (!next.has_value())
          return common::make_unexpected(next.error());
        key_section_length = *next;
      }
      encoded_states.reserve(message.states().size());
      for (const MergeableVectorAggregateState& state : message.states()) {
        auto encoded = encode_mergeable_vector_aggregate_state(state);
        if (!encoded.has_value())
          return common::make_unexpected(encoded.error());
        auto next = add_size(state_section_length, kStateHeaderLength + encoded->bytes().size(),
                             "distributed grouped aggregate state section overflowed");
        if (!next.has_value())
          return common::make_unexpected(next.error());
        state_section_length = *next;
        encoded_states.push_back(std::move(*encoded));
      }
    }
    auto payload_length = add_size(key_section_length, state_section_length,
                                   "distributed grouped aggregate payload overflowed");
    if (!payload_length.has_value())
      return common::make_unexpected(payload_length.error());
    auto frame_length =
        add_size(kHeaderLength, *payload_length, "distributed grouped aggregate frame overflowed");
    if (!frame_length.has_value())
      return common::make_unexpected(frame_length.error());
    frame_length =
        add_size(*frame_length, kTrailerLength, "distributed grouped aggregate frame overflowed");
    if (!frame_length.has_value() || *frame_length > kMaximumFrameLength ||
        key_section_length > std::numeric_limits<std::uint32_t>::max() ||
        state_section_length > std::numeric_limits<std::uint32_t>::max()) {
      return common::make_unexpected(
          exhausted("distributed grouped aggregate frame exceeds its limit"));
    }

    std::vector<std::byte> bytes(*frame_length);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(*frame_length);
    if (status.is_ok())
      status = writer.write_exact(message.position().query_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(message.position().tablet_id.bytes());
    if (status.is_ok())
      status = writer.write_u64_le(message.position().sequence);
    if (status.is_ok())
      status = writer.write_u32_le(message.position().group_ordinal);
    if (status.is_ok())
      status = writer.write_u32_le(message.position().group_count);
    if (status.is_ok())
      status = writer.write_u32_le(
          message.position().empty ? 0U : static_cast<std::uint32_t>(expected_keys.size()));
    if (status.is_ok())
      status = writer.write_u32_le(
          message.position().empty ? 0U : static_cast<std::uint32_t>(expected_aggregates.size()));
    const std::uint32_t flags = (message.position().terminal ? kTerminalFlag : 0U) |
                                (message.position().empty ? kEmptyFlag : 0U);
    if (status.is_ok())
      status = writer.write_u32_le(flags);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(key_section_length));
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(state_section_length));
    if (status.is_ok())
      status = writer.zero_fill(8U);
    if (status.is_ok())
      status = writer.zero_fill(28U);
    for (std::size_t index = 0U; status.is_ok() && index < message.keys().size(); ++index) {
      const VectorGroupKeyDefinition& definition = expected_keys[index];
      const ScalarValue& key = message.keys()[index];
      status = writer.write_u16_le(definition.type.code());
      if (status.is_ok())
        status = writer.write_u16_le(definition.type.parameter_0());
      if (status.is_ok())
        status = writer.write_u16_le(definition.type.parameter_1());
      const std::uint16_t key_flags =
          (definition.nullable ? kNullableKeyFlag : 0U) | (key.is_null() ? kNullKeyFlag : 0U);
      if (status.is_ok())
        status = writer.write_u16_le(key_flags);
      if (status.is_ok())
        status = writer.write_u32_le(static_cast<std::uint32_t>(key_lengths[index]));
      const std::size_t payload_offset = writer.offset() + 4U;
      if (status.is_ok()) {
        std::vector<std::byte> canonical(key_lengths[index]);
        auto written = write_canonical_scalar_value(key, canonical);
        if (!written.has_value())
          return common::make_unexpected(written.error());
        status = writer.write_u32_le(common::crc32c(canonical));
        if (status.is_ok())
          status = writer.write_exact(canonical);
        if (status.is_ok() && writer.offset() != payload_offset + canonical.size())
          status = common::Status{common::StatusCode::kInternal,
                                  "distributed grouped aggregate key layout failed"};
      }
    }
    for (std::size_t index = 0U; status.is_ok() && index < encoded_states.size(); ++index) {
      const common::ByteView state = encoded_states[index].bytes();
      status = writer.write_u32_le(static_cast<std::uint32_t>(index));
      if (status.is_ok())
        status = writer.write_u32_le(static_cast<std::uint32_t>(state.size()));
      if (status.is_ok())
        status = writer.write_u32_le(common::crc32c(state));
      if (status.is_ok())
        status = writer.zero_fill(4U);
      if (status.is_ok())
        status = writer.write_exact(state);
    }
    if (status.is_ok())
      status = writer.zero_fill(kTrailerLength);
    if (!status.is_ok() || !writer.full()) {
      return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                    "distributed grouped aggregate layout failed"});
    }
    const common::ByteView payload =
        common::ByteView{bytes}.subspan(kHeaderLength, *payload_length);
    common::ByteWriter payload_crc_writer{
        common::MutableByteView{bytes}.subspan(kPayloadCrcOffset, 4U)};
    status = payload_crc_writer.write_u32_le(common::crc32c(payload));
    common::ByteWriter header_crc_writer{
        common::MutableByteView{bytes}.subspan(kHeaderCrcOffset, 4U)};
    if (status.is_ok())
      status = header_crc_writer.write_u32_le(
          common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    common::ByteWriter frame_crc_writer{common::MutableByteView{bytes}.last(kTrailerLength)};
    if (status.is_ok())
      status = frame_crc_writer.write_u32_le(
          common::crc32c(common::ByteView{bytes}.first(bytes.size() - kTrailerLength)));
    if (!status.is_ok())
      return common::make_unexpected(status);
    return EncodedDistributedVectorGroupedAggregateExchangeMessage{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed grouped aggregate allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed grouped aggregate exceeds container limits"));
  }
}

common::Result<DistributedVectorGroupedAggregateExchangeMessage>
decode_distributed_vector_grouped_aggregate_exchange_message_exact(
    const common::ByteView bytes, const std::span<const VectorGroupKeyDefinition> expected_keys,
    const std::span<const VectorAggregateDefinition> expected_aggregates,
    const QueryResourceContext& resources,
    const DistributedVectorGroupedAggregateExchangeDecodeLimits limits) {
  using namespace distributed_vector_grouped_aggregate_exchange_format;
  const common::Status limit_status = validate_limits(limits);
  if (!limit_status.is_ok())
    return common::make_unexpected(limit_status);
  const common::Status authority = validate_distributed_vector_grouped_aggregate_authority(
      expected_keys, expected_aggregates, limits.maximum_group_keys, limits.maximum_aggregates);
  if (!authority.is_ok())
    return common::make_unexpected(authority);
  if (bytes.size() < kMinimumFrameLength || bytes.size() > kMaximumFrameLength)
    return common::make_unexpected(corruption("distributed grouped aggregate length is invalid"));
  auto header = decode_header(bytes, expected_keys, expected_aggregates, limits, true);
  if (!header.has_value())
    return common::make_unexpected(header.error());
  common::ByteReader trailer{bytes.last(kTrailerLength)};
  const auto frame_crc = trailer.read_u32_le();
  if (!frame_crc.has_value() ||
      *frame_crc != common::crc32c(bytes.first(bytes.size() - kTrailerLength))) {
    return common::make_unexpected(
        corruption("distributed grouped aggregate frame checksum is invalid"));
  }
  const common::ByteView payload =
      bytes.subspan(kHeaderLength, header->key_section_length + header->state_section_length);
  if (header->payload_crc != common::crc32c(payload))
    return common::make_unexpected(
        corruption("distributed grouped aggregate payload checksum is invalid"));
  if (header->position.empty) {
    return DistributedVectorGroupedAggregateExchangeMessage{header->position, {}, {}, {}};
  }

  const auto key_slots = common::checked_multiply(expected_keys.size(), sizeof(ScalarValue) * 2U);
  const auto state_slots = common::checked_multiply(expected_aggregates.size(),
                                                    sizeof(MergeableVectorAggregateState) * 2U);
  if (!key_slots.has_value() || !state_slots.has_value()) {
    return common::make_unexpected(
        exhausted("distributed grouped aggregate retained state charge overflowed"));
  }
  auto key_charge = add_size(header->key_section_length, *key_slots,
                             "distributed grouped aggregate key charge overflowed");
  if (!key_charge.has_value())
    return common::make_unexpected(key_charge.error());
  key_charge =
      add_size(*key_charge, *state_slots, "distributed grouped aggregate state charge overflowed");
  if (!key_charge.has_value())
    return common::make_unexpected(key_charge.error());
  key_charge = add_size(*key_charge, kConservativeAllocationOverheadBytes * 2U,
                        "distributed grouped aggregate key charge overflowed");
  if (!key_charge.has_value())
    return common::make_unexpected(key_charge.error());
  auto reservation = resources.reserve(*key_charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());
  try {
    std::vector<ScalarValue> keys;
    std::vector<MergeableVectorAggregateState> states;
    keys.reserve(expected_keys.size());
    states.reserve(expected_aggregates.size());
    common::ByteReader key_reader{payload.first(header->key_section_length)};
    std::size_t key_payload_bytes = 0U;
    for (const VectorGroupKeyDefinition& expected : expected_keys) {
      const auto type_code = key_reader.read_u16_le();
      const auto parameter_0 = key_reader.read_u16_le();
      const auto parameter_1 = key_reader.read_u16_le();
      const auto flags = key_reader.read_u16_le();
      const auto length = key_reader.read_u32_le();
      const auto payload_crc = key_reader.read_u32_le();
      if (!type_code.has_value() || !parameter_0.has_value() || !parameter_1.has_value() ||
          !flags.has_value() || !length.has_value() || !payload_crc.has_value()) {
        return common::make_unexpected(
            corruption("distributed grouped aggregate key header is truncated"));
      }
      const bool nullable = (*flags & kNullableKeyFlag) != 0U;
      const bool is_null = (*flags & kNullKeyFlag) != 0U;
      if ((*flags & ~kKnownKeyFlags) != 0U || *type_code != expected.type.code() ||
          *parameter_0 != expected.type.parameter_0() ||
          *parameter_1 != expected.type.parameter_1() || nullable != expected.nullable ||
          (is_null && !nullable)) {
        return common::make_unexpected(
            corruption("distributed grouped aggregate key header is noncanonical"));
      }
      auto next_payload = add_size(key_payload_bytes, *length,
                                   "distributed grouped aggregate key payload overflowed");
      if (!next_payload.has_value() || *next_payload > limits.maximum_key_payload_bytes) {
        return common::make_unexpected(
            exhausted("distributed grouped aggregate key payload exceeds its limit"));
      }
      key_payload_bytes = *next_payload;
      const auto canonical = key_reader.read_exact(*length);
      if (!canonical.has_value() || *payload_crc != common::crc32c(*canonical)) {
        return common::make_unexpected(
            corruption("distributed grouped aggregate key payload is invalid"));
      }
      auto decoded = decode_canonical_scalar_value(expected.type, is_null, *canonical);
      if (!decoded.has_value()) {
        if (decoded.error().code() == common::StatusCode::kResourceExhausted)
          return common::make_unexpected(decoded.error());
        return common::make_unexpected(corruption("distributed grouped aggregate key is invalid"));
      }
      keys.push_back(std::move(*decoded));
    }
    if (!key_reader.empty())
      return common::make_unexpected(
          corruption("distributed grouped aggregate key section has trailing bytes"));

    common::ByteReader state_reader{
        payload.subspan(header->key_section_length, header->state_section_length)};
    for (std::size_t index = 0U; index < expected_aggregates.size(); ++index) {
      const auto ordinal = state_reader.read_u32_le();
      const auto length = state_reader.read_u32_le();
      const auto state_crc = state_reader.read_u32_le();
      const auto reserved = state_reader.read_u32_le();
      if (!ordinal.has_value() || !length.has_value() || !state_crc.has_value() ||
          !reserved.has_value()) {
        return common::make_unexpected(
            corruption("distributed grouped aggregate state header is truncated"));
      }
      if (*ordinal != index || *reserved != 0U ||
          *length < distributed_vector_aggregate_state_format::kMinimumFrameLength ||
          *length > limits.state.maximum_frame_length) {
        return common::make_unexpected(
            corruption("distributed grouped aggregate state header is noncanonical"));
      }
      const auto state_bytes = state_reader.read_exact(*length);
      if (!state_bytes.has_value() || *state_crc != common::crc32c(*state_bytes)) {
        return common::make_unexpected(
            corruption("distributed grouped aggregate nested state is invalid"));
      }
      auto state =
          decode_mergeable_vector_aggregate_state_exact(*state_bytes, resources, limits.state);
      if (!state.has_value())
        return common::make_unexpected(state.error());
      if (state->definition() != expected_aggregates[index]) {
        return common::make_unexpected(
            corruption("distributed grouped aggregate state differs from its fragment"));
      }
      states.push_back(std::move(*state));
    }
    if (!state_reader.empty())
      return common::make_unexpected(
          corruption("distributed grouped aggregate state section has trailing bytes"));
    return DistributedVectorGroupedAggregateExchangeMessage{
        header->position, std::move(keys), std::move(states), std::move(*reservation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("distributed grouped aggregate decode allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed grouped aggregate decode exceeds container limits"));
  }
}

DistributedVectorGroupedAggregateExchangeReader::DistributedVectorGroupedAggregateExchangeReader(
    std::vector<VectorGroupKeyDefinition>&& expected_keys,
    std::vector<VectorAggregateDefinition>&& expected_aggregates, QueryResourceContext resources,
    const DistributedVectorGroupedAggregateExchangeDecodeLimits limits) noexcept
    : expected_keys_(std::move(expected_keys)),
      expected_aggregates_(std::move(expected_aggregates)), resources_(std::move(resources)),
      limits_(limits) {}

common::Result<DistributedVectorGroupedAggregateExchangeReadStep>
DistributedVectorGroupedAggregateExchangeReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  const common::Status limit_status = validate_limits(limits_);
  if (!limit_status.is_ok()) {
    failure_ = limit_status;
    return common::make_unexpected(*failure_);
  }
  const common::Status authority = validate_distributed_vector_grouped_aggregate_authority(
      expected_keys_, expected_aggregates_, limits_.maximum_group_keys, limits_.maximum_aggregates);
  if (!authority.is_ok()) {
    failure_ = authority;
    return common::make_unexpected(*failure_);
  }
  std::size_t consumed{};
  if (frame_.empty()) {
    const std::size_t copied =
        std::min(bytes.size(), distributed_vector_grouped_aggregate_exchange_format::kHeaderLength -
                                   header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != distributed_vector_grouped_aggregate_exchange_format::kHeaderLength) {
      return DistributedVectorGroupedAggregateExchangeReadStep{.consumed_bytes = consumed,
                                                               .message = std::nullopt};
    }
    auto header = decode_header(header_, expected_keys_, expected_aggregates_, limits_, false);
    if (!header.has_value()) {
      failure_ = header.error();
      return common::make_unexpected(*failure_);
    }
    try {
      frame_.resize(header->frame_length);
    } catch (const std::bad_alloc&) {
      failure_ = exhausted("distributed grouped aggregate reader allocation failed");
      return common::make_unexpected(*failure_);
    } catch (const std::length_error&) {
      failure_ = exhausted("distributed grouped aggregate reader frame exceeds container limits");
      return common::make_unexpected(*failure_);
    }
    std::ranges::copy(header_, frame_.begin());
    frame_bytes_ = header_.size();
  }

  const common::ByteView remainder = bytes.subspan(consumed);
  const std::size_t copied = std::min(remainder.size(), frame_.size() - frame_bytes_);
  std::ranges::copy(remainder.first(copied),
                    frame_.begin() + static_cast<std::ptrdiff_t>(frame_bytes_));
  frame_bytes_ += copied;
  consumed += copied;
  if (frame_bytes_ != frame_.size()) {
    return DistributedVectorGroupedAggregateExchangeReadStep{.consumed_bytes = consumed,
                                                             .message = std::nullopt};
  }
  auto decoded = decode_distributed_vector_grouped_aggregate_exchange_message_exact(
      frame_, expected_keys_, expected_aggregates_, resources_, limits_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  frame_.clear();
  frame_bytes_ = 0U;
  header_bytes_ = 0U;
  return DistributedVectorGroupedAggregateExchangeReadStep{.consumed_bytes = consumed,
                                                           .message = std::move(*decoded)};
}

std::size_t DistributedVectorGroupedAggregateExchangeReader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool DistributedVectorGroupedAggregateExchangeReader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorGroupedAggregateExchangeWriteCursor::
    DistributedVectorGroupedAggregateExchangeWriteCursor(
        EncodedDistributedVectorGroupedAggregateExchangeMessage encoded) noexcept
    : encoded_(std::move(encoded)) {}

DistributedVectorGroupedAggregateExchangeWriteCursor::
    DistributedVectorGroupedAggregateExchangeWriteCursor(
        DistributedVectorGroupedAggregateExchangeWriteCursor&& other) noexcept
    : encoded_(std::move(other.encoded_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_.bytes().size();
}

DistributedVectorGroupedAggregateExchangeWriteCursor&
DistributedVectorGroupedAggregateExchangeWriteCursor::operator=(
    DistributedVectorGroupedAggregateExchangeWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_ = std::move(other.encoded_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_.bytes().size();
  }
  return *this;
}

common::Result<DistributedVectorGroupedAggregateExchangeWriteCursor>
DistributedVectorGroupedAggregateExchangeWriteCursor::create(
    const DistributedVectorGroupedAggregateExchangeMessage& message,
    const std::span<const VectorGroupKeyDefinition> expected_keys,
    const std::span<const VectorAggregateDefinition> expected_aggregates) {
  auto encoded = encode_distributed_vector_grouped_aggregate_exchange_message(
      message, expected_keys, expected_aggregates);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedVectorGroupedAggregateExchangeWriteCursor{std::move(*encoded)};
}

common::ByteView
DistributedVectorGroupedAggregateExchangeWriteCursor::pending_write() const noexcept {
  return encoded_.bytes().subspan(written_bytes_);
}

common::Status DistributedVectorGroupedAggregateExchangeWriteCursor::consume_written(
    const std::size_t bytes) noexcept {
  if (bytes > encoded_.bytes().size() - written_bytes_)
    return invalid("written byte count exceeds distributed grouped aggregate frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t DistributedVectorGroupedAggregateExchangeWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedVectorGroupedAggregateExchangeWriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_.bytes().size();
}

} // namespace chronos::query
