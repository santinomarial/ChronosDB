#include "chronos/query/distributed_vector_aggregate_exchange.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'A'}, std::byte{'E'},
                                                  std::byte{'X'}, std::byte{'1'}};
inline constexpr std::uint32_t kTerminalFlag = 1U;
inline constexpr std::size_t kHeaderCrcOffset = 84U;

struct DecodedHeader {
  common::Uuid query_id;
  schema::TabletId tablet_id;
  std::size_t frame_length;
  std::size_t state_length;
  std::uint64_t sequence;
  std::uint32_t aggregate_ordinal;
  bool terminal;
  std::uint32_t state_crc;
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

[[nodiscard]] common::Status
validate_limits(const DistributedVectorAggregateExchangeDecodeLimits& limits) {
  if (limits.maximum_frame_length <
          distributed_vector_aggregate_exchange_format::kMinimumFrameLength ||
      limits.maximum_frame_length >
          distributed_vector_aggregate_exchange_format::kMaximumFrameLength ||
      limits.maximum_aggregates == 0U ||
      limits.maximum_aggregates >
          distributed_vector_aggregate_exchange_format::kMaximumAggregates ||
      limits.state.maximum_frame_length <
          distributed_vector_aggregate_state_format::kMinimumFrameLength ||
      limits.state.maximum_frame_length >
          distributed_vector_aggregate_state_format::kMaximumFrameLength ||
      limits.state.maximum_variable_extremum_bytes == 0U ||
      limits.state.maximum_variable_extremum_bytes >
          distributed_vector_aggregate_state_format::kMaximumExtremumBytes) {
    return invalid("distributed vector aggregate exchange limits are invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_definitions(const std::span<const VectorAggregateDefinition> definitions,
                     const std::uint32_t maximum_aggregates =
                         distributed_vector_aggregate_exchange_format::kMaximumAggregates) {
  if (definitions.empty())
    return invalid("distributed vector aggregate definition list is empty");
  if (definitions.size() > distributed_vector_aggregate_exchange_format::kMaximumAggregates ||
      definitions.size() > maximum_aggregates) {
    return exhausted("distributed vector aggregate definition count exceeds its limit");
  }
  for (const VectorAggregateDefinition& definition : definitions) {
    const auto shape = vector_aggregate_output_shape(definition);
    if (!shape.has_value())
      return invalid("distributed vector aggregate definition is invalid");
    if (definition.input.has_value() &&
        definition.input->column_ordinal >
            distributed_vector_aggregate_state_format::kMaximumInputColumnOrdinal) {
      return invalid("distributed vector aggregate input ordinal exceeds the wire limit");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_message(const DistributedVectorAggregateExchangeMessage& message,
                 const std::span<const VectorAggregateDefinition> expected_definitions) {
  common::Status definitions_status = validate_definitions(expected_definitions);
  if (!definitions_status.is_ok())
    return definitions_status;
  if (message.query_id.is_nil() || message.tablet_id.uuid().is_nil())
    return invalid("distributed vector aggregate exchange identity is invalid");
  if (message.aggregate_ordinal >= expected_definitions.size())
    return invalid("distributed vector aggregate exchange ordinal is invalid");
  const std::uint64_t expected_sequence =
      static_cast<std::uint64_t>(message.aggregate_ordinal) + 1U;
  const bool expected_terminal = message.aggregate_ordinal + 1U == expected_definitions.size();
  if (message.sequence != expected_sequence || message.terminal != expected_terminal)
    return invalid("distributed vector aggregate exchange sequence is noncanonical");
  if (message.state.definition() != expected_definitions[message.aggregate_ordinal])
    return invalid("distributed vector aggregate exchange state differs from its fragment");
  return common::Status::ok();
}

[[nodiscard]] common::Result<DecodedHeader>
decode_header(const common::ByteView bytes,
              const std::span<const VectorAggregateDefinition> expected_definitions,
              const DistributedVectorAggregateExchangeDecodeLimits& limits,
              const bool require_complete_frame) {
  if (bytes.size() < distributed_vector_aggregate_exchange_format::kHeaderLength)
    return common::make_unexpected(corruption("distributed vector aggregate header is truncated"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("distributed vector aggregate magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto header_crc = header_crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(bytes.first(kHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("distributed vector aggregate header checksum is invalid"));
  }

  common::ByteReader reader{
      bytes.first(distributed_vector_aggregate_exchange_format::kHeaderLength)};
  static_cast<void>(reader.skip(kMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto query_id = reader.read_exact(16U);
  const auto tablet_id = reader.read_exact(16U);
  const auto sequence = reader.read_u64_le();
  const auto aggregate_ordinal = reader.read_u32_le();
  const auto aggregate_count = reader.read_u32_le();
  const auto flags = reader.read_u32_le();
  const auto state_length = reader.read_u32_le();
  const auto state_crc = reader.read_u32_le();
  static_cast<void>(reader.skip(4U));
  const auto reserved = reader.read_exact(8U);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !query_id.has_value() || !tablet_id.has_value() ||
      !sequence.has_value() || !aggregate_ordinal.has_value() || !aggregate_count.has_value() ||
      !flags.has_value() || !state_length.has_value() || !state_crc.has_value() ||
      !reserved.has_value()) {
    return common::make_unexpected(
        corruption("distributed vector aggregate header fields are truncated"));
  }
  if (*major != distributed_vector_aggregate_exchange_format::kMajor ||
      *minor != distributed_vector_aggregate_exchange_format::kMinor) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported,
                       "distributed vector aggregate exchange version is unsupported"});
  }
  if (*header_length != distributed_vector_aggregate_exchange_format::kHeaderLength ||
      *frame_length < distributed_vector_aggregate_exchange_format::kMinimumFrameLength ||
      *frame_length > distributed_vector_aggregate_exchange_format::kMaximumFrameLength ||
      *state_length < distributed_vector_aggregate_state_format::kMinimumFrameLength ||
      *state_length > distributed_vector_aggregate_state_format::kMaximumFrameLength ||
      *frame_length != distributed_vector_aggregate_exchange_format::kHeaderLength +
                           static_cast<std::uint64_t>(*state_length) +
                           distributed_vector_aggregate_exchange_format::kTrailerLength ||
      *aggregate_count != expected_definitions.size() || *aggregate_ordinal >= *aggregate_count ||
      (*flags & ~kTerminalFlag) != 0U ||
      !std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{}; })) {
    return common::make_unexpected(
        corruption("distributed vector aggregate header is noncanonical"));
  }
  if (*frame_length > limits.maximum_frame_length ||
      *state_length > limits.state.maximum_frame_length) {
    return common::make_unexpected(
        exhausted("distributed vector aggregate exchange exceeds configured limits"));
  }
  if (require_complete_frame && *frame_length != bytes.size())
    return common::make_unexpected(
        corruption("distributed vector aggregate exact length is invalid"));
  const std::uint64_t expected_sequence = static_cast<std::uint64_t>(*aggregate_ordinal) + 1U;
  const bool terminal = (*flags & kTerminalFlag) != 0U;
  const bool expected_terminal = *aggregate_ordinal + 1U == *aggregate_count;
  if (*sequence != expected_sequence || terminal != expected_terminal) {
    return common::make_unexpected(
        corruption("distributed vector aggregate sequence is noncanonical"));
  }

  common::Uuid::Bytes query_bytes{};
  common::Uuid::Bytes tablet_bytes{};
  std::ranges::copy(*query_id, query_bytes.begin());
  std::ranges::copy(*tablet_id, tablet_bytes.begin());
  const common::Uuid query{query_bytes};
  const auto tablet = schema::TabletId::from_bytes(tablet_bytes);
  if (query.is_nil() || !tablet.has_value())
    return common::make_unexpected(corruption("distributed vector aggregate identity is invalid"));
  return DecodedHeader{.query_id = query,
                       .tablet_id = *tablet,
                       .frame_length = static_cast<std::size_t>(*frame_length),
                       .state_length = *state_length,
                       .sequence = *sequence,
                       .aggregate_ordinal = *aggregate_ordinal,
                       .terminal = terminal,
                       .state_crc = *state_crc};
}

} // namespace

common::Result<std::vector<VectorAggregateDefinition>>
bind_distributed_vector_ungrouped_aggregate_definitions(
    const DistributedVectorPlanIntent& intent,
    const std::span<const PhysicalColumnShape> projected_inputs,
    const DistributedVectorResultSchema& result_schema) {
  if (projected_inputs.empty() ||
      projected_inputs.size() > distributed_vector_plan_format::kMaximumInputColumns) {
    return common::make_unexpected(
        invalid("distributed vector aggregate projected input width is invalid"));
  }
  const common::Status plan_status = validate_distributed_vector_plan_intent(
      intent, static_cast<std::uint32_t>(projected_inputs.size()),
      distributed_vector_plan_format::kMaximumOutputColumns);
  if (!plan_status.is_ok())
    return common::make_unexpected(plan_status);
  if (intent.mode != DistributedVectorPlanMode::kUngroupedAggregate)
    return common::make_unexpected(
        invalid("distributed vector aggregate exchange requires an ungrouped plan"));
  const common::Status schema_status =
      validate_distributed_vector_result_schema(intent, projected_inputs, result_schema);
  if (!schema_status.is_ok())
    return common::make_unexpected(schema_status);
  try {
    std::vector<VectorAggregateDefinition> definitions;
    definitions.reserve(intent.aggregates.size());
    for (const DistributedVectorAggregateIntent& aggregate : intent.aggregates) {
      std::optional<VectorAggregateInput> input;
      if (aggregate.input_index.has_value()) {
        const std::uint32_t index = *aggregate.input_index;
        const PhysicalColumnShape& shape = projected_inputs[index];
        input = VectorAggregateInput{
            .column_ordinal = index, .type = shape.type, .nullable = shape.nullable};
      }
      definitions.push_back({.operation = aggregate.operation, .input = input});
    }
    const common::Status definitions_status = validate_definitions(definitions);
    if (!definitions_status.is_ok())
      return common::make_unexpected(definitions_status);
    return definitions;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("distributed vector aggregate definition allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed vector aggregate definitions exceed container limits"));
  }
}

DistributedVectorAggregateExchangeMessage::DistributedVectorAggregateExchangeMessage(
    const DistributedVectorAggregateExchangePosition position,
    MergeableVectorAggregateState state_value) noexcept
    : query_id(position.query_id), tablet_id(position.tablet_id), sequence(position.sequence),
      aggregate_ordinal(position.aggregate_ordinal), terminal(position.terminal),
      state(std::move(state_value)) {}

EncodedDistributedVectorAggregateExchangeMessage::EncodedDistributedVectorAggregateExchangeMessage(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedVectorAggregateExchangeMessage::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedVectorAggregateExchangeMessage>
encode_distributed_vector_aggregate_exchange_message(
    const DistributedVectorAggregateExchangeMessage& message,
    const std::span<const VectorAggregateDefinition> expected_definitions) {
  const common::Status validation = validate_message(message, expected_definitions);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  auto encoded_state = encode_mergeable_vector_aggregate_state(message.state);
  if (!encoded_state.has_value())
    return common::make_unexpected(encoded_state.error());
  const common::ByteView state_bytes = encoded_state->bytes();
  const std::size_t frame_length = distributed_vector_aggregate_exchange_format::kHeaderLength +
                                   state_bytes.size() +
                                   distributed_vector_aggregate_exchange_format::kTrailerLength;
  try {
    std::vector<std::byte> bytes(frame_length);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_aggregate_exchange_format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_aggregate_exchange_format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(distributed_vector_aggregate_exchange_format::kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(frame_length);
    if (status.is_ok())
      status = writer.write_exact(message.query_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(message.tablet_id.bytes());
    if (status.is_ok())
      status = writer.write_u64_le(message.sequence);
    if (status.is_ok())
      status = writer.write_u32_le(message.aggregate_ordinal);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(expected_definitions.size()));
    if (status.is_ok())
      status = writer.write_u32_le(message.terminal ? kTerminalFlag : 0U);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(state_bytes.size()));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(state_bytes));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.zero_fill(8U);
    if (status.is_ok())
      status = writer.write_exact(state_bytes);
    if (status.is_ok())
      status =
          writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!status.is_ok() || !writer.full()) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "distributed vector aggregate exchange layout failed"});
    }
    return EncodedDistributedVectorAggregateExchangeMessage{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("distributed vector aggregate exchange allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed vector aggregate exchange exceeds container limits"));
  }
}

common::Result<DistributedVectorAggregateExchangeMessage>
decode_distributed_vector_aggregate_exchange_message_exact(
    const common::ByteView bytes,
    const std::span<const VectorAggregateDefinition> expected_definitions,
    const QueryResourceContext& resources,
    const DistributedVectorAggregateExchangeDecodeLimits limits) {
  const common::Status limit_status = validate_limits(limits);
  if (!limit_status.is_ok())
    return common::make_unexpected(limit_status);
  const common::Status definitions_status =
      validate_definitions(expected_definitions, limits.maximum_aggregates);
  if (!definitions_status.is_ok())
    return common::make_unexpected(definitions_status);
  if (bytes.size() < distributed_vector_aggregate_exchange_format::kMinimumFrameLength ||
      bytes.size() > distributed_vector_aggregate_exchange_format::kMaximumFrameLength) {
    return common::make_unexpected(
        corruption("distributed vector aggregate exchange length is invalid"));
  }
  auto header = decode_header(bytes, expected_definitions, limits, true);
  if (!header.has_value())
    return common::make_unexpected(header.error());
  common::ByteReader trailer{
      bytes.last(distributed_vector_aggregate_exchange_format::kTrailerLength)};
  const auto frame_crc = trailer.read_u32_le();
  if (!frame_crc.has_value() || *frame_crc != common::crc32c(bytes.first(bytes.size() - 4U))) {
    return common::make_unexpected(
        corruption("distributed vector aggregate exchange checksum is invalid"));
  }
  const common::ByteView state_bytes = bytes.subspan(
      distributed_vector_aggregate_exchange_format::kHeaderLength, header->state_length);
  if (header->state_crc != common::crc32c(state_bytes))
    return common::make_unexpected(
        corruption("distributed vector aggregate nested state checksum is invalid"));
  auto state = decode_mergeable_vector_aggregate_state_exact(state_bytes, resources, limits.state);
  if (!state.has_value())
    return common::make_unexpected(state.error());
  if (state->definition() != expected_definitions[header->aggregate_ordinal]) {
    return common::make_unexpected(
        corruption("distributed vector aggregate state differs from its fragment"));
  }
  return DistributedVectorAggregateExchangeMessage{{.query_id = header->query_id,
                                                    .tablet_id = header->tablet_id,
                                                    .sequence = header->sequence,
                                                    .aggregate_ordinal = header->aggregate_ordinal,
                                                    .terminal = header->terminal},
                                                   std::move(*state)};
}

DistributedVectorAggregateExchangeReader::DistributedVectorAggregateExchangeReader(
    std::vector<VectorAggregateDefinition>&& expected_definitions, QueryResourceContext resources,
    const DistributedVectorAggregateExchangeDecodeLimits limits) noexcept
    : expected_definitions_(std::move(expected_definitions)), resources_(std::move(resources)),
      limits_(limits) {}

common::Result<DistributedVectorAggregateExchangeReadStep>
DistributedVectorAggregateExchangeReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  const common::Status limit_status = validate_limits(limits_);
  if (!limit_status.is_ok()) {
    failure_ = limit_status;
    return common::make_unexpected(*failure_);
  }
  const common::Status definitions_status =
      validate_definitions(expected_definitions_, limits_.maximum_aggregates);
  if (!definitions_status.is_ok()) {
    failure_ = definitions_status;
    return common::make_unexpected(*failure_);
  }
  std::size_t consumed{};
  if (frame_.empty()) {
    const std::size_t copied = std::min(
        bytes.size(), distributed_vector_aggregate_exchange_format::kHeaderLength - header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != distributed_vector_aggregate_exchange_format::kHeaderLength) {
      return DistributedVectorAggregateExchangeReadStep{.consumed_bytes = consumed,
                                                        .message = std::nullopt};
    }
    auto header = decode_header(header_, expected_definitions_, limits_, false);
    if (!header.has_value()) {
      failure_ = header.error();
      return common::make_unexpected(*failure_);
    }
    try {
      frame_.resize(header->frame_length);
    } catch (const std::bad_alloc&) {
      failure_ = exhausted("distributed vector aggregate reader allocation failed");
      return common::make_unexpected(*failure_);
    } catch (const std::length_error&) {
      failure_ = exhausted("distributed vector aggregate reader frame exceeds container limits");
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
    return DistributedVectorAggregateExchangeReadStep{.consumed_bytes = consumed,
                                                      .message = std::nullopt};
  }
  auto decoded = decode_distributed_vector_aggregate_exchange_message_exact(
      frame_, expected_definitions_, resources_, limits_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  frame_.clear();
  frame_bytes_ = 0U;
  header_bytes_ = 0U;
  return DistributedVectorAggregateExchangeReadStep{.consumed_bytes = consumed,
                                                    .message = std::move(*decoded)};
}

std::size_t DistributedVectorAggregateExchangeReader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool DistributedVectorAggregateExchangeReader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorAggregateExchangeWriteCursor::DistributedVectorAggregateExchangeWriteCursor(
    EncodedDistributedVectorAggregateExchangeMessage encoded) noexcept
    : encoded_(std::move(encoded)) {}

DistributedVectorAggregateExchangeWriteCursor::DistributedVectorAggregateExchangeWriteCursor(
    DistributedVectorAggregateExchangeWriteCursor&& other) noexcept
    : encoded_(std::move(other.encoded_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_.bytes().size();
}

DistributedVectorAggregateExchangeWriteCursor&
DistributedVectorAggregateExchangeWriteCursor::operator=(
    DistributedVectorAggregateExchangeWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_ = std::move(other.encoded_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_.bytes().size();
  }
  return *this;
}

common::Result<DistributedVectorAggregateExchangeWriteCursor>
DistributedVectorAggregateExchangeWriteCursor::create(
    const DistributedVectorAggregateExchangeMessage& message,
    const std::span<const VectorAggregateDefinition> expected_definitions) {
  auto encoded =
      encode_distributed_vector_aggregate_exchange_message(message, expected_definitions);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedVectorAggregateExchangeWriteCursor{std::move(*encoded)};
}

common::ByteView DistributedVectorAggregateExchangeWriteCursor::pending_write() const noexcept {
  return encoded_.bytes().subspan(written_bytes_);
}

common::Status
DistributedVectorAggregateExchangeWriteCursor::consume_written(const std::size_t bytes) noexcept {
  if (bytes > encoded_.bytes().size() - written_bytes_)
    return invalid("written byte count exceeds distributed vector aggregate frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t DistributedVectorAggregateExchangeWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedVectorAggregateExchangeWriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_.bytes().size();
}

} // namespace chronos::query
