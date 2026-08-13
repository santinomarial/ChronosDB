#include "chronos/cluster/distributed_vector_aggregate_query_transport_v2.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

inline constexpr std::array<std::byte, 8U> kResponseMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'V'},
    std::byte{'A'}, std::byte{'R'}, std::byte{'P'}, std::byte{'2'}};
inline constexpr std::uint16_t kMajor = 2U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderCrcOffset = 108U;
inline constexpr std::uint8_t kLeaderHintFlag = 1U;
inline constexpr std::uint8_t kNoPayload = 0U;
inline constexpr std::uint8_t kAggregateExchangePayload = 1U;

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

[[nodiscard]] common::Status
validate_payload_limits(const query::DistributedVectorAggregateExchangeDecodeLimits& limits) {
  if (limits.maximum_frame_length <
          query::distributed_vector_aggregate_exchange_format::kMinimumFrameLength ||
      limits.maximum_frame_length >
          query::distributed_vector_aggregate_exchange_format::kMaximumFrameLength ||
      limits.maximum_aggregates == 0U ||
      limits.maximum_aggregates >
          query::distributed_vector_aggregate_exchange_format::kMaximumAggregates ||
      limits.state.maximum_frame_length <
          query::distributed_vector_aggregate_state_format::kMinimumFrameLength ||
      limits.state.maximum_frame_length >
          query::distributed_vector_aggregate_state_format::kMaximumFrameLength ||
      limits.state.maximum_variable_extremum_bytes == 0U ||
      limits.state.maximum_variable_extremum_bytes >
          query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes) {
    return invalid("vector aggregate query v2 response payload limits are invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<std::uint8_t> encode_status(const common::StatusCode code) {
  switch (code) {
  case common::StatusCode::kOk:
    return 0U;
  case common::StatusCode::kCancelled:
    return 1U;
  case common::StatusCode::kInvalidArgument:
    return 2U;
  case common::StatusCode::kOutOfRange:
    return 3U;
  case common::StatusCode::kNotFound:
    return 4U;
  case common::StatusCode::kAlreadyExists:
    return 5U;
  case common::StatusCode::kCorruption:
    return 6U;
  case common::StatusCode::kIoError:
    return 7U;
  case common::StatusCode::kResourceExhausted:
    return 8U;
  case common::StatusCode::kUnavailable:
    return 9U;
  case common::StatusCode::kNotSupported:
    return 10U;
  case common::StatusCode::kUnauthenticated:
    return 11U;
  case common::StatusCode::kInternal:
    return 12U;
  }
  return common::make_unexpected(invalid("vector aggregate query v2 response status is invalid"));
}

[[nodiscard]] common::Result<common::StatusCode> decode_status(const std::uint8_t code) {
  switch (code) {
  case 0U:
    return common::StatusCode::kOk;
  case 1U:
    return common::StatusCode::kCancelled;
  case 2U:
    return common::StatusCode::kInvalidArgument;
  case 3U:
    return common::StatusCode::kOutOfRange;
  case 4U:
    return common::StatusCode::kNotFound;
  case 5U:
    return common::StatusCode::kAlreadyExists;
  case 6U:
    return common::StatusCode::kCorruption;
  case 7U:
    return common::StatusCode::kIoError;
  case 8U:
    return common::StatusCode::kResourceExhausted;
  case 9U:
    return common::StatusCode::kUnavailable;
  case 10U:
    return common::StatusCode::kNotSupported;
  case 11U:
    return common::StatusCode::kUnauthenticated;
  case 12U:
    return common::StatusCode::kInternal;
  default:
    return common::make_unexpected(
        corruption("vector aggregate query v2 response status is unknown"));
  }
}

[[nodiscard]] common::Result<common::Uuid> read_uuid(common::ByteReader& reader) {
  const auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  return common::Uuid{owned};
}

[[nodiscard]] common::Result<schema::TabletId> read_tablet(common::ByteReader& reader) {
  const auto value = read_uuid(reader);
  return value.has_value() ? schema::TabletId::from_uuid(*value)
                           : common::make_unexpected(value.error());
}

[[nodiscard]] common::Result<std::size_t>
response_frame_length(const common::ByteView header,
                      const query::DistributedVectorAggregateExchangeDecodeLimits& payload_limits) {
  if (header.size() != kDistributedVectorAggregateQueryResponseV2HeaderSize ||
      !std::ranges::equal(header.first(kResponseMagic.size()), kResponseMagic)) {
    return common::make_unexpected(
        corruption("vector aggregate query v2 response streaming header is invalid"));
  }
  const common::Status limits_status = validate_payload_limits(payload_limits);
  if (!limits_status.is_ok())
    return common::make_unexpected(limits_status);
  common::ByteReader crc_reader{header.last(4U)};
  const auto stored_crc = crc_reader.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(header.first(kHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("vector aggregate query v2 response streaming header checksum differs"));
  }
  common::ByteReader reader{header.subspan(kResponseMagic.size())};
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto source = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto query_id = read_uuid(reader);
  const auto tablet_id = read_tablet(reader);
  const auto status_code = reader.read_u8();
  const auto payload_kind = reader.read_u8();
  const auto flags = reader.read_u8();
  const auto small_reserved = reader.read_u8();
  const auto payload_length = reader.read_u32_le();
  const auto payload_crc = reader.read_u32_le();
  const auto reserved = reader.read_u32_le();
  const auto leader_node = reader.read_u64_le();
  const auto leader_epoch = reader.read_u64_le();
  const auto trailing_reserved = reader.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value() || !source.has_value() || !target.has_value() ||
      !query_id.has_value() || !tablet_id.has_value() || !status_code.has_value() ||
      !payload_kind.has_value() || !flags.has_value() || !small_reserved.has_value() ||
      !payload_length.has_value() || !payload_crc.has_value() || !reserved.has_value() ||
      !leader_node.has_value() || !leader_epoch.has_value() || !trailing_reserved.has_value()) {
    return common::make_unexpected(
        corruption("vector aggregate query v2 response streaming header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor) {
    return common::make_unexpected(
        unsupported("vector aggregate query v2 response version is unsupported"));
  }
  const auto status = decode_status(*status_code);
  if (!status.has_value())
    return common::make_unexpected(status.error());
  const bool payload_present = *payload_kind == kAggregateExchangePayload;
  const bool hint_present = (*flags & kLeaderHintFlag) != 0U;
  if (*header_length != kDistributedVectorAggregateQueryResponseV2HeaderSize || *source == 0U ||
      *target == 0U || *source == *target || query_id->is_nil() || tablet_id->uuid().is_nil() ||
      (*flags & ~kLeaderHintFlag) != 0U || *small_reserved != 0U || *reserved != 0U ||
      *trailing_reserved != 0U ||
      (*payload_kind != kNoPayload && *payload_kind != kAggregateExchangePayload) ||
      (*status == common::StatusCode::kOk) != payload_present ||
      (payload_present &&
       (*payload_length <
            query::distributed_vector_aggregate_exchange_format::kMinimumFrameLength ||
        *payload_length >
            query::distributed_vector_aggregate_exchange_format::kMaximumFrameLength)) ||
      (!payload_present && (*payload_length != 0U || *payload_crc != 0U)) ||
      *total_length != kDistributedVectorAggregateQueryResponseV2HeaderSize + *payload_length +
                           kDistributedVectorAggregateQueryResponseV2TrailerSize ||
      *total_length > kMaximumDistributedVectorAggregateQueryResponseV2Size ||
      (hint_present && (*leader_node == 0U || *leader_epoch == 0U)) ||
      (!hint_present && (*leader_node != 0U || *leader_epoch != 0U))) {
    return common::make_unexpected(
        corruption("vector aggregate query v2 response streaming header is invalid"));
  }
  if (payload_present && *payload_length > payload_limits.maximum_frame_length) {
    return common::make_unexpected(
        exhausted("vector aggregate query v2 response payload exceeds its frame limit"));
  }
  return static_cast<std::size_t>(*total_length);
}

} // namespace

common::Result<std::vector<std::byte>> encode_distributed_vector_aggregate_query_response_v2(
    const DistributedVectorAggregateQueryResponseV2& response,
    const std::span<const query::VectorAggregateDefinition> expected_definitions) {
  const common::Status definitions_status =
      query::validate_distributed_vector_aggregate_definitions(expected_definitions);
  if (!definitions_status.is_ok())
    return common::make_unexpected(definitions_status);
  if (response.source_node_id == 0U || response.target_node_id == 0U ||
      response.source_node_id == response.target_node_id || response.query_id.is_nil() ||
      response.tablet_id.uuid().is_nil() ||
      (response.status_code == common::StatusCode::kOk) != response.payload.has_value()) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 response identity or result is invalid"));
  }
  const auto status = encode_status(response.status_code);
  if (!status.has_value())
    return common::make_unexpected(status.error());
  std::vector<std::byte> payload_bytes;
  if (response.payload.has_value()) {
    if (response.payload->query_id != response.query_id ||
        response.payload->tablet_id != response.tablet_id) {
      return common::make_unexpected(
          invalid("vector aggregate query v2 response payload is not correlated"));
    }
    auto encoded = query::encode_distributed_vector_aggregate_exchange_message(
        *response.payload, expected_definitions);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    try {
      payload_bytes.assign(encoded->bytes().begin(), encoded->bytes().end());
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(
          exhausted("vector aggregate query v2 response payload allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(
          exhausted("vector aggregate query v2 response payload exceeds container limits"));
    }
  }
  if (response.leader_hint.has_value() &&
      (response.leader_hint->node_id == 0U || response.leader_hint->placement_epoch == 0U)) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 response leader hint is invalid"));
  }
  try {
    const std::size_t total = kDistributedVectorAggregateQueryResponseV2HeaderSize +
                              payload_bytes.size() +
                              kDistributedVectorAggregateQueryResponseV2TrailerSize;
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kResponseMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kDistributedVectorAggregateQueryResponseV2HeaderSize);
    if (write.is_ok())
      write = writer.write_u64_le(total);
    if (write.is_ok())
      write = writer.write_u64_le(response.source_node_id);
    if (write.is_ok())
      write = writer.write_u64_le(response.target_node_id);
    if (write.is_ok())
      write = writer.write_exact(response.query_id.bytes());
    if (write.is_ok())
      write = writer.write_exact(response.tablet_id.bytes());
    if (write.is_ok())
      write = writer.write_u8(*status);
    if (write.is_ok())
      write =
          writer.write_u8(response.payload.has_value() ? kAggregateExchangePayload : kNoPayload);
    if (write.is_ok())
      write = writer.write_u8(response.leader_hint.has_value() ? kLeaderHintFlag : 0U);
    if (write.is_ok())
      write = writer.zero_fill(1U);
    if (write.is_ok())
      write = writer.write_u32_le(static_cast<std::uint32_t>(payload_bytes.size()));
    if (write.is_ok())
      write = writer.write_u32_le(payload_bytes.empty() ? 0U : common::crc32c(payload_bytes));
    if (write.is_ok())
      write = writer.zero_fill(4U);
    if (write.is_ok())
      write = writer.write_u64_le(response.leader_hint.has_value() ? response.leader_hint->node_id
                                                                   : 0U);
    if (write.is_ok())
      write = writer.write_u64_le(
          response.leader_hint.has_value() ? response.leader_hint->placement_epoch : 0U);
    if (write.is_ok())
      write = writer.zero_fill(4U);
    if (!write.is_ok() || writer.offset() != kHeaderCrcOffset) {
      return common::make_unexpected(
          invalid("vector aggregate query v2 response header is inconsistent"));
    }
    write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_exact(payload_bytes);
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!write.is_ok() || !writer.full()) {
      return common::make_unexpected(
          invalid("vector aggregate query v2 response frame is inconsistent"));
    }
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("vector aggregate query v2 response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("vector aggregate query v2 response exceeds container limits"));
  }
}

common::Result<DistributedVectorAggregateQueryResponseV2>
decode_distributed_vector_aggregate_query_response_v2_exact(
    const common::ByteView bytes,
    const std::span<const query::VectorAggregateDefinition> expected_definitions,
    const query::QueryResourceContext& resources,
    const query::DistributedVectorAggregateExchangeDecodeLimits limits) {
  const common::Status limits_status = validate_payload_limits(limits);
  if (!limits_status.is_ok())
    return common::make_unexpected(limits_status);
  const common::Status definitions_status =
      query::validate_distributed_vector_aggregate_definitions(expected_definitions,
                                                               limits.maximum_aggregates);
  if (!definitions_status.is_ok())
    return common::make_unexpected(definitions_status);
  if (bytes.size() < kDistributedVectorAggregateQueryResponseV2HeaderSize +
                         kDistributedVectorAggregateQueryResponseV2TrailerSize ||
      bytes.size() > kMaximumDistributedVectorAggregateQueryResponseV2Size) {
    return common::make_unexpected(
        corruption("vector aggregate query v2 response length is invalid"));
  }
  const auto frame_length = response_frame_length(
      bytes.first(kDistributedVectorAggregateQueryResponseV2HeaderSize), limits);
  if (!frame_length.has_value())
    return common::make_unexpected(frame_length.error());
  if (*frame_length != bytes.size()) {
    return common::make_unexpected(corruption("vector aggregate query v2 response length differs"));
  }
  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kResponseMagic.size() + 4U + 4U + 8U));
  const auto source = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  auto query_id = read_uuid(reader);
  auto tablet_id = read_tablet(reader);
  const auto status_code = reader.read_u8();
  const auto payload_kind = reader.read_u8();
  const auto flags = reader.read_u8();
  static_cast<void>(reader.skip(1U));
  const auto payload_length = reader.read_u32_le();
  const auto payload_crc = reader.read_u32_le();
  static_cast<void>(reader.skip(4U));
  const auto leader_node = reader.read_u64_le();
  const auto leader_epoch = reader.read_u64_le();
  if (!source.has_value() || !target.has_value() || !query_id.has_value() ||
      !tablet_id.has_value() || !status_code.has_value() || !payload_kind.has_value() ||
      !flags.has_value() || !payload_length.has_value() || !payload_crc.has_value() ||
      !leader_node.has_value() || !leader_epoch.has_value()) {
    return common::make_unexpected(
        corruption("vector aggregate query v2 response header is truncated"));
  }
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U))) {
    return common::make_unexpected(
        corruption("vector aggregate query v2 response checksum differs"));
  }
  const bool payload_present = *payload_kind == kAggregateExchangePayload;
  const common::ByteView payload =
      bytes.subspan(kDistributedVectorAggregateQueryResponseV2HeaderSize,
                    static_cast<std::size_t>(*payload_length));
  if (payload_present && *payload_crc != common::crc32c(payload)) {
    return common::make_unexpected(
        corruption("vector aggregate query v2 response payload checksum differs"));
  }
  std::optional<query::DistributedVectorAggregateExchangeMessage> decoded_payload;
  if (payload_present) {
    auto decoded = query::decode_distributed_vector_aggregate_exchange_message_exact(
        payload, expected_definitions, resources, limits);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
    if (decoded->query_id != *query_id || decoded->tablet_id != *tablet_id) {
      return common::make_unexpected(
          corruption("vector aggregate query v2 response payload is not correlated"));
    }
    decoded_payload = std::move(*decoded);
  }
  const auto status = decode_status(*status_code);
  if (!status.has_value())
    return common::make_unexpected(status.error());
  std::optional<DistributedQueryLeaderHint> hint;
  if ((*flags & kLeaderHintFlag) != 0U)
    hint = DistributedQueryLeaderHint{*leader_node, *leader_epoch};
  return DistributedVectorAggregateQueryResponseV2{.source_node_id = *source,
                                                   .target_node_id = *target,
                                                   .query_id = *query_id,
                                                   .tablet_id = *tablet_id,
                                                   .status_code = *status,
                                                   .payload = std::move(decoded_payload),
                                                   .leader_hint = hint};
}

DistributedVectorAggregateQueryResponseV2Reader::DistributedVectorAggregateQueryResponseV2Reader(
    std::vector<query::VectorAggregateDefinition>&& expected_definitions,
    query::QueryResourceContext resources, const std::size_t maximum_frame_length,
    const query::DistributedVectorAggregateExchangeDecodeLimits payload_limits) noexcept
    : expected_definitions_(std::move(expected_definitions)), resources_(std::move(resources)),
      maximum_frame_length_(maximum_frame_length), payload_limits_(payload_limits) {}

common::Result<DistributedVectorAggregateQueryResponseV2ReadStep>
DistributedVectorAggregateQueryResponseV2Reader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  const common::Status limits_status = validate_payload_limits(payload_limits_);
  if (!limits_status.is_ok())
    return common::make_unexpected(limits_status);
  const common::Status definitions_status =
      query::validate_distributed_vector_aggregate_definitions(expected_definitions_,
                                                               payload_limits_.maximum_aggregates);
  if (!definitions_status.is_ok())
    return common::make_unexpected(definitions_status);
  constexpr std::size_t kMinimum = kDistributedVectorAggregateQueryResponseV2HeaderSize +
                                   kDistributedVectorAggregateQueryResponseV2TrailerSize;
  if (maximum_frame_length_ < kMinimum ||
      maximum_frame_length_ > kMaximumDistributedVectorAggregateQueryResponseV2Size) {
    return common::make_unexpected(
        invalid("vector aggregate query v2 response reader limit is invalid"));
  }
  std::size_t consumed{};
  if (frame_.empty()) {
    const std::size_t copied = std::min(bytes.size(), header_.size() - header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != header_.size()) {
      return DistributedVectorAggregateQueryResponseV2ReadStep{.consumed_bytes = consumed,
                                                               .response = std::nullopt};
    }
    const auto frame_length = response_frame_length(header_, payload_limits_);
    if (!frame_length.has_value()) {
      failure_ = frame_length.error();
      return common::make_unexpected(*failure_);
    }
    if (*frame_length > maximum_frame_length_) {
      failure_ = exhausted("vector aggregate query v2 response exceeds reader frame limit");
      return common::make_unexpected(*failure_);
    }
    try {
      frame_.resize(*frame_length);
    } catch (const std::bad_alloc&) {
      failure_ = exhausted("vector aggregate query v2 response reader allocation failed");
      return common::make_unexpected(*failure_);
    } catch (const std::length_error&) {
      failure_ = exhausted("vector aggregate query v2 response reader exceeds container limits");
      return common::make_unexpected(*failure_);
    }
    std::ranges::copy(header_, frame_.begin());
    frame_bytes_ = header_.size();
  }
  const common::ByteView remaining = bytes.subspan(consumed);
  const std::size_t copied = std::min(remaining.size(), frame_.size() - frame_bytes_);
  std::ranges::copy(remaining.first(copied),
                    frame_.begin() + static_cast<std::ptrdiff_t>(frame_bytes_));
  frame_bytes_ += copied;
  consumed += copied;
  if (frame_bytes_ != frame_.size()) {
    return DistributedVectorAggregateQueryResponseV2ReadStep{.consumed_bytes = consumed,
                                                             .response = std::nullopt};
  }
  auto decoded = decode_distributed_vector_aggregate_query_response_v2_exact(
      frame_, expected_definitions_, resources_, payload_limits_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  DistributedVectorAggregateQueryResponseV2 result = std::move(*decoded);
  frame_.clear();
  frame_bytes_ = 0U;
  header_bytes_ = 0U;
  return DistributedVectorAggregateQueryResponseV2ReadStep{.consumed_bytes = consumed,
                                                           .response = std::move(result)};
}

std::size_t DistributedVectorAggregateQueryResponseV2Reader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool DistributedVectorAggregateQueryResponseV2Reader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorAggregateQueryResponseV2WriteCursor::
    DistributedVectorAggregateQueryResponseV2WriteCursor(
        std::vector<std::byte> encoded_frame) noexcept
    : encoded_frame_(std::move(encoded_frame)) {}

DistributedVectorAggregateQueryResponseV2WriteCursor::
    DistributedVectorAggregateQueryResponseV2WriteCursor(
        DistributedVectorAggregateQueryResponseV2WriteCursor&& other) noexcept
    : encoded_frame_(std::move(other.encoded_frame_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_frame_.size();
}

DistributedVectorAggregateQueryResponseV2WriteCursor&
DistributedVectorAggregateQueryResponseV2WriteCursor::operator=(
    DistributedVectorAggregateQueryResponseV2WriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_frame_ = std::move(other.encoded_frame_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_frame_.size();
  }
  return *this;
}

common::Result<DistributedVectorAggregateQueryResponseV2WriteCursor>
DistributedVectorAggregateQueryResponseV2WriteCursor::create(
    const DistributedVectorAggregateQueryResponseV2& response,
    const std::span<const query::VectorAggregateDefinition> expected_definitions) {
  auto encoded =
      encode_distributed_vector_aggregate_query_response_v2(response, expected_definitions);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedVectorAggregateQueryResponseV2WriteCursor{std::move(*encoded)};
}

common::ByteView
DistributedVectorAggregateQueryResponseV2WriteCursor::pending_write() const noexcept {
  return common::ByteView{encoded_frame_}.subspan(written_bytes_);
}

common::Status DistributedVectorAggregateQueryResponseV2WriteCursor::consume_written(
    const std::size_t bytes) noexcept {
  if (bytes > encoded_frame_.size() - written_bytes_)
    return invalid("written byte count exceeds vector aggregate query v2 response frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t DistributedVectorAggregateQueryResponseV2WriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedVectorAggregateQueryResponseV2WriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_frame_.size();
}

} // namespace chronos::cluster
