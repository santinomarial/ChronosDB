#include "chronos/cluster/distributed_vector_grouped_aggregate_query_transport_v2.hpp"

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
    std::byte{'G'}, std::byte{'R'}, std::byte{'P'}, std::byte{'2'}};
inline constexpr std::uint16_t kMajor = 2U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderCrcOffset = 108U;
inline constexpr std::uint8_t kLeaderHintFlag = 1U;
inline constexpr std::uint8_t kNoPayload = 0U;
inline constexpr std::uint8_t kGroupedExchangePayload = 1U;

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

[[nodiscard]] common::Status unauthenticated(const char* message) {
  return {common::StatusCode::kUnauthenticated, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] bool retryable_status(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable ||
         code == common::StatusCode::kResourceExhausted || code == common::StatusCode::kIoError;
}

[[nodiscard]] DistributedVectorGroupedAggregateQuerySenderV2::TimePoint
saturating_add(const DistributedVectorGroupedAggregateQuerySenderV2::TimePoint now,
               const std::chrono::milliseconds delay) noexcept {
  const auto converted = std::chrono::duration_cast<
      DistributedVectorGroupedAggregateQuerySenderV2::TimePoint::duration>(delay);
  if (now > DistributedVectorGroupedAggregateQuerySenderV2::TimePoint::max() - converted)
    return DistributedVectorGroupedAggregateQuerySenderV2::TimePoint::max();
  return now + converted;
}

[[nodiscard]] bool valid_payload_limits(
    const query::DistributedVectorGroupedAggregateExchangeDecodeLimits& limits) noexcept {
  return limits.maximum_frame_length >=
             query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.maximum_frame_length <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength &&
         limits.maximum_key_payload_bytes > 0U &&
         limits.maximum_key_payload_bytes <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes &&
         limits.maximum_groups > 0U &&
         limits.maximum_groups <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_group_keys > 0U &&
         limits.maximum_group_keys <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys &&
         limits.maximum_aggregates <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates &&
         limits.state.maximum_frame_length >=
             query::distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.state.maximum_frame_length <=
             query::distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.state.maximum_variable_extremum_bytes > 0U &&
         limits.state.maximum_variable_extremum_bytes <=
             query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes;
}

[[nodiscard]] common::Status
validate_authority(const std::span<const query::VectorGroupKeyDefinition> keys,
                   const std::span<const query::VectorAggregateDefinition> aggregates,
                   const query::DistributedVectorGroupedAggregateExchangeDecodeLimits& limits) {
  if (!valid_payload_limits(limits))
    return invalid("grouped vector query v2 response payload limits are invalid");
  return query::validate_distributed_vector_grouped_aggregate_authority(
      keys, aggregates, limits.maximum_group_keys, limits.maximum_aggregates);
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
  return common::make_unexpected(invalid("grouped vector query v2 response status is invalid"));
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
        corruption("grouped vector query v2 response status is unknown"));
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

struct ParsedHeader {
  std::size_t total_length{};
  raft::NodeId source{};
  raft::NodeId target{};
  common::Uuid query_id;
  schema::TabletId tablet_id;
  common::StatusCode status{common::StatusCode::kInternal};
  std::uint32_t payload_length{};
  std::uint32_t payload_crc{};
  std::optional<DistributedQueryLeaderHint> leader_hint;
};

[[nodiscard]] common::Result<ParsedHeader>
parse_header(const common::ByteView header,
             const query::DistributedVectorGroupedAggregateExchangeDecodeLimits& payload_limits) {
  if (header.size() != kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize ||
      !std::ranges::equal(header.first(kResponseMagic.size()), kResponseMagic)) {
    return common::make_unexpected(
        corruption("grouped vector query v2 response streaming header is invalid"));
  }
  if (!valid_payload_limits(payload_limits)) {
    return common::make_unexpected(
        invalid("grouped vector query v2 response payload limits are invalid"));
  }
  common::ByteReader crc_reader{header.last(4U)};
  const auto stored_crc = crc_reader.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(header.first(kHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("grouped vector query v2 response streaming header checksum differs"));
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
        corruption("grouped vector query v2 response streaming header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor) {
    return common::make_unexpected(
        unsupported("grouped vector query v2 response version is unsupported"));
  }
  const auto status = decode_status(*status_code);
  if (!status.has_value())
    return common::make_unexpected(status.error());
  const bool payload_present = *payload_kind == kGroupedExchangePayload;
  const bool hint_present = (*flags & kLeaderHintFlag) != 0U;
  if (*header_length != kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize ||
      *source == 0U || *target == 0U || *source == *target || query_id->is_nil() ||
      tablet_id->uuid().is_nil() || (*flags & ~kLeaderHintFlag) != 0U || *small_reserved != 0U ||
      *reserved != 0U || *trailing_reserved != 0U ||
      (*payload_kind != kNoPayload && *payload_kind != kGroupedExchangePayload) ||
      (*status == common::StatusCode::kOk) != payload_present ||
      (payload_present &&
       (*payload_length <
            query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength ||
        *payload_length >
            query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength)) ||
      (!payload_present && (*payload_length != 0U || *payload_crc != 0U)) ||
      *total_length != kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
                           *payload_length +
                           kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize ||
      *total_length > kMaximumDistributedVectorGroupedAggregateQueryResponseV2Size ||
      (hint_present && (*leader_node == 0U || *leader_epoch == 0U)) ||
      (!hint_present && (*leader_node != 0U || *leader_epoch != 0U))) {
    return common::make_unexpected(
        corruption("grouped vector query v2 response streaming header is invalid"));
  }
  if (payload_present && *payload_length > payload_limits.maximum_frame_length) {
    return common::make_unexpected(
        exhausted("grouped vector query v2 response payload exceeds its frame limit"));
  }
  std::optional<DistributedQueryLeaderHint> hint;
  if (hint_present)
    hint = DistributedQueryLeaderHint{*leader_node, *leader_epoch};
  return ParsedHeader{.total_length = static_cast<std::size_t>(*total_length),
                      .source = *source,
                      .target = *target,
                      .query_id = *query_id,
                      .tablet_id = *tablet_id,
                      .status = *status,
                      .payload_length = *payload_length,
                      .payload_crc = *payload_crc,
                      .leader_hint = hint};
}

[[nodiscard]] common::Result<query::DistributedVectorGroupedAggregateAuthority>
bind_worker_authority(DistributedVectorGroupedAggregateQueryWorkerServiceV2& worker,
                      const query::DistributedVectorFragmentDispatchV2& dispatch) noexcept {
  try {
    return worker.bind_authority(dispatch);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped vector query v2 authority binding allocation failed"));
  } catch (...) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "grouped vector query v2 authority binding threw"});
  }
}

[[nodiscard]] common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
execute_grouped_worker(DistributedVectorGroupedAggregateQueryWorkerServiceV2& worker,
                       const query::DistributedVectorFragmentDispatchV2& dispatch) noexcept {
  try {
    return worker.execute(dispatch);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped vector query v2 worker allocation failed"));
  } catch (...) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "grouped vector query v2 worker threw"});
  }
}

[[nodiscard]] common::Result<std::vector<std::vector<std::byte>>>
one_response(std::vector<std::byte> encoded) {
  try {
    std::vector<std::vector<std::byte>> frames;
    frames.push_back(std::move(encoded));
    return frames;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped vector query v2 response publication allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("grouped vector query v2 response publication exceeds container limits"));
  }
}

[[nodiscard]] bool
same_grouped_authority(const query::DistributedVectorGroupedAggregateAuthority& left,
                       const query::DistributedVectorGroupedAggregateAuthority& right) noexcept {
  if (left.keys.size() != right.keys.size() || left.aggregates.size() != right.aggregates.size()) {
    return false;
  }
  for (std::size_t ordinal = 0U; ordinal < left.keys.size(); ++ordinal) {
    if (left.keys[ordinal].column_ordinal != right.keys[ordinal].column_ordinal ||
        left.keys[ordinal].type != right.keys[ordinal].type ||
        left.keys[ordinal].nullable != right.keys[ordinal].nullable) {
      return false;
    }
  }
  return std::equal(left.aggregates.begin(), left.aggregates.end(), right.aggregates.begin());
}

} // namespace

common::Status validate_distributed_vector_grouped_aggregate_query_authority_v2(
    const query::DistributedVectorFragmentDispatchV2& dispatch,
    const std::span<const query::VectorGroupKeyDefinition> keys,
    const std::span<const query::VectorAggregateDefinition> aggregates) {
  const auto& plan = dispatch.dispatch.plan;
  const auto& columns = dispatch.result_schema.columns;
  if (plan.mode != query::DistributedVectorPlanMode::kGroupedAggregate || keys.empty() ||
      keys.size() != plan.group_key_input_indices.size() ||
      aggregates.size() != plan.aggregates.size() ||
      columns.size() != keys.size() + aggregates.size()) {
    return invalid("grouped vector query v2 requires exact key and aggregate authority");
  }
  common::Status authority =
      query::validate_distributed_vector_grouped_aggregate_authority(keys, aggregates);
  if (!authority.is_ok())
    return authority;
  for (std::size_t ordinal = 0U; ordinal < keys.size(); ++ordinal) {
    if (keys[ordinal].column_ordinal != plan.group_key_input_indices[ordinal] ||
        keys[ordinal].type != columns[ordinal].type ||
        keys[ordinal].nullable != columns[ordinal].nullable) {
      return invalid("grouped vector query v2 key authority differs from the admitted plan");
    }
  }
  for (std::size_t ordinal = 0U; ordinal < aggregates.size(); ++ordinal) {
    const auto& intent = plan.aggregates[ordinal];
    const auto& definition = aggregates[ordinal];
    if (definition.operation != intent.operation ||
        definition.input.has_value() != intent.input_index.has_value() ||
        (definition.input.has_value() && definition.input->column_ordinal != *intent.input_index)) {
      return invalid("grouped vector query v2 aggregate authority differs from the admitted plan");
    }
    const auto shape = query::vector_aggregate_output_shape(definition);
    if (!shape.has_value())
      return shape.error();
    const auto& column = columns[keys.size() + ordinal];
    if (shape->type != column.type || shape->nullable != column.nullable) {
      return invalid("grouped vector query v2 aggregate authority differs from result schema");
    }
  }
  return common::Status::ok();
}

common::Result<std::vector<std::byte>>
encode_distributed_vector_grouped_aggregate_query_response_v2(
    const DistributedVectorGroupedAggregateQueryResponseV2& response,
    const std::span<const query::VectorGroupKeyDefinition> expected_keys,
    const std::span<const query::VectorAggregateDefinition> expected_aggregates) {
  const common::Status authority = query::validate_distributed_vector_grouped_aggregate_authority(
      expected_keys, expected_aggregates);
  if (!authority.is_ok())
    return common::make_unexpected(authority);
  if (response.source_node_id == 0U || response.target_node_id == 0U ||
      response.source_node_id == response.target_node_id || response.query_id.is_nil() ||
      response.tablet_id.uuid().is_nil() ||
      (response.status_code == common::StatusCode::kOk) != response.payload.has_value()) {
    return common::make_unexpected(
        invalid("grouped vector query v2 response identity or result is invalid"));
  }
  const auto status = encode_status(response.status_code);
  if (!status.has_value())
    return common::make_unexpected(status.error());
  std::vector<std::byte> payload_bytes;
  if (response.payload.has_value()) {
    const auto& position = response.payload->position();
    if (position.query_id != response.query_id || position.tablet_id != response.tablet_id) {
      return common::make_unexpected(
          invalid("grouped vector query v2 response payload is not correlated"));
    }
    auto encoded = query::encode_distributed_vector_grouped_aggregate_exchange_message(
        *response.payload, expected_keys, expected_aggregates);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    try {
      payload_bytes.assign(encoded->bytes().begin(), encoded->bytes().end());
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(
          exhausted("grouped vector query v2 response payload allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(
          exhausted("grouped vector query v2 response payload exceeds container limits"));
    }
  }
  if (response.leader_hint.has_value() &&
      (response.leader_hint->node_id == 0U || response.leader_hint->placement_epoch == 0U)) {
    return common::make_unexpected(
        invalid("grouped vector query v2 response leader hint is invalid"));
  }
  try {
    const std::size_t total = kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
                              payload_bytes.size() +
                              kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kResponseMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize);
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
      write = writer.write_u8(response.payload.has_value() ? kGroupedExchangePayload : kNoPayload);
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
          invalid("grouped vector query v2 response header is inconsistent"));
    }
    write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_exact(payload_bytes);
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!write.is_ok() || !writer.full()) {
      return common::make_unexpected(
          invalid("grouped vector query v2 response frame is inconsistent"));
    }
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped vector query v2 response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("grouped vector query v2 response exceeds container limits"));
  }
}

common::Result<DistributedVectorGroupedAggregateQueryResponseV2>
decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
    const common::ByteView bytes,
    const std::span<const query::VectorGroupKeyDefinition> expected_keys,
    const std::span<const query::VectorAggregateDefinition> expected_aggregates,
    const query::QueryResourceContext& resources,
    const query::DistributedVectorGroupedAggregateExchangeDecodeLimits limits) {
  const common::Status authority = validate_authority(expected_keys, expected_aggregates, limits);
  if (!authority.is_ok())
    return common::make_unexpected(authority);
  constexpr std::size_t kMinimum = kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
                                   kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
  if (bytes.size() < kMinimum ||
      bytes.size() > kMaximumDistributedVectorGroupedAggregateQueryResponseV2Size) {
    return common::make_unexpected(
        corruption("grouped vector query v2 response length is invalid"));
  }
  auto header = parse_header(
      bytes.first(kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize), limits);
  if (!header.has_value())
    return common::make_unexpected(header.error());
  if (header->total_length != bytes.size()) {
    return common::make_unexpected(corruption("grouped vector query v2 response length differs"));
  }
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U))) {
    return common::make_unexpected(corruption("grouped vector query v2 response checksum differs"));
  }
  const common::ByteView payload = bytes.subspan(
      kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize, header->payload_length);
  if (header->payload_length != 0U && header->payload_crc != common::crc32c(payload)) {
    return common::make_unexpected(
        corruption("grouped vector query v2 response payload checksum differs"));
  }
  std::optional<query::DistributedVectorGroupedAggregateExchangeMessage> decoded_payload;
  if (header->payload_length != 0U) {
    auto decoded = query::decode_distributed_vector_grouped_aggregate_exchange_message_exact(
        payload, expected_keys, expected_aggregates, resources, limits);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
    if (decoded->position().query_id != header->query_id ||
        decoded->position().tablet_id != header->tablet_id) {
      return common::make_unexpected(
          corruption("grouped vector query v2 response payload is not correlated"));
    }
    decoded_payload.emplace(std::move(*decoded));
  }
  return DistributedVectorGroupedAggregateQueryResponseV2{.source_node_id = header->source,
                                                          .target_node_id = header->target,
                                                          .query_id = header->query_id,
                                                          .tablet_id = header->tablet_id,
                                                          .status_code = header->status,
                                                          .payload = std::move(decoded_payload),
                                                          .leader_hint = header->leader_hint};
}

DistributedVectorGroupedAggregateQueryResponseV2Reader::
    DistributedVectorGroupedAggregateQueryResponseV2Reader(
        std::vector<query::VectorGroupKeyDefinition>&& expected_keys,
        std::vector<query::VectorAggregateDefinition>&& expected_aggregates,
        query::QueryResourceContext resources, const std::size_t maximum_frame_length,
        const query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload_limits) noexcept
    : expected_keys_(std::move(expected_keys)),
      expected_aggregates_(std::move(expected_aggregates)), resources_(std::move(resources)),
      maximum_frame_length_(maximum_frame_length), payload_limits_(payload_limits) {}

common::Result<DistributedVectorGroupedAggregateQueryResponseV2ReadStep>
DistributedVectorGroupedAggregateQueryResponseV2Reader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  const common::Status authority =
      validate_authority(expected_keys_, expected_aggregates_, payload_limits_);
  if (!authority.is_ok())
    return common::make_unexpected(authority);
  constexpr std::size_t kMinimum = kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
                                   kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
  if (maximum_frame_length_ < kMinimum ||
      maximum_frame_length_ > kMaximumDistributedVectorGroupedAggregateQueryResponseV2Size) {
    return common::make_unexpected(
        invalid("grouped vector query v2 response reader limit is invalid"));
  }
  std::size_t consumed{};
  if (frame_.empty()) {
    const std::size_t copied = std::min(bytes.size(), header_.size() - header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != header_.size()) {
      return DistributedVectorGroupedAggregateQueryResponseV2ReadStep{.consumed_bytes = consumed,
                                                                      .response = std::nullopt};
    }
    const auto header = parse_header(header_, payload_limits_);
    if (!header.has_value()) {
      failure_ = header.error();
      return common::make_unexpected(*failure_);
    }
    if (header->total_length > maximum_frame_length_) {
      failure_ = exhausted("grouped vector query v2 response exceeds reader frame limit");
      return common::make_unexpected(*failure_);
    }
    try {
      frame_.resize(header->total_length);
    } catch (const std::bad_alloc&) {
      failure_ = exhausted("grouped vector query v2 response reader allocation failed");
      return common::make_unexpected(*failure_);
    } catch (const std::length_error&) {
      failure_ = exhausted("grouped vector query v2 response reader exceeds container limits");
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
    return DistributedVectorGroupedAggregateQueryResponseV2ReadStep{.consumed_bytes = consumed,
                                                                    .response = std::nullopt};
  }
  auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
      frame_, expected_keys_, expected_aggregates_, resources_, payload_limits_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  DistributedVectorGroupedAggregateQueryResponseV2 result = std::move(*decoded);
  frame_.clear();
  frame_bytes_ = 0U;
  header_bytes_ = 0U;
  return DistributedVectorGroupedAggregateQueryResponseV2ReadStep{.consumed_bytes = consumed,
                                                                  .response = std::move(result)};
}

std::size_t
DistributedVectorGroupedAggregateQueryResponseV2Reader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool DistributedVectorGroupedAggregateQueryResponseV2Reader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorGroupedAggregateQueryResponseV2WriteCursor::
    DistributedVectorGroupedAggregateQueryResponseV2WriteCursor(
        std::vector<std::byte> encoded_frame) noexcept
    : encoded_frame_(std::move(encoded_frame)) {}

DistributedVectorGroupedAggregateQueryResponseV2WriteCursor::
    DistributedVectorGroupedAggregateQueryResponseV2WriteCursor(
        DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&& other) noexcept
    : encoded_frame_(std::move(other.encoded_frame_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_frame_.size();
}

DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&
DistributedVectorGroupedAggregateQueryResponseV2WriteCursor::operator=(
    DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_frame_ = std::move(other.encoded_frame_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_frame_.size();
  }
  return *this;
}

common::Result<DistributedVectorGroupedAggregateQueryResponseV2WriteCursor>
DistributedVectorGroupedAggregateQueryResponseV2WriteCursor::create(
    const DistributedVectorGroupedAggregateQueryResponseV2& response,
    const std::span<const query::VectorGroupKeyDefinition> expected_keys,
    const std::span<const query::VectorAggregateDefinition> expected_aggregates) {
  auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
      response, expected_keys, expected_aggregates);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedVectorGroupedAggregateQueryResponseV2WriteCursor{std::move(*encoded)};
}

common::ByteView
DistributedVectorGroupedAggregateQueryResponseV2WriteCursor::pending_write() const noexcept {
  return common::ByteView{encoded_frame_}.subspan(written_bytes_);
}

common::Status DistributedVectorGroupedAggregateQueryResponseV2WriteCursor::consume_written(
    const std::size_t bytes) noexcept {
  if (bytes > encoded_frame_.size() - written_bytes_) {
    return invalid("written byte count exceeds grouped vector query v2 response frame");
  }
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t
DistributedVectorGroupedAggregateQueryResponseV2WriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedVectorGroupedAggregateQueryResponseV2WriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_frame_.size();
}

DistributedVectorGroupedAggregateQueryReceiverV2::DistributedVectorGroupedAggregateQueryReceiverV2(
    const DistributedVectorGroupedAggregateQueryReceiverV2Config config) noexcept
    : config_(config) {}

common::Result<DistributedVectorGroupedAggregateQueryReceiverV2>
DistributedVectorGroupedAggregateQueryReceiverV2::create(
    const DistributedVectorGroupedAggregateQueryReceiverV2Config config) {
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
      kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
  if (config.local_node_id == 0U || config.authorizer == nullptr || config.worker == nullptr ||
      config.maximum_response_frames == 0U ||
      config.maximum_response_frames >
          query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups ||
      config.maximum_response_frames > config.payload.maximum_groups ||
      config.maximum_response_bytes < kMinimumResponseBytes ||
      config.maximum_response_bytes >
          kMaximumDistributedVectorGroupedAggregateQueryV2ResponseBytes ||
      config.maximum_decode_memory_bytes == 0U ||
      config.maximum_decode_memory_bytes >
          kMaximumDistributedVectorGroupedAggregateQueryV2DecodeMemoryBytes ||
      !valid_payload_limits(config.payload)) {
    return common::make_unexpected(
        invalid("grouped vector query v2 receiver configuration is invalid"));
  }
  return DistributedVectorGroupedAggregateQueryReceiverV2{config};
}

common::Result<std::vector<std::vector<std::byte>>>
DistributedVectorGroupedAggregateQueryReceiverV2::receive(
    const common::ByteView request_bytes,
    const network::PeerAuthenticationResult& authenticated_peer) {
  auto bound = receive_bound(request_bytes, authenticated_peer);
  if (!bound.has_value())
    return common::make_unexpected(bound.error());
  return std::move(bound->encoded_responses);
}

common::Result<DistributedVectorGroupedAggregateQueryBoundResponsesV2>
DistributedVectorGroupedAggregateQueryReceiverV2::receive_bound(
    const common::ByteView request_bytes,
    const network::PeerAuthenticationResult& authenticated_peer) {
  if (!authenticated_peer.authorized || authenticated_peer.principal_id == 0U) {
    return common::make_unexpected(
        unauthenticated("grouped vector query v2 requires an authenticated principal"));
  }
  auto request = decode_distributed_vector_query_request_v2_exact(request_bytes);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  auto authorized =
      config_.authorizer->authorize_node(authenticated_peer.principal_id, request->source_node_id);
  if (!authorized.has_value())
    return common::make_unexpected(authorized.error());
  if (!*authorized) {
    return common::make_unexpected(unauthenticated(
        "authenticated principal cannot claim grouped vector query v2 source node"));
  }
  if (request->target_node_id != config_.local_node_id) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnavailable, "grouped vector query v2 targets a different node"});
  }
  if (request->dispatch.dispatch.plan.mode != query::DistributedVectorPlanMode::kGroupedAggregate) {
    return common::make_unexpected(
        invalid("grouped vector query v2 receiver requires a grouped aggregate plan"));
  }

  auto authority = bind_worker_authority(*config_.worker, request->dispatch);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
  const common::Status authority_status =
      validate_distributed_vector_grouped_aggregate_query_authority_v2(
          request->dispatch, authority->keys, authority->aggregates);
  if (!authority_status.is_ok())
    return common::make_unexpected(authority_status);
  auto resources = query::QueryResourceContext::create(config_.maximum_decode_memory_bytes);
  if (!resources.has_value())
    return common::make_unexpected(resources.error());

  const auto& identity = request->dispatch.dispatch;
  const auto encode_failure = [&](const common::StatusCode code,
                                  const std::optional<DistributedQueryLeaderHint> leader_hint =
                                      std::nullopt)
      -> common::Result<DistributedVectorGroupedAggregateQueryBoundResponsesV2> {
    auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
        {.source_node_id = config_.local_node_id,
         .target_node_id = request->source_node_id,
         .query_id = identity.query_id,
         .tablet_id = identity.tablet_id,
         .status_code = code,
         .leader_hint = leader_hint},
        authority->keys, authority->aggregates);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    auto frames = one_response(std::move(*encoded));
    if (!frames.has_value())
      return common::make_unexpected(frames.error());
    return DistributedVectorGroupedAggregateQueryBoundResponsesV2{
        .authority = std::move(*authority), .encoded_responses = std::move(*frames)};
  };

  auto result = execute_grouped_worker(*config_.worker, request->dispatch);
  if (!result.has_value()) {
    std::optional<DistributedQueryLeaderHint> leader_hint;
    if (result.error().code() == common::StatusCode::kUnavailable &&
        config_.leader_hint_provider != nullptr) {
      auto resolved = config_.leader_hint_provider->current_leader_hint(identity.tablet_id,
                                                                        identity.raft_group_id);
      if (!resolved.has_value())
        return common::make_unexpected(resolved.error());
      leader_hint = *resolved;
    }
    return encode_failure(result.error().code(), leader_hint);
  }
  if (!same_grouped_authority(result->authority, *authority)) {
    return common::make_unexpected(
        invalid("grouped vector query v2 worker authority changed after binding"));
  }
  if (result->messages.empty()) {
    return common::make_unexpected(
        invalid("grouped vector query v2 worker returned an empty response vector"));
  }
  if (result->messages.size() > config_.maximum_response_frames)
    return encode_failure(common::StatusCode::kResourceExhausted);

  std::size_t total_response_bytes{};
  try {
    std::vector<std::vector<std::byte>> frames;
    frames.reserve(result->messages.size());
    for (std::size_t ordinal = 0U; ordinal < result->messages.size(); ++ordinal) {
      auto decoded = query::decode_distributed_vector_grouped_aggregate_exchange_message_exact(
          result->messages[ordinal].bytes(), authority->keys, authority->aggregates, *resources,
          config_.payload);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      const auto& position = decoded->position();
      const bool last = ordinal + 1U == result->messages.size();
      const bool valid_empty = position.empty && result->messages.size() == 1U &&
                               position.group_count == 0U && position.group_ordinal == 0U &&
                               position.sequence == 1U && position.terminal;
      const bool valid_groups = !position.empty &&
                                position.group_count == result->messages.size() &&
                                position.group_ordinal == ordinal &&
                                position.sequence == ordinal + 1U && position.terminal == last;
      if (position.query_id != identity.query_id || position.tablet_id != identity.tablet_id ||
          (!valid_empty && !valid_groups)) {
        return common::make_unexpected(
            invalid("grouped vector query v2 worker stream is not correlated and complete"));
      }
      auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
          {.source_node_id = config_.local_node_id,
           .target_node_id = request->source_node_id,
           .query_id = identity.query_id,
           .tablet_id = identity.tablet_id,
           .status_code = common::StatusCode::kOk,
           .payload = std::move(*decoded)},
          authority->keys, authority->aggregates);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      if (encoded->size() > config_.maximum_response_bytes - total_response_bytes)
        return encode_failure(common::StatusCode::kResourceExhausted);
      total_response_bytes += encoded->size();
      frames.push_back(std::move(*encoded));
    }
    return DistributedVectorGroupedAggregateQueryBoundResponsesV2{
        .authority = std::move(*authority), .encoded_responses = std::move(frames)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped vector query v2 response publication allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("grouped vector query v2 response publication exceeds container limits"));
  }
}

DistributedVectorGroupedAggregateQuerySenderV2::DistributedVectorGroupedAggregateQuerySenderV2(
    const raft::NodeId source_node_id, query::DistributedVectorFragmentDispatchV2 dispatch,
    std::vector<query::VectorGroupKeyDefinition>&& keys,
    std::vector<query::VectorAggregateDefinition>&& aggregates,
    query::QueryResourceContext resources, std::vector<std::byte>&& request_bytes,
    const DistributedVectorGroupedAggregateQuerySenderLimitsV2 limits) noexcept
    : source_node_id_(source_node_id), dispatch_(std::move(dispatch)), keys_(std::move(keys)),
      aggregates_(std::move(aggregates)), resources_(std::move(resources)),
      request_bytes_(std::move(request_bytes)), limits_(limits),
      next_backoff_(limits.retry.initial_backoff) {}

common::Result<DistributedVectorGroupedAggregateQuerySenderV2>
DistributedVectorGroupedAggregateQuerySenderV2::create(
    const raft::NodeId source_node_id, query::DistributedVectorFragmentDispatchV2 dispatch,
    std::vector<query::VectorGroupKeyDefinition>&& keys,
    std::vector<query::VectorAggregateDefinition>&& aggregates,
    query::QueryResourceContext resources,
    const DistributedVectorGroupedAggregateQuerySenderLimitsV2 limits) {
  const auto maximum_supported_backoff =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
      kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
  if (source_node_id == 0U || limits.retry.maximum_attempts == 0U ||
      limits.retry.maximum_attempts > 1024U || limits.retry.initial_backoff.count() <= 0 ||
      limits.retry.maximum_backoff < limits.retry.initial_backoff ||
      limits.retry.maximum_backoff > maximum_supported_backoff ||
      limits.maximum_response_frames == 0U ||
      limits.maximum_response_frames >
          query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups ||
      limits.maximum_response_frames > limits.payload.maximum_groups ||
      limits.maximum_response_bytes < kMinimumResponseBytes ||
      limits.maximum_response_bytes >
          kMaximumDistributedVectorGroupedAggregateQueryV2ResponseBytes ||
      dispatch.dispatch.serving_node == source_node_id || !valid_payload_limits(limits.payload)) {
    return common::make_unexpected(
        invalid("grouped vector query v2 sender configuration is invalid"));
  }
  const common::Status authority_status =
      validate_distributed_vector_grouped_aggregate_query_authority_v2(dispatch, keys, aggregates);
  if (!authority_status.is_ok())
    return common::make_unexpected(authority_status);
  if (keys.size() > limits.payload.maximum_group_keys ||
      aggregates.size() > limits.payload.maximum_aggregates) {
    return common::make_unexpected(
        invalid("grouped vector query v2 sender authority exceeds decode limits"));
  }
  auto request_bytes = encode_distributed_vector_query_request_v2(
      {source_node_id, dispatch.dispatch.serving_node, dispatch});
  if (!request_bytes.has_value())
    return common::make_unexpected(request_bytes.error());
  return DistributedVectorGroupedAggregateQuerySenderV2{
      source_node_id,       std::move(dispatch),       std::move(keys), std::move(aggregates),
      std::move(resources), std::move(*request_bytes), limits};
}

common::Result<DistributedVectorGroupedAggregateQueryAttemptV2>
DistributedVectorGroupedAggregateQuerySenderV2::begin_attempt(const TimePoint now) {
  if (state_ == DistributedQuerySenderState::kSucceeded ||
      state_ == DistributedQuerySenderState::kFailed) {
    return common::make_unexpected(invalid("grouped vector query v2 sender is terminal"));
  }
  if (state_ == DistributedQuerySenderState::kWaitingForResponse) {
    return common::make_unexpected(unavailable("grouped vector query v2 response is pending"));
  }
  if (state_ == DistributedQuerySenderState::kBackoff && now < *next_attempt_not_before_) {
    return common::make_unexpected(unavailable("grouped vector query v2 retry backoff is active"));
  }
  if (attempts_started_ >= limits_.retry.maximum_attempts) {
    return common::make_unexpected(invalid("grouped vector query v2 retry budget is exhausted"));
  }
  try {
    std::vector<std::byte> request_bytes = request_bytes_;
    ++attempts_started_;
    state_ = DistributedQuerySenderState::kWaitingForResponse;
    suggested_leader_.reset();
    next_attempt_not_before_.reset();
    return DistributedVectorGroupedAggregateQueryAttemptV2{
        attempts_started_, dispatch_.dispatch.serving_node, std::move(request_bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped vector query v2 attempt allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("grouped vector query v2 attempt exceeds container limits"));
  }
}

common::Status DistributedVectorGroupedAggregateQuerySenderV2::accept_responses(
    const std::span<const DistributedVectorGroupedAggregateQueryResponseV2> responses,
    const TimePoint now) {
  if (state_ != DistributedQuerySenderState::kWaitingForResponse)
    return invalid("grouped vector query v2 sender has no pending response");
  if (responses.empty())
    return invalid("grouped vector query v2 response vector is empty");
  if (responses.size() > limits_.maximum_response_frames)
    return exhausted("grouped vector query v2 response vector exceeds sender frame limit");

  const auto& identity = dispatch_.dispatch;
  const auto validate_correlation =
      [&](const DistributedVectorGroupedAggregateQueryResponseV2& response) {
        return response.source_node_id == identity.serving_node &&
               response.target_node_id == source_node_id_ &&
               response.query_id == identity.query_id && response.tablet_id == identity.tablet_id;
      };
  if (responses.front().status_code != common::StatusCode::kOk) {
    if (responses.size() != 1U || responses.front().payload.has_value() ||
        !validate_correlation(responses.front())) {
      return invalid("grouped vector query v2 failure response is invalid");
    }
    auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
        responses.front(), keys_, aggregates_);
    if (!encoded.has_value())
      return encoded.error();
    if (encoded->size() > limits_.maximum_response_bytes)
      return exhausted("grouped vector query v2 response exceeds sender byte limit");
    auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
        *encoded, keys_, aggregates_, resources_, limits_.payload);
    if (!decoded.has_value())
      return decoded.error();
    suggested_leader_ = decoded->leader_hint;
    return schedule(decoded->status_code, now);
  }

  std::size_t total_response_bytes{};
  try {
    std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> accepted;
    accepted.reserve(responses.size());
    for (std::size_t ordinal = 0U; ordinal < responses.size(); ++ordinal) {
      const auto& response = responses[ordinal];
      if (!validate_correlation(response) || response.status_code != common::StatusCode::kOk ||
          !response.payload.has_value() || response.leader_hint.has_value()) {
        return invalid("grouped vector query v2 success response is invalid");
      }
      const auto& position = response.payload->position();
      const bool last = ordinal + 1U == responses.size();
      const bool valid_empty = position.empty && responses.size() == 1U &&
                               position.group_count == 0U && position.group_ordinal == 0U &&
                               position.sequence == 1U && position.terminal;
      const bool valid_groups = !position.empty && position.group_count == responses.size() &&
                                position.group_ordinal == ordinal &&
                                position.sequence == ordinal + 1U && position.terminal == last;
      if (position.query_id != identity.query_id || position.tablet_id != identity.tablet_id ||
          (!valid_empty && !valid_groups)) {
        return invalid("grouped vector query v2 success response sequence is invalid");
      }
      auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(response, keys_,
                                                                                   aggregates_);
      if (!encoded.has_value())
        return encoded.error();
      if (encoded->size() > limits_.maximum_response_bytes - total_response_bytes)
        return exhausted("grouped vector query v2 response vector exceeds sender byte limit");
      total_response_bytes += encoded->size();
      auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
          *encoded, keys_, aggregates_, resources_, limits_.payload);
      if (!decoded.has_value())
        return decoded.error();
      if (!decoded->payload.has_value())
        return corruption("grouped vector query v2 canonical success payload is absent");
      auto nested = query::encode_distributed_vector_grouped_aggregate_exchange_message(
          *decoded->payload, keys_, aggregates_);
      if (!nested.has_value())
        return nested.error();
      accepted.push_back(std::move(*nested));
    }
    result_ = std::move(accepted);
  } catch (const std::bad_alloc&) {
    return exhausted("grouped vector query v2 sender result allocation failed");
  } catch (const std::length_error&) {
    return exhausted("grouped vector query v2 sender result exceeds container limits");
  }
  last_status_code_ = common::StatusCode::kOk;
  suggested_leader_.reset();
  state_ = DistributedQuerySenderState::kSucceeded;
  next_attempt_not_before_.reset();
  return common::Status::ok();
}

common::Status DistributedVectorGroupedAggregateQuerySenderV2::record_transport_failure(
    const common::StatusCode code, const TimePoint now) {
  if (state_ != DistributedQuerySenderState::kWaitingForResponse)
    return invalid("grouped vector query v2 sender has no active transport attempt");
  if (code == common::StatusCode::kOk)
    return invalid("grouped vector query v2 transport failure cannot be OK");
  suggested_leader_.reset();
  return schedule(code, now);
}

common::Status
DistributedVectorGroupedAggregateQuerySenderV2::schedule(const common::StatusCode code,
                                                         const TimePoint now) {
  last_status_code_ = code;
  if (!retryable_status(code) || attempts_started_ >= limits_.retry.maximum_attempts) {
    state_ = DistributedQuerySenderState::kFailed;
    next_attempt_not_before_.reset();
    return common::Status::ok();
  }
  state_ = DistributedQuerySenderState::kBackoff;
  next_attempt_not_before_ = saturating_add(now, next_backoff_);
  if (next_backoff_ < limits_.retry.maximum_backoff) {
    const auto current = next_backoff_.count();
    const auto maximum = limits_.retry.maximum_backoff.count();
    next_backoff_ = current > maximum / 2
                        ? limits_.retry.maximum_backoff
                        : std::min(next_backoff_ * 2, limits_.retry.maximum_backoff);
  }
  return common::Status::ok();
}

DistributedQuerySenderState DistributedVectorGroupedAggregateQuerySenderV2::state() const noexcept {
  return state_;
}

std::size_t DistributedVectorGroupedAggregateQuerySenderV2::attempts_started() const noexcept {
  return attempts_started_;
}

std::optional<DistributedVectorGroupedAggregateQuerySenderV2::TimePoint>
DistributedVectorGroupedAggregateQuerySenderV2::next_attempt_not_before() const noexcept {
  return next_attempt_not_before_;
}

std::optional<common::StatusCode>
DistributedVectorGroupedAggregateQuerySenderV2::last_status_code() const noexcept {
  return last_status_code_;
}

std::optional<DistributedQueryLeaderHint>
DistributedVectorGroupedAggregateQuerySenderV2::suggested_leader() const noexcept {
  return suggested_leader_;
}

std::span<const query::VectorGroupKeyDefinition>
DistributedVectorGroupedAggregateQuerySenderV2::keys() const noexcept {
  return keys_;
}

std::span<const query::VectorAggregateDefinition>
DistributedVectorGroupedAggregateQuerySenderV2::aggregates() const noexcept {
  return aggregates_;
}

const std::optional<std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>>&
DistributedVectorGroupedAggregateQuerySenderV2::result() const noexcept {
  return result_;
}

} // namespace chronos::cluster
