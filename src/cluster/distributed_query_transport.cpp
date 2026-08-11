#include "chronos/cluster/distributed_query_transport.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

inline constexpr std::array<std::byte, 8U> kRequestMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'Q'},
    std::byte{'R'}, std::byte{'E'}, std::byte{'Q'}, std::byte{'1'}};
inline constexpr std::array<std::byte, 8U> kResponseMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'Q'},
    std::byte{'R'}, std::byte{'S'}, std::byte{'P'}, std::byte{'1'}};
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kRequestHeaderCrcOffset = 76U;
inline constexpr std::size_t kResponseHeaderCrcOffset = 108U;
inline constexpr std::uint8_t kLeaderHintFlag = 1U << 0U;
inline constexpr std::size_t kMinimumDispatchSize =
    query::distributed_fragment_dispatch_format::kHeaderLength +
    query::distributed_fragment_format::kHeaderLength + sizeof(std::uint32_t) +
    query::distributed_fragment_format::kTrailerLength +
    query::distributed_fragment_dispatch_format::kTrailerLength;

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
  return common::make_unexpected(invalid("distributed query response status is invalid"));
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
    return common::make_unexpected(corruption("distributed query response status is unknown"));
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

[[nodiscard]] bool zero(const common::ByteView bytes) {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0U}; });
}

[[nodiscard]] common::Result<query::ExchangeMessage>
execute_worker(DistributedQueryWorkerService& worker,
               const query::DistributedAggregateFragmentDispatch& dispatch) noexcept {
  try {
    return worker.execute(dispatch);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed query worker allocation failed"));
  } catch (...) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "distributed query worker threw"});
  }
}

[[nodiscard]] common::Result<std::size_t> request_frame_length(const common::ByteView header) {
  if (header.size() != kDistributedQueryRequestHeaderSize)
    return common::make_unexpected(corruption("distributed query request header is truncated"));
  if (!std::ranges::equal(header.first(kRequestMagic.size()), kRequestMagic))
    return common::make_unexpected(corruption("distributed query request magic is invalid"));
  common::ByteReader crc_reader{header.last(4U)};
  const auto stored_crc = crc_reader.read_u32_le();
  if (!stored_crc.has_value() ||
      *stored_crc != common::crc32c(header.first(kRequestHeaderCrcOffset))) {
    return common::make_unexpected(corruption("distributed query request header checksum differs"));
  }
  common::ByteReader reader{header.subspan(kRequestMagic.size())};
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto source = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto payload_length = reader.read_u64_le();
  const auto payload_crc = reader.read_u32_le();
  const auto reserved = reader.read_exact(24U);
  const auto header_crc = reader.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value() || !source.has_value() || !target.has_value() ||
      !payload_length.has_value() || !payload_crc.has_value() || !reserved.has_value() ||
      !header_crc.has_value()) {
    return common::make_unexpected(corruption("distributed query request header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("distributed query request version is unsupported"));
  if (*header_length != kDistributedQueryRequestHeaderSize || *source == 0U || *target == 0U ||
      *source == *target || !zero(*reserved) || *payload_length < kMinimumDispatchSize ||
      *payload_length > query::distributed_fragment_dispatch_format::kMaximumFrameLength ||
      *total_length != kDistributedQueryRequestHeaderSize + *payload_length +
                           kDistributedQueryRequestTrailerSize ||
      *total_length > kMaximumDistributedQueryRequestSize) {
    return common::make_unexpected(corruption("distributed query request header is invalid"));
  }
  return static_cast<std::size_t>(*total_length);
}

[[nodiscard]] common::Result<std::size_t> response_frame_length(const common::ByteView header) {
  if (header.size() != kDistributedQueryResponseHeaderSize)
    return common::make_unexpected(corruption("distributed query response header is truncated"));
  if (!std::ranges::equal(header.first(kResponseMagic.size()), kResponseMagic))
    return common::make_unexpected(corruption("distributed query response magic is invalid"));
  common::ByteReader crc_reader{header.last(4U)};
  const auto stored_crc = crc_reader.read_u32_le();
  if (!stored_crc.has_value() ||
      *stored_crc != common::crc32c(header.first(kResponseHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("distributed query response header checksum differs"));
  }
  common::ByteReader reader{header.subspan(kResponseMagic.size())};
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value()) {
    return common::make_unexpected(corruption("distributed query response header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("distributed query response version is unsupported"));
  if (*header_length != kDistributedQueryResponseHeaderSize ||
      (*total_length !=
           kDistributedQueryResponseHeaderSize + kDistributedQueryResponseTrailerSize &&
       *total_length != kMaximumDistributedQueryResponseSize)) {
    return common::make_unexpected(corruption("distributed query response header is invalid"));
  }
  return static_cast<std::size_t>(*total_length);
}

[[nodiscard]] bool retryable_status(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable ||
         code == common::StatusCode::kResourceExhausted || code == common::StatusCode::kIoError;
}

[[nodiscard]] DistributedQuerySender::TimePoint
saturating_add(const DistributedQuerySender::TimePoint now,
               const std::chrono::milliseconds delay) noexcept {
  const auto converted =
      std::chrono::duration_cast<DistributedQuerySender::TimePoint::duration>(delay);
  if (now > DistributedQuerySender::TimePoint::max() - converted)
    return DistributedQuerySender::TimePoint::max();
  return now + converted;
}

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_sender_request(const raft::NodeId source_node_id,
                      const query::DistributedAggregateFragmentDispatch& dispatch) noexcept {
  try {
    return encode_distributed_query_request_v1(
        {source_node_id, dispatch.fragment.serving_node, dispatch});
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed query sender allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("distributed query sender request exceeds limits"));
  }
}

} // namespace

common::Result<std::vector<std::byte>>
encode_distributed_query_request_v1(const DistributedQueryRequest& request) {
  if (request.source_node_id == 0U || request.target_node_id == 0U ||
      request.source_node_id == request.target_node_id) {
    return common::make_unexpected(invalid("distributed query request route is invalid"));
  }
  auto payload = query::encode_distributed_aggregate_fragment_dispatch(request.dispatch);
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  try {
    const std::size_t total = kDistributedQueryRequestHeaderSize + payload->bytes().size() +
                              kDistributedQueryRequestTrailerSize;
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kRequestMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kDistributedQueryRequestHeaderSize);
    if (status.is_ok())
      status = writer.write_u64_le(total);
    if (status.is_ok())
      status = writer.write_u64_le(request.source_node_id);
    if (status.is_ok())
      status = writer.write_u64_le(request.target_node_id);
    if (status.is_ok())
      status = writer.write_u64_le(payload->bytes().size());
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(payload->bytes()));
    if (status.is_ok())
      status = writer.zero_fill(24U);
    if (!status.is_ok() || writer.offset() != kRequestHeaderCrcOffset)
      return common::make_unexpected(invalid("distributed query request header is inconsistent"));
    status =
        writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kRequestHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.write_exact(payload->bytes());
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(invalid("distributed query request frame is inconsistent"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed query request allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("distributed query request exceeds limits"));
  }
}

common::Result<DistributedQueryRequest>
decode_distributed_query_request_v1(const common::ByteView bytes) {
  if (bytes.size() < kDistributedQueryRequestHeaderSize + kMinimumDispatchSize +
                         kDistributedQueryRequestTrailerSize ||
      bytes.size() > kMaximumDistributedQueryRequestSize) {
    return common::make_unexpected(corruption("distributed query request length is invalid"));
  }
  if (!std::ranges::equal(bytes.first(kRequestMagic.size()), kRequestMagic))
    return common::make_unexpected(corruption("distributed query request magic is invalid"));
  common::ByteReader crc_reader{bytes.subspan(kRequestHeaderCrcOffset, 4U)};
  const auto stored_header_crc = crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kRequestHeaderCrcOffset))) {
    return common::make_unexpected(corruption("distributed query request header checksum differs"));
  }
  common::ByteReader reader{bytes};
  if (!reader.skip(kRequestMagic.size()).is_ok())
    return common::make_unexpected(corruption("distributed query request header is truncated"));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto source = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto payload_length = reader.read_u64_le();
  const auto payload_crc = reader.read_u32_le();
  const auto reserved = reader.read_exact(24U);
  const auto header_crc = reader.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value() || !source.has_value() || !target.has_value() ||
      !payload_length.has_value() || !payload_crc.has_value() || !reserved.has_value() ||
      !header_crc.has_value()) {
    return common::make_unexpected(corruption("distributed query request header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("distributed query request version is unsupported"));
  if (*header_length != kDistributedQueryRequestHeaderSize || *total_length != bytes.size() ||
      *source == 0U || *target == 0U || *source == *target || !zero(*reserved) ||
      *payload_length < kMinimumDispatchSize ||
      *payload_length > query::distributed_fragment_dispatch_format::kMaximumFrameLength ||
      *payload_length !=
          bytes.size() - kDistributedQueryRequestHeaderSize - kDistributedQueryRequestTrailerSize) {
    return common::make_unexpected(corruption("distributed query request header is invalid"));
  }
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("distributed query request checksum differs"));
  const common::ByteView payload =
      bytes.subspan(kDistributedQueryRequestHeaderSize, static_cast<std::size_t>(*payload_length));
  if (*payload_crc != common::crc32c(payload))
    return common::make_unexpected(
        corruption("distributed query request payload checksum differs"));
  auto dispatch = query::decode_distributed_aggregate_fragment_dispatch_exact(payload);
  if (!dispatch.has_value())
    return common::make_unexpected(dispatch.error());
  return DistributedQueryRequest{*source, *target, std::move(*dispatch)};
}

common::Result<std::vector<std::byte>>
encode_distributed_query_response_v1(const DistributedQueryResponse& response) {
  if (response.source_node_id == 0U || response.target_node_id == 0U ||
      response.source_node_id == response.target_node_id || response.query_id.is_nil() ||
      response.tablet_id.uuid().is_nil() ||
      (response.status_code == common::StatusCode::kOk) != response.message.has_value()) {
    return common::make_unexpected(
        invalid("distributed query response identity or result is invalid"));
  }
  std::optional<query::EncodedExchangeMessage> payload;
  if (response.message.has_value()) {
    if (response.message->query_id != response.query_id ||
        response.message->tablet_id != response.tablet_id || response.message->sequence != 1U ||
        !response.message->terminal) {
      return common::make_unexpected(
          invalid("distributed query response message is not terminal or correlated"));
    }
    auto encoded = query::encode_exchange_message(*response.message);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    payload = std::move(*encoded);
  }
  auto status_code = encode_status(response.status_code);
  if (!status_code.has_value())
    return common::make_unexpected(status_code.error());
  std::uint8_t flags = 0U;
  std::uint64_t leader_node = 0U;
  std::uint64_t placement_epoch = 0U;
  if (response.leader_hint.has_value()) {
    if (response.leader_hint->node_id == 0U || response.leader_hint->placement_epoch == 0U)
      return common::make_unexpected(invalid("distributed query response leader hint is invalid"));
    flags |= kLeaderHintFlag;
    leader_node = response.leader_hint->node_id;
    placement_epoch = response.leader_hint->placement_epoch;
  }
  try {
    const std::size_t payload_length = payload.has_value() ? payload->bytes().size() : 0U;
    const std::size_t total =
        kDistributedQueryResponseHeaderSize + payload_length + kDistributedQueryResponseTrailerSize;
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kResponseMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kDistributedQueryResponseHeaderSize);
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
      write = writer.write_u8(*status_code);
    if (write.is_ok())
      write = writer.write_u8(flags);
    if (write.is_ok())
      write = writer.zero_fill(2U);
    if (write.is_ok())
      write = writer.write_u32_le(static_cast<std::uint32_t>(payload_length));
    if (write.is_ok())
      write = writer.write_u32_le(payload.has_value() ? common::crc32c(payload->bytes()) : 0U);
    if (write.is_ok())
      write = writer.zero_fill(4U);
    if (write.is_ok())
      write = writer.write_u64_le(leader_node);
    if (write.is_ok())
      write = writer.write_u64_le(placement_epoch);
    if (write.is_ok())
      write = writer.zero_fill(4U);
    if (!write.is_ok() || writer.offset() != kResponseHeaderCrcOffset)
      return common::make_unexpected(invalid("distributed query response header is inconsistent"));
    write = writer.write_u32_le(
        common::crc32c(common::ByteView{bytes}.first(kResponseHeaderCrcOffset)));
    if (write.is_ok() && payload.has_value())
      write = writer.write_exact(payload->bytes());
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("distributed query response frame is inconsistent"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed query response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("distributed query response exceeds limits"));
  }
}

common::Result<DistributedQueryResponse>
decode_distributed_query_response_v1(const common::ByteView bytes) {
  if ((bytes.size() != kDistributedQueryResponseHeaderSize + kDistributedQueryResponseTrailerSize &&
       bytes.size() != kMaximumDistributedQueryResponseSize)) {
    return common::make_unexpected(corruption("distributed query response length is invalid"));
  }
  if (!std::ranges::equal(bytes.first(kResponseMagic.size()), kResponseMagic))
    return common::make_unexpected(corruption("distributed query response magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kResponseHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kResponseHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("distributed query response header checksum differs"));
  }
  common::ByteReader reader{bytes};
  if (!reader.skip(kResponseMagic.size()).is_ok())
    return common::make_unexpected(corruption("distributed query response header is truncated"));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto source = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto query_id = read_uuid(reader);
  const auto tablet_id = read_tablet(reader);
  const auto status_byte = reader.read_u8();
  const auto flags = reader.read_u8();
  const auto reserved = reader.read_exact(2U);
  const auto payload_length = reader.read_u32_le();
  const auto payload_crc = reader.read_u32_le();
  const auto reserved_two = reader.read_exact(4U);
  const auto leader_node = reader.read_u64_le();
  const auto placement_epoch = reader.read_u64_le();
  const auto reserved_three = reader.read_exact(4U);
  const auto header_crc = reader.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value() || !source.has_value() || !target.has_value() ||
      !query_id.has_value() || !tablet_id.has_value() || !status_byte.has_value() ||
      !flags.has_value() || !reserved.has_value() || !payload_length.has_value() ||
      !payload_crc.has_value() || !reserved_two.has_value() || !leader_node.has_value() ||
      !placement_epoch.has_value() || !reserved_three.has_value() || !header_crc.has_value()) {
    return common::make_unexpected(corruption("distributed query response header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("distributed query response version is unsupported"));
  const auto status = decode_status(*status_byte);
  const bool has_hint = (*flags & kLeaderHintFlag) != 0U;
  const bool has_payload = *payload_length != 0U;
  if (*header_length != kDistributedQueryResponseHeaderSize || *total_length != bytes.size() ||
      *source == 0U || *target == 0U || *source == *target || query_id->is_nil() ||
      tablet_id->uuid().is_nil() || !status.has_value() || (*flags & ~kLeaderHintFlag) != 0U ||
      !zero(*reserved) || !zero(*reserved_two) || !zero(*reserved_three) ||
      (has_payload && *payload_length != query::distributed_format::kExchangeMessageLength) ||
      (!has_payload && *payload_crc != 0U) ||
      *payload_length != bytes.size() - kDistributedQueryResponseHeaderSize -
                             kDistributedQueryResponseTrailerSize ||
      ((*status == common::StatusCode::kOk) != has_payload) ||
      (has_hint && (*leader_node == 0U || *placement_epoch == 0U)) ||
      (!has_hint && (*leader_node != 0U || *placement_epoch != 0U))) {
    return common::make_unexpected(corruption("distributed query response header is invalid"));
  }
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("distributed query response checksum differs"));
  std::optional<query::ExchangeMessage> message;
  if (has_payload) {
    const common::ByteView payload = bytes.subspan(
        kDistributedQueryResponseHeaderSize, query::distributed_format::kExchangeMessageLength);
    if (*payload_crc != common::crc32c(payload))
      return common::make_unexpected(
          corruption("distributed query response payload checksum differs"));
    auto decoded = query::decode_exchange_message_exact(payload);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
    if (decoded->query_id != *query_id || decoded->tablet_id != *tablet_id ||
        decoded->sequence != 1U || !decoded->terminal) {
      return common::make_unexpected(
          corruption("distributed query response payload is not correlated"));
    }
    message = std::move(*decoded);
  }
  std::optional<DistributedQueryLeaderHint> leader_hint;
  if (has_hint)
    leader_hint = DistributedQueryLeaderHint{*leader_node, *placement_epoch};
  return DistributedQueryResponse{
      *source, *target, *query_id, *tablet_id, *status, std::move(message), leader_hint};
}

DistributedQueryReceiver::DistributedQueryReceiver(DistributedQueryReceiverConfig config) noexcept
    : config_(config) {}

common::Result<DistributedQueryReceiver>
DistributedQueryReceiver::create(const DistributedQueryReceiverConfig config) {
  if (config.local_node_id == 0U || config.authorizer == nullptr || config.worker == nullptr)
    return common::make_unexpected(invalid("distributed query receiver configuration is invalid"));
  return DistributedQueryReceiver{config};
}

common::Result<std::vector<std::byte>>
DistributedQueryReceiver::receive(const common::ByteView request_bytes,
                                  const network::PeerAuthenticationResult& authenticated_peer,
                                  std::optional<DistributedQueryLeaderHint> leader_hint) {
  if (!authenticated_peer.authorized || authenticated_peer.principal_id == 0U)
    return common::make_unexpected(
        unauthenticated("distributed query requires an authenticated principal"));
  auto request = decode_distributed_query_request_v1(request_bytes);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  auto authorized =
      config_.authorizer->authorize_node(authenticated_peer.principal_id, request->source_node_id);
  if (!authorized.has_value())
    return common::make_unexpected(authorized.error());
  if (!*authorized)
    return common::make_unexpected(
        unauthenticated("authenticated principal cannot claim query source node"));
  if (request->target_node_id != config_.local_node_id)
    return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                  "distributed query targets a different node"});
  const common::Uuid query_id = request->dispatch.fragment.query_id;
  const schema::TabletId tablet_id = request->dispatch.fragment.tablet_id;
  auto result = execute_worker(*config_.worker, request->dispatch);
  const common::StatusCode code =
      result.has_value() ? common::StatusCode::kOk : result.error().code();
  std::optional<query::ExchangeMessage> message;
  if (result.has_value())
    message = std::move(*result);
  return encode_distributed_query_response_v1({.source_node_id = config_.local_node_id,
                                               .target_node_id = request->source_node_id,
                                               .query_id = query_id,
                                               .tablet_id = tablet_id,
                                               .status_code = code,
                                               .message = std::move(message),
                                               .leader_hint = std::move(leader_hint)});
}

common::Result<DistributedQueryRequestReadStep>
DistributedQueryRequestReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  std::size_t consumed = 0U;
  if (!expected_frame_bytes_.has_value()) {
    const std::size_t header_bytes =
        std::min(bytes.size(), kDistributedQueryRequestHeaderSize - buffered_bytes_);
    std::ranges::copy(bytes.first(header_bytes),
                      bytes_.begin() + static_cast<std::ptrdiff_t>(buffered_bytes_));
    buffered_bytes_ += header_bytes;
    consumed += header_bytes;
    if (buffered_bytes_ != kDistributedQueryRequestHeaderSize)
      return DistributedQueryRequestReadStep{.consumed_bytes = consumed};
    auto length =
        request_frame_length(common::ByteView{bytes_}.first(kDistributedQueryRequestHeaderSize));
    if (!length.has_value()) {
      failure_.emplace(std::move(length).error());
      return common::make_unexpected(*failure_);
    }
    expected_frame_bytes_ = *length;
  }
  const std::size_t frame_bytes =
      std::min(bytes.size() - consumed, *expected_frame_bytes_ - buffered_bytes_);
  std::ranges::copy(bytes.subspan(consumed, frame_bytes),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(buffered_bytes_));
  buffered_bytes_ += frame_bytes;
  consumed += frame_bytes;
  if (buffered_bytes_ != *expected_frame_bytes_)
    return DistributedQueryRequestReadStep{.consumed_bytes = consumed};
  auto decoded =
      decode_distributed_query_request_v1(common::ByteView{bytes_}.first(*expected_frame_bytes_));
  if (!decoded.has_value()) {
    failure_.emplace(std::move(decoded).error());
    return common::make_unexpected(*failure_);
  }
  buffered_bytes_ = 0U;
  expected_frame_bytes_.reset();
  return DistributedQueryRequestReadStep{.consumed_bytes = consumed,
                                         .request = std::move(*decoded)};
}

std::size_t DistributedQueryRequestReader::buffered_bytes() const noexcept {
  return buffered_bytes_;
}

std::optional<std::size_t> DistributedQueryRequestReader::expected_frame_bytes() const noexcept {
  return expected_frame_bytes_;
}

bool DistributedQueryRequestReader::failed() const noexcept {
  return failure_.has_value();
}

common::Result<DistributedQueryResponseReadStep>
DistributedQueryResponseReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  std::size_t consumed = 0U;
  if (!expected_frame_bytes_.has_value()) {
    const std::size_t header_bytes =
        std::min(bytes.size(), kDistributedQueryResponseHeaderSize - buffered_bytes_);
    std::ranges::copy(bytes.first(header_bytes),
                      bytes_.begin() + static_cast<std::ptrdiff_t>(buffered_bytes_));
    buffered_bytes_ += header_bytes;
    consumed += header_bytes;
    if (buffered_bytes_ != kDistributedQueryResponseHeaderSize)
      return DistributedQueryResponseReadStep{.consumed_bytes = consumed};
    auto length =
        response_frame_length(common::ByteView{bytes_}.first(kDistributedQueryResponseHeaderSize));
    if (!length.has_value()) {
      failure_.emplace(std::move(length).error());
      return common::make_unexpected(*failure_);
    }
    expected_frame_bytes_ = *length;
  }
  const std::size_t frame_bytes =
      std::min(bytes.size() - consumed, *expected_frame_bytes_ - buffered_bytes_);
  std::ranges::copy(bytes.subspan(consumed, frame_bytes),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(buffered_bytes_));
  buffered_bytes_ += frame_bytes;
  consumed += frame_bytes;
  if (buffered_bytes_ != *expected_frame_bytes_)
    return DistributedQueryResponseReadStep{.consumed_bytes = consumed};
  auto decoded =
      decode_distributed_query_response_v1(common::ByteView{bytes_}.first(*expected_frame_bytes_));
  if (!decoded.has_value()) {
    failure_.emplace(std::move(decoded).error());
    return common::make_unexpected(*failure_);
  }
  buffered_bytes_ = 0U;
  expected_frame_bytes_.reset();
  return DistributedQueryResponseReadStep{.consumed_bytes = consumed,
                                          .response = std::move(*decoded)};
}

std::size_t DistributedQueryResponseReader::buffered_bytes() const noexcept {
  return buffered_bytes_;
}

std::optional<std::size_t> DistributedQueryResponseReader::expected_frame_bytes() const noexcept {
  return expected_frame_bytes_;
}

bool DistributedQueryResponseReader::failed() const noexcept {
  return failure_.has_value();
}

DistributedQueryFrameWriteCursor::DistributedQueryFrameWriteCursor(
    std::vector<std::byte> encoded_frame) noexcept
    : encoded_frame_(std::move(encoded_frame)) {}

DistributedQueryFrameWriteCursor::DistributedQueryFrameWriteCursor(
    DistributedQueryFrameWriteCursor&& other) noexcept
    : encoded_frame_(std::move(other.encoded_frame_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_frame_.size();
}

DistributedQueryFrameWriteCursor&
DistributedQueryFrameWriteCursor::operator=(DistributedQueryFrameWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_frame_ = std::move(other.encoded_frame_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_frame_.size();
  }
  return *this;
}

common::Result<DistributedQueryFrameWriteCursor>
DistributedQueryFrameWriteCursor::create(std::vector<std::byte> encoded_frame) {
  if (encoded_frame.size() < kRequestMagic.size())
    return common::make_unexpected(corruption("distributed query frame is truncated"));
  const common::ByteView bytes{encoded_frame};
  if (std::ranges::equal(bytes.first(kRequestMagic.size()), kRequestMagic)) {
    auto decoded = decode_distributed_query_request_v1(bytes);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
  } else if (std::ranges::equal(bytes.first(kResponseMagic.size()), kResponseMagic)) {
    auto decoded = decode_distributed_query_response_v1(bytes);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
  } else {
    return common::make_unexpected(corruption("distributed query frame magic is invalid"));
  }
  return DistributedQueryFrameWriteCursor{std::move(encoded_frame)};
}

common::ByteView DistributedQueryFrameWriteCursor::pending_write() const noexcept {
  return common::ByteView{encoded_frame_}.subspan(written_bytes_);
}

common::Status DistributedQueryFrameWriteCursor::consume_written(const std::size_t bytes) noexcept {
  if (bytes > encoded_frame_.size() - written_bytes_)
    return invalid("written byte count exceeds the distributed query frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t DistributedQueryFrameWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedQueryFrameWriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_frame_.size();
}

DistributedQuerySender::DistributedQuerySender(const raft::NodeId source_node_id,
                                               query::DistributedAggregateFragmentDispatch dispatch,
                                               const DistributedQueryRetryLimits limits) noexcept
    : source_node_id_(source_node_id), dispatch_(std::move(dispatch)), limits_(limits),
      next_backoff_(limits.initial_backoff) {}

common::Result<DistributedQuerySender>
DistributedQuerySender::create(const raft::NodeId source_node_id,
                               query::DistributedAggregateFragmentDispatch dispatch,
                               const DistributedQueryRetryLimits limits) {
  const auto maximum_supported_backoff =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  if (source_node_id == 0U || limits.maximum_attempts == 0U || limits.maximum_attempts > 1024U ||
      limits.initial_backoff.count() <= 0 || limits.maximum_backoff < limits.initial_backoff ||
      limits.maximum_backoff > maximum_supported_backoff ||
      dispatch.fragment.serving_node == source_node_id) {
    return common::make_unexpected(invalid("distributed query retry configuration is invalid"));
  }
  auto encoded = encode_sender_request(source_node_id, dispatch);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedQuerySender{source_node_id, std::move(dispatch), limits};
}

common::Result<DistributedQueryAttempt> DistributedQuerySender::begin_attempt(const TimePoint now) {
  if (state_ == DistributedQuerySenderState::kSucceeded ||
      state_ == DistributedQuerySenderState::kFailed) {
    return common::make_unexpected(invalid("distributed query sender is terminal"));
  }
  if (state_ == DistributedQuerySenderState::kWaitingForResponse)
    return common::make_unexpected(unavailable("distributed query response is pending"));
  if (state_ == DistributedQuerySenderState::kBackoff && now < *next_attempt_not_before_)
    return common::make_unexpected(unavailable("distributed query retry backoff is active"));
  if (attempts_started_ >= limits_.maximum_attempts)
    return common::make_unexpected(invalid("distributed query retry budget is exhausted"));
  auto bytes = encode_sender_request(source_node_id_, dispatch_);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  ++attempts_started_;
  state_ = DistributedQuerySenderState::kWaitingForResponse;
  suggested_leader_.reset();
  next_attempt_not_before_.reset();
  return DistributedQueryAttempt{attempts_started_, dispatch_.fragment.serving_node,
                                 std::move(*bytes)};
}

common::Status DistributedQuerySender::accept_response(const common::ByteView response_bytes,
                                                       const TimePoint now) {
  if (state_ != DistributedQuerySenderState::kWaitingForResponse)
    return invalid("distributed query sender has no pending response");
  auto response = decode_distributed_query_response_v1(response_bytes);
  if (!response.has_value())
    return response.error();
  if (response->source_node_id != dispatch_.fragment.serving_node ||
      response->target_node_id != source_node_id_ ||
      response->query_id != dispatch_.fragment.query_id ||
      response->tablet_id != dispatch_.fragment.tablet_id) {
    return invalid("distributed query response correlation mismatch");
  }
  suggested_leader_ = response->leader_hint;
  if (response->status_code == common::StatusCode::kOk) {
    last_status_code_ = common::StatusCode::kOk;
    result_ = std::move(response->message);
    state_ = DistributedQuerySenderState::kSucceeded;
    next_attempt_not_before_.reset();
    return common::Status::ok();
  }
  return schedule(response->status_code, now);
}

common::Status DistributedQuerySender::record_transport_failure(const common::StatusCode code,
                                                                const TimePoint now) {
  if (state_ != DistributedQuerySenderState::kWaitingForResponse)
    return invalid("distributed query sender has no active transport attempt");
  if (code == common::StatusCode::kOk)
    return invalid("distributed query transport failure cannot be OK");
  suggested_leader_.reset();
  return schedule(code, now);
}

common::Status DistributedQuerySender::schedule(const common::StatusCode code,
                                                const TimePoint now) {
  last_status_code_ = code;
  if (!retryable_status(code) || attempts_started_ >= limits_.maximum_attempts) {
    state_ = DistributedQuerySenderState::kFailed;
    next_attempt_not_before_.reset();
    return common::Status::ok();
  }
  state_ = DistributedQuerySenderState::kBackoff;
  next_attempt_not_before_ = saturating_add(now, next_backoff_);
  if (next_backoff_ < limits_.maximum_backoff) {
    const auto current = next_backoff_.count();
    const auto maximum = limits_.maximum_backoff.count();
    next_backoff_ = current > maximum / 2 ? limits_.maximum_backoff
                                          : std::min(next_backoff_ * 2, limits_.maximum_backoff);
  }
  return common::Status::ok();
}

DistributedQuerySenderState DistributedQuerySender::state() const noexcept {
  return state_;
}

std::size_t DistributedQuerySender::attempts_started() const noexcept {
  return attempts_started_;
}

std::optional<DistributedQuerySender::TimePoint>
DistributedQuerySender::next_attempt_not_before() const noexcept {
  return next_attempt_not_before_;
}

std::optional<common::StatusCode> DistributedQuerySender::last_status_code() const noexcept {
  return last_status_code_;
}

std::optional<DistributedQueryLeaderHint>
DistributedQuerySender::suggested_leader() const noexcept {
  return suggested_leader_;
}

const std::optional<query::ExchangeMessage>& DistributedQuerySender::result() const noexcept {
  return result_;
}

} // namespace chronos::cluster
