#include "chronos/cluster/distributed_vector_query_transport.hpp"

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
#include <vector>

namespace chronos::cluster {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'R'}, std::byte{'E'},
                                                  std::byte{'Q'}, std::byte{'1'}};
inline constexpr std::array<std::byte, 8U> kResponseMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'V'},
    std::byte{'R'}, std::byte{'S'}, std::byte{'P'}, std::byte{'1'}};
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderCrcOffset = 76U;
inline constexpr std::size_t kResponseHeaderCrcOffset = 108U;
inline constexpr std::uint8_t kLeaderHintFlag = 1U << 0U;
inline constexpr std::uint8_t kNoPayload = 0U;
inline constexpr std::uint8_t kVectorPayload = 1U;
inline constexpr std::size_t kMinimumDispatchSize =
    query::distributed_vector_fragment_format::kHeaderLength + sizeof(std::uint32_t) +
    query::distributed_vector_plan_format::kHeaderLength +
    query::distributed_vector_plan_format::kTrailerLength +
    query::distributed_vector_fragment_format::kTrailerLength;

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

[[nodiscard]] bool zero(const common::ByteView bytes) {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{}; });
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
  return common::make_unexpected(invalid("vector query response status is invalid"));
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
    return common::make_unexpected(corruption("vector query response status is unknown"));
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

[[nodiscard]] common::Result<std::size_t> request_frame_length(const common::ByteView header) {
  if (header.size() != kDistributedVectorQueryRequestHeaderSize ||
      !std::ranges::equal(header.first(kMagic.size()), kMagic)) {
    return common::make_unexpected(corruption("vector query request streaming header is invalid"));
  }
  common::ByteReader crc_reader{header.last(4U)};
  const auto stored_crc = crc_reader.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(header.first(kHeaderCrcOffset)))
    return common::make_unexpected(
        corruption("vector query request streaming header checksum differs"));
  common::ByteReader reader{header.subspan(kMagic.size())};
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto source = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto payload_length = reader.read_u64_le();
  static_cast<void>(reader.skip(4U));
  const auto reserved = reader.read_exact(24U);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value() || !source.has_value() || !target.has_value() ||
      !payload_length.has_value() || !reserved.has_value()) {
    return common::make_unexpected(
        corruption("vector query request streaming header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("vector query request version is unsupported"));
  if (*header_length != kDistributedVectorQueryRequestHeaderSize || *source == 0U ||
      *target == 0U || *source == *target || !zero(*reserved) ||
      *payload_length < kMinimumDispatchSize ||
      *payload_length > query::distributed_vector_fragment_format::kMaximumFrameLength ||
      *total_length != kDistributedVectorQueryRequestHeaderSize + *payload_length +
                           kDistributedVectorQueryRequestTrailerSize ||
      *total_length > kMaximumDistributedVectorQueryRequestSize) {
    return common::make_unexpected(corruption("vector query request streaming header is invalid"));
  }
  return static_cast<std::size_t>(*total_length);
}

[[nodiscard]] common::Result<std::size_t> response_frame_length(const common::ByteView header) {
  if (header.size() != kDistributedVectorQueryResponseHeaderSize ||
      !std::ranges::equal(header.first(kResponseMagic.size()), kResponseMagic)) {
    return common::make_unexpected(corruption("vector query response streaming header is invalid"));
  }
  common::ByteReader crc_reader{header.last(4U)};
  const auto stored_crc = crc_reader.read_u32_le();
  if (!stored_crc.has_value() ||
      *stored_crc != common::crc32c(header.first(kResponseHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("vector query response streaming header checksum differs"));
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
        corruption("vector query response streaming header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("vector query response version is unsupported"));
  const auto status = decode_status(*status_code);
  if (!status.has_value())
    return common::make_unexpected(status.error());
  const bool payload_present = *payload_kind == kVectorPayload;
  const bool hint_present = (*flags & kLeaderHintFlag) != 0U;
  if (*header_length != kDistributedVectorQueryResponseHeaderSize || *source == 0U ||
      *target == 0U || *source == *target || query_id->is_nil() || tablet_id->uuid().is_nil() ||
      (*flags & ~kLeaderHintFlag) != 0U || *small_reserved != 0U || *reserved != 0U ||
      *trailing_reserved != 0U ||
      (*payload_kind != kNoPayload && *payload_kind != kVectorPayload) ||
      (*status == common::StatusCode::kOk) != payload_present ||
      (payload_present &&
       (*payload_length < query::distributed_vector_exchange_format::kHeaderLength +
                              query::distributed_vector_exchange_format::kTrailerLength ||
        *payload_length > query::distributed_vector_exchange_format::kMaximumFrameLength)) ||
      (!payload_present && (*payload_length != 0U || *payload_crc != 0U)) ||
      *total_length != kDistributedVectorQueryResponseHeaderSize + *payload_length +
                           kDistributedVectorQueryResponseTrailerSize ||
      *total_length > kMaximumDistributedVectorQueryResponseSize ||
      (hint_present && (*leader_node == 0U || *leader_epoch == 0U)) ||
      (!hint_present && (*leader_node != 0U || *leader_epoch != 0U))) {
    return common::make_unexpected(corruption("vector query response streaming header is invalid"));
  }
  return static_cast<std::size_t>(*total_length);
}

} // namespace

common::Result<std::vector<std::byte>>
encode_distributed_vector_query_request_v1(const DistributedVectorQueryRequest& request) {
  if (request.source_node_id == 0U || request.target_node_id == 0U ||
      request.source_node_id == request.target_node_id) {
    return common::make_unexpected(invalid("vector query request route is invalid"));
  }
  auto payload = query::encode_distributed_vector_fragment_dispatch(request.dispatch);
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  try {
    const std::size_t total = kDistributedVectorQueryRequestHeaderSize + payload->bytes().size() +
                              kDistributedVectorQueryRequestTrailerSize;
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kDistributedVectorQueryRequestHeaderSize);
    if (write.is_ok())
      write = writer.write_u64_le(total);
    if (write.is_ok())
      write = writer.write_u64_le(request.source_node_id);
    if (write.is_ok())
      write = writer.write_u64_le(request.target_node_id);
    if (write.is_ok())
      write = writer.write_u64_le(payload->bytes().size());
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(payload->bytes()));
    if (write.is_ok())
      write = writer.zero_fill(24U);
    if (!write.is_ok() || writer.offset() != kHeaderCrcOffset)
      return common::make_unexpected(invalid("vector query request header is inconsistent"));
    write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_exact(payload->bytes());
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("vector query request frame is inconsistent"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector query request allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("vector query request exceeds container limits"));
  }
}

common::Result<DistributedVectorQueryRequest>
decode_distributed_vector_query_request_v1(const common::ByteView bytes) {
  if (bytes.size() < kDistributedVectorQueryRequestHeaderSize + kMinimumDispatchSize +
                         kDistributedVectorQueryRequestTrailerSize ||
      bytes.size() > kMaximumDistributedVectorQueryRequestSize) {
    return common::make_unexpected(corruption("vector query request length is invalid"));
  }
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("vector query request magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kHeaderCrcOffset))) {
    return common::make_unexpected(corruption("vector query request header checksum differs"));
  }

  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kMagic.size()));
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
    return common::make_unexpected(corruption("vector query request header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("vector query request version is unsupported"));
  if (*header_length != kDistributedVectorQueryRequestHeaderSize || *total_length != bytes.size() ||
      *source == 0U || *target == 0U || *source == *target || !zero(*reserved) ||
      *payload_length < kMinimumDispatchSize ||
      *payload_length > query::distributed_vector_fragment_format::kMaximumFrameLength ||
      *payload_length != bytes.size() - kDistributedVectorQueryRequestHeaderSize -
                             kDistributedVectorQueryRequestTrailerSize) {
    return common::make_unexpected(corruption("vector query request header is invalid"));
  }
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("vector query request checksum differs"));
  const common::ByteView payload = bytes.subspan(kDistributedVectorQueryRequestHeaderSize,
                                                 static_cast<std::size_t>(*payload_length));
  if (*payload_crc != common::crc32c(payload))
    return common::make_unexpected(corruption("vector query request payload checksum differs"));
  auto dispatch = query::decode_distributed_vector_fragment_dispatch_exact(payload);
  if (!dispatch.has_value())
    return common::make_unexpected(dispatch.error());
  return DistributedVectorQueryRequest{*source, *target, std::move(*dispatch)};
}

common::Result<std::vector<std::byte>>
encode_distributed_vector_query_response_v1(const DistributedVectorQueryResponse& response) {
  if (response.source_node_id == 0U || response.target_node_id == 0U ||
      response.source_node_id == response.target_node_id || response.query_id.is_nil() ||
      response.tablet_id.uuid().is_nil() ||
      (response.status_code == common::StatusCode::kOk) != response.payload.has_value()) {
    return common::make_unexpected(invalid("vector query response identity or result is invalid"));
  }
  const auto status = encode_status(response.status_code);
  if (!status.has_value())
    return common::make_unexpected(status.error());
  std::vector<std::byte> payload_bytes;
  if (response.payload.has_value()) {
    if (response.payload->query_id != response.query_id ||
        response.payload->tablet_id != response.tablet_id) {
      return common::make_unexpected(invalid("vector query response payload is not correlated"));
    }
    auto encoded = query::encode_distributed_vector_exchange_message(*response.payload);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    try {
      payload_bytes.assign(encoded->bytes().begin(), encoded->bytes().end());
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("vector query response payload allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("vector query response payload exceeds limits"));
    }
  }
  if (response.leader_hint.has_value() &&
      (response.leader_hint->node_id == 0U || response.leader_hint->placement_epoch == 0U)) {
    return common::make_unexpected(invalid("vector query response leader hint is invalid"));
  }
  try {
    const std::size_t total = kDistributedVectorQueryResponseHeaderSize + payload_bytes.size() +
                              kDistributedVectorQueryResponseTrailerSize;
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kResponseMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kDistributedVectorQueryResponseHeaderSize);
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
      write = writer.write_u8(response.payload.has_value() ? kVectorPayload : kNoPayload);
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
    if (!write.is_ok() || writer.offset() != kResponseHeaderCrcOffset)
      return common::make_unexpected(invalid("vector query response header is inconsistent"));
    write = writer.write_u32_le(
        common::crc32c(common::ByteView{bytes}.first(kResponseHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_exact(payload_bytes);
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("vector query response frame is inconsistent"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector query response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("vector query response exceeds container limits"));
  }
}

common::Result<DistributedVectorQueryResponse>
decode_distributed_vector_query_response_v1(const common::ByteView bytes) {
  if (bytes.size() <
          kDistributedVectorQueryResponseHeaderSize + kDistributedVectorQueryResponseTrailerSize ||
      bytes.size() > kMaximumDistributedVectorQueryResponseSize) {
    return common::make_unexpected(corruption("vector query response length is invalid"));
  }
  if (!std::ranges::equal(bytes.first(kResponseMagic.size()), kResponseMagic))
    return common::make_unexpected(corruption("vector query response magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kResponseHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kResponseHeaderCrcOffset))) {
    return common::make_unexpected(corruption("vector query response header checksum differs"));
  }

  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kResponseMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto source = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  auto query_id = read_uuid(reader);
  auto tablet_id = read_tablet(reader);
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
  const auto header_crc = reader.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value() || !source.has_value() || !target.has_value() ||
      !query_id.has_value() || !tablet_id.has_value() || !status_code.has_value() ||
      !payload_kind.has_value() || !flags.has_value() || !small_reserved.has_value() ||
      !payload_length.has_value() || !payload_crc.has_value() || !reserved.has_value() ||
      !leader_node.has_value() || !leader_epoch.has_value() || !trailing_reserved.has_value() ||
      !header_crc.has_value()) {
    return common::make_unexpected(corruption("vector query response header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("vector query response version is unsupported"));
  auto status = decode_status(*status_code);
  if (!status.has_value())
    return common::make_unexpected(status.error());
  const bool payload_present = *payload_kind == kVectorPayload;
  const bool hint_present = (*flags & kLeaderHintFlag) != 0U;
  if (*header_length != kDistributedVectorQueryResponseHeaderSize ||
      *total_length != bytes.size() || *source == 0U || *target == 0U || *source == *target ||
      query_id->is_nil() || tablet_id->uuid().is_nil() || (*flags & ~kLeaderHintFlag) != 0U ||
      *small_reserved != 0U || *reserved != 0U || *trailing_reserved != 0U ||
      (*payload_kind != kNoPayload && *payload_kind != kVectorPayload) ||
      (*status == common::StatusCode::kOk) != payload_present ||
      (payload_present &&
       (*payload_length < query::distributed_vector_exchange_format::kHeaderLength +
                              query::distributed_vector_exchange_format::kTrailerLength ||
        *payload_length > query::distributed_vector_exchange_format::kMaximumFrameLength)) ||
      (!payload_present && (*payload_length != 0U || *payload_crc != 0U)) ||
      *payload_length != bytes.size() - kDistributedVectorQueryResponseHeaderSize -
                             kDistributedVectorQueryResponseTrailerSize ||
      (hint_present && (*leader_node == 0U || *leader_epoch == 0U)) ||
      (!hint_present && (*leader_node != 0U || *leader_epoch != 0U))) {
    return common::make_unexpected(corruption("vector query response header is invalid"));
  }
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("vector query response checksum differs"));
  const common::ByteView payload = bytes.subspan(kDistributedVectorQueryResponseHeaderSize,
                                                 static_cast<std::size_t>(*payload_length));
  if (payload_present && *payload_crc != common::crc32c(payload))
    return common::make_unexpected(corruption("vector query response payload checksum differs"));

  std::optional<query::DistributedVectorExchangeMessage> decoded_payload;
  if (payload_present) {
    auto decoded = query::decode_distributed_vector_exchange_message_exact(payload);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
    if (decoded->query_id != *query_id || decoded->tablet_id != *tablet_id)
      return common::make_unexpected(corruption("vector query response payload is not correlated"));
    decoded_payload = std::move(*decoded);
  }
  std::optional<DistributedQueryLeaderHint> hint;
  if (hint_present)
    hint = DistributedQueryLeaderHint{*leader_node, *leader_epoch};
  return DistributedVectorQueryResponse{.source_node_id = *source,
                                        .target_node_id = *target,
                                        .query_id = *query_id,
                                        .tablet_id = *tablet_id,
                                        .status_code = *status,
                                        .payload = std::move(decoded_payload),
                                        .leader_hint = hint};
}

DistributedVectorQueryRequestReader::DistributedVectorQueryRequestReader(
    const std::size_t maximum_frame_length)
    : maximum_frame_length_(maximum_frame_length) {}

common::Result<DistributedVectorQueryRequestReadStep>
DistributedVectorQueryRequestReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  constexpr std::size_t kMinimum = kDistributedVectorQueryRequestHeaderSize + kMinimumDispatchSize +
                                   kDistributedVectorQueryRequestTrailerSize;
  if (maximum_frame_length_ < kMinimum ||
      maximum_frame_length_ > kMaximumDistributedVectorQueryRequestSize) {
    return common::make_unexpected(invalid("vector query request reader limit is invalid"));
  }
  std::size_t consumed{};
  if (frame_.empty()) {
    const std::size_t copied = std::min(bytes.size(), header_.size() - header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != header_.size())
      return DistributedVectorQueryRequestReadStep{.consumed_bytes = consumed};
    const auto frame_length = request_frame_length(header_);
    if (!frame_length.has_value()) {
      failure_ = frame_length.error();
      return common::make_unexpected(*failure_);
    }
    if (*frame_length > maximum_frame_length_) {
      failure_ = exhausted("vector query request exceeds reader frame limit");
      return common::make_unexpected(*failure_);
    }
    try {
      frame_.resize(*frame_length);
    } catch (const std::bad_alloc&) {
      failure_ = exhausted("vector query request reader allocation failed");
      return common::make_unexpected(*failure_);
    } catch (const std::length_error&) {
      failure_ = exhausted("vector query request reader exceeds container limits");
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
  if (frame_bytes_ != frame_.size())
    return DistributedVectorQueryRequestReadStep{.consumed_bytes = consumed};
  auto decoded = decode_distributed_vector_query_request_v1(frame_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  DistributedVectorQueryRequest result = std::move(*decoded);
  frame_.clear();
  frame_bytes_ = 0U;
  header_bytes_ = 0U;
  return DistributedVectorQueryRequestReadStep{.consumed_bytes = consumed,
                                               .request = std::move(result)};
}

std::size_t DistributedVectorQueryRequestReader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool DistributedVectorQueryRequestReader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorQueryResponseReader::DistributedVectorQueryResponseReader(
    const std::size_t maximum_frame_length)
    : maximum_frame_length_(maximum_frame_length) {}

common::Result<DistributedVectorQueryResponseReadStep>
DistributedVectorQueryResponseReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  constexpr std::size_t kMinimum =
      kDistributedVectorQueryResponseHeaderSize + kDistributedVectorQueryResponseTrailerSize;
  if (maximum_frame_length_ < kMinimum ||
      maximum_frame_length_ > kMaximumDistributedVectorQueryResponseSize) {
    return common::make_unexpected(invalid("vector query response reader limit is invalid"));
  }
  std::size_t consumed{};
  if (frame_.empty()) {
    const std::size_t copied = std::min(bytes.size(), header_.size() - header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != header_.size())
      return DistributedVectorQueryResponseReadStep{.consumed_bytes = consumed};
    const auto frame_length = response_frame_length(header_);
    if (!frame_length.has_value()) {
      failure_ = frame_length.error();
      return common::make_unexpected(*failure_);
    }
    if (*frame_length > maximum_frame_length_) {
      failure_ = exhausted("vector query response exceeds reader frame limit");
      return common::make_unexpected(*failure_);
    }
    try {
      frame_.resize(*frame_length);
    } catch (const std::bad_alloc&) {
      failure_ = exhausted("vector query response reader allocation failed");
      return common::make_unexpected(*failure_);
    } catch (const std::length_error&) {
      failure_ = exhausted("vector query response reader exceeds container limits");
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
  if (frame_bytes_ != frame_.size())
    return DistributedVectorQueryResponseReadStep{.consumed_bytes = consumed};
  auto decoded = decode_distributed_vector_query_response_v1(frame_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  DistributedVectorQueryResponse result = std::move(*decoded);
  frame_.clear();
  frame_bytes_ = 0U;
  header_bytes_ = 0U;
  return DistributedVectorQueryResponseReadStep{.consumed_bytes = consumed,
                                                .response = std::move(result)};
}

std::size_t DistributedVectorQueryResponseReader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool DistributedVectorQueryResponseReader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorQueryFrameWriteCursor::DistributedVectorQueryFrameWriteCursor(
    std::vector<std::byte> encoded_frame) noexcept
    : encoded_frame_(std::move(encoded_frame)) {}

DistributedVectorQueryFrameWriteCursor::DistributedVectorQueryFrameWriteCursor(
    DistributedVectorQueryFrameWriteCursor&& other) noexcept
    : encoded_frame_(std::move(other.encoded_frame_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_frame_.size();
}

DistributedVectorQueryFrameWriteCursor& DistributedVectorQueryFrameWriteCursor::operator=(
    DistributedVectorQueryFrameWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_frame_ = std::move(other.encoded_frame_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_frame_.size();
  }
  return *this;
}

common::Result<DistributedVectorQueryFrameWriteCursor>
DistributedVectorQueryFrameWriteCursor::create(std::vector<std::byte> encoded_frame) {
  if (encoded_frame.size() < kMagic.size())
    return common::make_unexpected(invalid("vector query write frame is invalid"));
  if (std::ranges::equal(common::ByteView{encoded_frame}.first(kMagic.size()), kMagic)) {
    auto decoded = decode_distributed_vector_query_request_v1(encoded_frame);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
  } else if (std::ranges::equal(common::ByteView{encoded_frame}.first(kResponseMagic.size()),
                                kResponseMagic)) {
    auto decoded = decode_distributed_vector_query_response_v1(encoded_frame);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
  } else {
    return common::make_unexpected(invalid("vector query write frame magic is invalid"));
  }
  return DistributedVectorQueryFrameWriteCursor{std::move(encoded_frame)};
}

common::ByteView DistributedVectorQueryFrameWriteCursor::pending_write() const noexcept {
  return common::ByteView{encoded_frame_}.subspan(written_bytes_);
}

common::Status
DistributedVectorQueryFrameWriteCursor::consume_written(const std::size_t bytes) noexcept {
  if (bytes > encoded_frame_.size() - written_bytes_)
    return invalid("written byte count exceeds vector query frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t DistributedVectorQueryFrameWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedVectorQueryFrameWriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_frame_.size();
}

} // namespace chronos::cluster
