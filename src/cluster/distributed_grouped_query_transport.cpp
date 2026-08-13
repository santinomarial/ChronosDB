#include "chronos/cluster/distributed_grouped_query_transport.hpp"

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

inline constexpr std::array<std::byte, 8U> kRequestMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'G'},
    std::byte{'R'}, std::byte{'E'}, std::byte{'Q'}, std::byte{'1'}};
inline constexpr std::array<std::byte, 8U> kResponseMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'G'},
    std::byte{'R'}, std::byte{'S'}, std::byte{'P'}, std::byte{'1'}};
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kRequestHeaderCrcOffset = 76U;
inline constexpr std::size_t kResponseHeaderCrcOffset = 108U;
inline constexpr std::uint8_t kLeaderHintFlag = 1U << 0U;
inline constexpr std::uint8_t kNoPayload = 0U;
inline constexpr std::uint8_t kGroupedPartialPayload = 1U;
inline constexpr std::uint8_t kEmptyTerminalPayload = 2U;
inline constexpr std::size_t kMinimumDispatchSize =
    query::distributed_grouped_float64_fragment_dispatch_format::kHeaderLength +
    query::distributed_grouped_float64_fragment_format::kHeaderLength +
    query::distributed_fragment_format::kHeaderLength + sizeof(std::uint32_t) +
    query::distributed_fragment_format::kTrailerLength +
    query::distributed_grouped_float64_fragment_format::kTrailerLength +
    query::distributed_grouped_float64_fragment_dispatch_format::kTrailerLength;

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

[[nodiscard]] bool zero(const common::ByteView bytes) {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0U}; });
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
  return common::make_unexpected(invalid("grouped query response status is invalid"));
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
    return common::make_unexpected(corruption("grouped query response status is unknown"));
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

[[nodiscard]] bool valid_payload_shape(const std::uint8_t kind,
                                       const std::uint32_t length) noexcept {
  return (kind == kNoPayload && length == 0U) ||
         (kind == kGroupedPartialPayload &&
          length == query::grouped_float64_exchange_format::kFrameLength) ||
         (kind == kEmptyTerminalPayload &&
          length == query::grouped_exchange_terminal_format::kFrameLength);
}

[[nodiscard]] common::Result<std::size_t> request_frame_length(const common::ByteView header) {
  if (header.size() != kDistributedGroupedQueryRequestHeaderSize)
    return common::make_unexpected(corruption("grouped query request header is truncated"));
  if (!std::ranges::equal(header.first(kRequestMagic.size()), kRequestMagic))
    return common::make_unexpected(corruption("grouped query request magic is invalid"));
  common::ByteReader crc_reader{header.last(4U)};
  const auto stored_crc = crc_reader.read_u32_le();
  if (!stored_crc.has_value() ||
      *stored_crc != common::crc32c(header.first(kRequestHeaderCrcOffset))) {
    return common::make_unexpected(corruption("grouped query request header checksum differs"));
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
    return common::make_unexpected(corruption("grouped query request header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("grouped query request version is unsupported"));
  if (*header_length != kDistributedGroupedQueryRequestHeaderSize || *source == 0U ||
      *target == 0U || *source == *target || !zero(*reserved) ||
      *payload_length < kMinimumDispatchSize ||
      *payload_length >
          query::distributed_grouped_float64_fragment_dispatch_format::kMaximumFrameLength ||
      *total_length != kDistributedGroupedQueryRequestHeaderSize + *payload_length +
                           kDistributedGroupedQueryRequestTrailerSize ||
      *total_length > kMaximumDistributedGroupedQueryRequestSize) {
    return common::make_unexpected(corruption("grouped query request header is invalid"));
  }
  return static_cast<std::size_t>(*total_length);
}

[[nodiscard]] common::Result<std::size_t> response_frame_length(const common::ByteView header) {
  if (header.size() != kDistributedGroupedQueryResponseHeaderSize)
    return common::make_unexpected(corruption("grouped query response header is truncated"));
  if (!std::ranges::equal(header.first(kResponseMagic.size()), kResponseMagic))
    return common::make_unexpected(corruption("grouped query response magic is invalid"));
  common::ByteReader crc_reader{header.last(4U)};
  const auto stored_crc = crc_reader.read_u32_le();
  if (!stored_crc.has_value() ||
      *stored_crc != common::crc32c(header.first(kResponseHeaderCrcOffset))) {
    return common::make_unexpected(corruption("grouped query response header checksum differs"));
  }
  common::ByteReader reader{header.subspan(kResponseMagic.size())};
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value()) {
    return common::make_unexpected(corruption("grouped query response header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("grouped query response version is unsupported"));
  constexpr std::size_t kFailureLength =
      kDistributedGroupedQueryResponseHeaderSize + kDistributedGroupedQueryResponseTrailerSize;
  constexpr std::size_t kTerminalLength =
      kFailureLength + query::grouped_exchange_terminal_format::kFrameLength;
  if (*header_length != kDistributedGroupedQueryResponseHeaderSize ||
      (*total_length != kFailureLength && *total_length != kTerminalLength &&
       *total_length != kMaximumDistributedGroupedQueryResponseSize)) {
    return common::make_unexpected(corruption("grouped query response header is invalid"));
  }
  return static_cast<std::size_t>(*total_length);
}

[[nodiscard]] common::Result<query::DistributedGroupedFloat64WorkerResult>
execute_worker(DistributedGroupedQueryWorkerService& worker,
               const query::DistributedGroupedFloat64FragmentDispatch& dispatch) noexcept {
  try {
    return worker.execute(dispatch);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped query worker allocation failed"));
  } catch (...) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "grouped query worker threw"});
  }
}

} // namespace

common::Result<std::vector<std::byte>>
encode_distributed_grouped_query_request_v1(const DistributedGroupedQueryRequest& request) {
  if (request.source_node_id == 0U || request.target_node_id == 0U ||
      request.source_node_id == request.target_node_id) {
    return common::make_unexpected(invalid("grouped query request route is invalid"));
  }
  auto payload = query::encode_distributed_grouped_float64_fragment_dispatch(request.dispatch);
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  try {
    const std::size_t total = kDistributedGroupedQueryRequestHeaderSize + payload->bytes().size() +
                              kDistributedGroupedQueryRequestTrailerSize;
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kRequestMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kDistributedGroupedQueryRequestHeaderSize);
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
    if (!write.is_ok() || writer.offset() != kRequestHeaderCrcOffset)
      return common::make_unexpected(invalid("grouped query request header is inconsistent"));
    write =
        writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kRequestHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_exact(payload->bytes());
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("grouped query request frame is inconsistent"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped query request allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped query request exceeds limits"));
  }
}

common::Result<DistributedGroupedQueryRequest>
decode_distributed_grouped_query_request_v1(const common::ByteView bytes) {
  if (bytes.size() < kDistributedGroupedQueryRequestHeaderSize + kMinimumDispatchSize +
                         kDistributedGroupedQueryRequestTrailerSize ||
      bytes.size() > kMaximumDistributedGroupedQueryRequestSize) {
    return common::make_unexpected(corruption("grouped query request length is invalid"));
  }
  if (!std::ranges::equal(bytes.first(kRequestMagic.size()), kRequestMagic))
    return common::make_unexpected(corruption("grouped query request magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kRequestHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kRequestHeaderCrcOffset))) {
    return common::make_unexpected(corruption("grouped query request header checksum differs"));
  }
  common::ByteReader reader{bytes};
  if (!reader.skip(kRequestMagic.size()).is_ok())
    return common::make_unexpected(corruption("grouped query request header is truncated"));
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
    return common::make_unexpected(corruption("grouped query request header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("grouped query request version is unsupported"));
  if (*header_length != kDistributedGroupedQueryRequestHeaderSize ||
      *total_length != bytes.size() || *source == 0U || *target == 0U || *source == *target ||
      !zero(*reserved) || *payload_length < kMinimumDispatchSize ||
      *payload_length >
          query::distributed_grouped_float64_fragment_dispatch_format::kMaximumFrameLength ||
      *payload_length != bytes.size() - kDistributedGroupedQueryRequestHeaderSize -
                             kDistributedGroupedQueryRequestTrailerSize) {
    return common::make_unexpected(corruption("grouped query request header is invalid"));
  }
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("grouped query request checksum differs"));
  const common::ByteView payload = bytes.subspan(kDistributedGroupedQueryRequestHeaderSize,
                                                 static_cast<std::size_t>(*payload_length));
  if (*payload_crc != common::crc32c(payload))
    return common::make_unexpected(corruption("grouped query request payload checksum differs"));
  auto dispatch = query::decode_distributed_grouped_float64_fragment_dispatch_exact(payload);
  if (!dispatch.has_value())
    return common::make_unexpected(dispatch.error());
  return DistributedGroupedQueryRequest{*source, *target, std::move(*dispatch)};
}

common::Result<std::vector<std::byte>>
encode_distributed_grouped_query_response_v1(const DistributedGroupedQueryResponse& response) {
  if (response.source_node_id == 0U || response.target_node_id == 0U ||
      response.source_node_id == response.target_node_id || response.query_id.is_nil() ||
      response.tablet_id.uuid().is_nil() ||
      (response.status_code == common::StatusCode::kOk) != response.payload.has_value()) {
    return common::make_unexpected(invalid("grouped query response identity or result is invalid"));
  }
  std::vector<std::byte> payload_bytes;
  std::uint8_t payload_kind = kNoPayload;
  try {
    if (response.payload.has_value()) {
      if (const auto* partial =
              std::get_if<query::GroupedFloat64ExchangeMessage>(&*response.payload)) {
        if (partial->query_id != response.query_id || partial->tablet_id != response.tablet_id)
          return common::make_unexpected(invalid("grouped query partial is not correlated"));
        auto encoded = query::encode_grouped_float64_exchange_message(*partial);
        if (!encoded.has_value())
          return common::make_unexpected(encoded.error());
        payload_kind = kGroupedPartialPayload;
        payload_bytes.assign(encoded->bytes().begin(), encoded->bytes().end());
      } else {
        const auto& terminal = std::get<query::GroupedExchangeTerminalMessage>(*response.payload);
        if (terminal.query_id != response.query_id || terminal.tablet_id != response.tablet_id)
          return common::make_unexpected(invalid("grouped query terminal is not correlated"));
        auto encoded = query::encode_grouped_exchange_terminal_message(terminal);
        if (!encoded.has_value())
          return common::make_unexpected(encoded.error());
        payload_kind = kEmptyTerminalPayload;
        payload_bytes.assign(encoded->bytes().begin(), encoded->bytes().end());
      }
    }
    auto status_code = encode_status(response.status_code);
    if (!status_code.has_value())
      return common::make_unexpected(status_code.error());
    std::uint8_t flags = 0U;
    std::uint64_t leader_node = 0U;
    std::uint64_t placement_epoch = 0U;
    if (response.leader_hint.has_value()) {
      if (response.leader_hint->node_id == 0U || response.leader_hint->placement_epoch == 0U)
        return common::make_unexpected(invalid("grouped query leader hint is invalid"));
      flags |= kLeaderHintFlag;
      leader_node = response.leader_hint->node_id;
      placement_epoch = response.leader_hint->placement_epoch;
    }
    const std::size_t total = kDistributedGroupedQueryResponseHeaderSize + payload_bytes.size() +
                              kDistributedGroupedQueryResponseTrailerSize;
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kResponseMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kDistributedGroupedQueryResponseHeaderSize);
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
      write = writer.write_u8(payload_kind);
    if (write.is_ok())
      write = writer.write_u8(flags);
    if (write.is_ok())
      write = writer.zero_fill(1U);
    if (write.is_ok())
      write = writer.write_u32_le(static_cast<std::uint32_t>(payload_bytes.size()));
    if (write.is_ok())
      write = writer.write_u32_le(payload_bytes.empty() ? 0U : common::crc32c(payload_bytes));
    if (write.is_ok())
      write = writer.zero_fill(4U);
    if (write.is_ok())
      write = writer.write_u64_le(leader_node);
    if (write.is_ok())
      write = writer.write_u64_le(placement_epoch);
    if (write.is_ok())
      write = writer.zero_fill(4U);
    if (!write.is_ok() || writer.offset() != kResponseHeaderCrcOffset)
      return common::make_unexpected(invalid("grouped query response header is inconsistent"));
    write = writer.write_u32_le(
        common::crc32c(common::ByteView{bytes}.first(kResponseHeaderCrcOffset)));
    if (write.is_ok() && !payload_bytes.empty())
      write = writer.write_exact(payload_bytes);
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("grouped query response frame is inconsistent"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped query response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped query response exceeds limits"));
  }
}

common::Result<DistributedGroupedQueryResponse>
decode_distributed_grouped_query_response_v1(const common::ByteView bytes) {
  constexpr std::size_t kFailureLength =
      kDistributedGroupedQueryResponseHeaderSize + kDistributedGroupedQueryResponseTrailerSize;
  constexpr std::size_t kTerminalLength =
      kFailureLength + query::grouped_exchange_terminal_format::kFrameLength;
  if (bytes.size() != kFailureLength && bytes.size() != kTerminalLength &&
      bytes.size() != kMaximumDistributedGroupedQueryResponseSize) {
    return common::make_unexpected(corruption("grouped query response length is invalid"));
  }
  if (!std::ranges::equal(bytes.first(kResponseMagic.size()), kResponseMagic))
    return common::make_unexpected(corruption("grouped query response magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kResponseHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kResponseHeaderCrcOffset))) {
    return common::make_unexpected(corruption("grouped query response header checksum differs"));
  }
  common::ByteReader reader{bytes};
  if (!reader.skip(kResponseMagic.size()).is_ok())
    return common::make_unexpected(corruption("grouped query response header is truncated"));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto source = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto query_id = read_uuid(reader);
  const auto tablet_id = read_tablet(reader);
  const auto status_byte = reader.read_u8();
  const auto payload_kind = reader.read_u8();
  const auto flags = reader.read_u8();
  const auto reserved = reader.read_exact(1U);
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
      !payload_kind.has_value() || !flags.has_value() || !reserved.has_value() ||
      !payload_length.has_value() || !payload_crc.has_value() || !reserved_two.has_value() ||
      !leader_node.has_value() || !placement_epoch.has_value() || !reserved_three.has_value() ||
      !header_crc.has_value()) {
    return common::make_unexpected(corruption("grouped query response header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("grouped query response version is unsupported"));
  const auto status = decode_status(*status_byte);
  const bool has_payload = *payload_kind != kNoPayload;
  const bool has_hint = (*flags & kLeaderHintFlag) != 0U;
  if (*header_length != kDistributedGroupedQueryResponseHeaderSize ||
      *total_length != bytes.size() || *source == 0U || *target == 0U || *source == *target ||
      query_id->is_nil() || tablet_id->uuid().is_nil() || !status.has_value() ||
      !valid_payload_shape(*payload_kind, *payload_length) || (*flags & ~kLeaderHintFlag) != 0U ||
      !zero(*reserved) || !zero(*reserved_two) || !zero(*reserved_three) ||
      (!has_payload && *payload_crc != 0U) ||
      *payload_length != bytes.size() - kDistributedGroupedQueryResponseHeaderSize -
                             kDistributedGroupedQueryResponseTrailerSize ||
      ((*status == common::StatusCode::kOk) != has_payload) ||
      (has_hint && (*leader_node == 0U || *placement_epoch == 0U)) ||
      (!has_hint && (*leader_node != 0U || *placement_epoch != 0U))) {
    return common::make_unexpected(corruption("grouped query response header is invalid"));
  }
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("grouped query response checksum differs"));
  std::optional<DistributedGroupedQueryResponsePayload> payload;
  if (has_payload) {
    const common::ByteView payload_bytes = bytes.subspan(kDistributedGroupedQueryResponseHeaderSize,
                                                         static_cast<std::size_t>(*payload_length));
    if (*payload_crc != common::crc32c(payload_bytes))
      return common::make_unexpected(corruption("grouped query response payload checksum differs"));
    if (*payload_kind == kGroupedPartialPayload) {
      auto decoded = query::decode_grouped_float64_exchange_message_exact(payload_bytes);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      if (decoded->query_id != *query_id || decoded->tablet_id != *tablet_id)
        return common::make_unexpected(corruption("grouped query partial is not correlated"));
      payload = std::move(*decoded);
    } else {
      auto decoded = query::decode_grouped_exchange_terminal_message_exact(payload_bytes);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      if (decoded->query_id != *query_id || decoded->tablet_id != *tablet_id)
        return common::make_unexpected(corruption("grouped query terminal is not correlated"));
      payload = std::move(*decoded);
    }
  }
  std::optional<DistributedQueryLeaderHint> leader_hint;
  if (has_hint)
    leader_hint = DistributedQueryLeaderHint{*leader_node, *placement_epoch};
  return DistributedGroupedQueryResponse{
      *source, *target, *query_id, *tablet_id, *status, std::move(payload), leader_hint};
}

common::Result<DistributedGroupedQueryRequestReadStep>
DistributedGroupedQueryRequestReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  std::size_t consumed = 0U;
  if (!expected_frame_bytes_.has_value()) {
    const std::size_t header_bytes =
        std::min(bytes.size(), kDistributedGroupedQueryRequestHeaderSize - buffered_bytes_);
    std::ranges::copy(bytes.first(header_bytes),
                      bytes_.begin() + static_cast<std::ptrdiff_t>(buffered_bytes_));
    buffered_bytes_ += header_bytes;
    consumed += header_bytes;
    if (buffered_bytes_ != kDistributedGroupedQueryRequestHeaderSize)
      return DistributedGroupedQueryRequestReadStep{.consumed_bytes = consumed};
    auto length = request_frame_length(
        common::ByteView{bytes_}.first(kDistributedGroupedQueryRequestHeaderSize));
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
    return DistributedGroupedQueryRequestReadStep{.consumed_bytes = consumed};
  auto decoded = decode_distributed_grouped_query_request_v1(
      common::ByteView{bytes_}.first(*expected_frame_bytes_));
  if (!decoded.has_value()) {
    failure_.emplace(std::move(decoded).error());
    return common::make_unexpected(*failure_);
  }
  buffered_bytes_ = 0U;
  expected_frame_bytes_.reset();
  return DistributedGroupedQueryRequestReadStep{.consumed_bytes = consumed,
                                                .request = std::move(*decoded)};
}

std::size_t DistributedGroupedQueryRequestReader::buffered_bytes() const noexcept {
  return buffered_bytes_;
}

std::optional<std::size_t>
DistributedGroupedQueryRequestReader::expected_frame_bytes() const noexcept {
  return expected_frame_bytes_;
}

bool DistributedGroupedQueryRequestReader::failed() const noexcept {
  return failure_.has_value();
}

common::Result<DistributedGroupedQueryResponseReadStep>
DistributedGroupedQueryResponseReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  std::size_t consumed = 0U;
  if (!expected_frame_bytes_.has_value()) {
    const std::size_t header_bytes =
        std::min(bytes.size(), kDistributedGroupedQueryResponseHeaderSize - buffered_bytes_);
    std::ranges::copy(bytes.first(header_bytes),
                      bytes_.begin() + static_cast<std::ptrdiff_t>(buffered_bytes_));
    buffered_bytes_ += header_bytes;
    consumed += header_bytes;
    if (buffered_bytes_ != kDistributedGroupedQueryResponseHeaderSize)
      return DistributedGroupedQueryResponseReadStep{.consumed_bytes = consumed};
    auto length = response_frame_length(
        common::ByteView{bytes_}.first(kDistributedGroupedQueryResponseHeaderSize));
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
    return DistributedGroupedQueryResponseReadStep{.consumed_bytes = consumed};
  auto decoded = decode_distributed_grouped_query_response_v1(
      common::ByteView{bytes_}.first(*expected_frame_bytes_));
  if (!decoded.has_value()) {
    failure_.emplace(std::move(decoded).error());
    return common::make_unexpected(*failure_);
  }
  buffered_bytes_ = 0U;
  expected_frame_bytes_.reset();
  return DistributedGroupedQueryResponseReadStep{.consumed_bytes = consumed,
                                                 .response = std::move(*decoded)};
}

std::size_t DistributedGroupedQueryResponseReader::buffered_bytes() const noexcept {
  return buffered_bytes_;
}

std::optional<std::size_t>
DistributedGroupedQueryResponseReader::expected_frame_bytes() const noexcept {
  return expected_frame_bytes_;
}

bool DistributedGroupedQueryResponseReader::failed() const noexcept {
  return failure_.has_value();
}

DistributedGroupedQueryFrameWriteCursor::DistributedGroupedQueryFrameWriteCursor(
    std::vector<std::byte> encoded_frame) noexcept
    : encoded_frame_(std::move(encoded_frame)) {}

DistributedGroupedQueryFrameWriteCursor::DistributedGroupedQueryFrameWriteCursor(
    DistributedGroupedQueryFrameWriteCursor&& other) noexcept
    : encoded_frame_(std::move(other.encoded_frame_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_frame_.size();
}

DistributedGroupedQueryFrameWriteCursor& DistributedGroupedQueryFrameWriteCursor::operator=(
    DistributedGroupedQueryFrameWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_frame_ = std::move(other.encoded_frame_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_frame_.size();
  }
  return *this;
}

common::Result<DistributedGroupedQueryFrameWriteCursor>
DistributedGroupedQueryFrameWriteCursor::create(std::vector<std::byte> encoded_frame) {
  if (encoded_frame.size() < kRequestMagic.size())
    return common::make_unexpected(corruption("grouped query frame is truncated"));
  const common::ByteView bytes{encoded_frame};
  if (std::ranges::equal(bytes.first(kRequestMagic.size()), kRequestMagic)) {
    auto decoded = decode_distributed_grouped_query_request_v1(bytes);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
  } else if (std::ranges::equal(bytes.first(kResponseMagic.size()), kResponseMagic)) {
    auto decoded = decode_distributed_grouped_query_response_v1(bytes);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
  } else {
    return common::make_unexpected(corruption("grouped query frame magic is invalid"));
  }
  return DistributedGroupedQueryFrameWriteCursor{std::move(encoded_frame)};
}

common::ByteView DistributedGroupedQueryFrameWriteCursor::pending_write() const noexcept {
  return common::ByteView{encoded_frame_}.subspan(written_bytes_);
}

common::Status
DistributedGroupedQueryFrameWriteCursor::consume_written(const std::size_t bytes) noexcept {
  if (bytes > encoded_frame_.size() - written_bytes_)
    return invalid("written byte count exceeds the grouped query frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t DistributedGroupedQueryFrameWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedGroupedQueryFrameWriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_frame_.size();
}

DistributedGroupedQueryReceiver::DistributedGroupedQueryReceiver(
    DistributedGroupedQueryReceiverConfig config) noexcept
    : config_(config) {}

common::Result<DistributedGroupedQueryReceiver>
DistributedGroupedQueryReceiver::create(const DistributedGroupedQueryReceiverConfig config) {
  if (config.local_node_id == 0U || config.authorizer == nullptr || config.worker == nullptr ||
      config.maximum_response_frames == 0U ||
      config.maximum_response_frames > query::kMaximumDistributedCoordinatorMessages) {
    return common::make_unexpected(invalid("grouped query receiver configuration is invalid"));
  }
  return DistributedGroupedQueryReceiver{config};
}

common::Result<std::vector<std::vector<std::byte>>> DistributedGroupedQueryReceiver::receive(
    const common::ByteView request_bytes,
    const network::PeerAuthenticationResult& authenticated_peer) {
  if (!authenticated_peer.authorized || authenticated_peer.principal_id == 0U)
    return common::make_unexpected(
        unauthenticated("grouped query requires an authenticated principal"));
  auto request = decode_distributed_grouped_query_request_v1(request_bytes);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  auto authorized =
      config_.authorizer->authorize_node(authenticated_peer.principal_id, request->source_node_id);
  if (!authorized.has_value())
    return common::make_unexpected(authorized.error());
  if (!*authorized)
    return common::make_unexpected(
        unauthenticated("authenticated principal cannot claim grouped query source node"));
  if (request->target_node_id != config_.local_node_id) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "grouped query targets a different node"});
  }

  const auto& fragment = request->dispatch.fragment.aggregate;
  const common::Uuid query_id = fragment.query_id;
  const schema::TabletId tablet_id = fragment.tablet_id;
  auto result = execute_worker(*config_.worker, request->dispatch);
  if (!result.has_value()) {
    std::optional<DistributedQueryLeaderHint> leader_hint;
    if (result.error().code() == common::StatusCode::kUnavailable &&
        config_.leader_hint_provider != nullptr) {
      auto resolved = config_.leader_hint_provider->current_leader_hint(
          tablet_id, request->dispatch.raft_group_id);
      if (!resolved.has_value())
        return common::make_unexpected(resolved.error());
      leader_hint = std::move(*resolved);
    }
    auto encoded =
        encode_distributed_grouped_query_response_v1({.source_node_id = config_.local_node_id,
                                                      .target_node_id = request->source_node_id,
                                                      .query_id = query_id,
                                                      .tablet_id = tablet_id,
                                                      .status_code = result.error().code(),
                                                      .leader_hint = std::move(leader_hint)});
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    try {
      std::vector<std::vector<std::byte>> frames;
      frames.push_back(std::move(*encoded));
      return frames;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("grouped query response allocation failed"));
    }
  }

  try {
    std::vector<std::vector<std::byte>> frames;
    if (const auto* messages =
            std::get_if<std::vector<query::GroupedFloat64ExchangeMessage>>(&*result)) {
      if (messages->empty())
        return common::make_unexpected(invalid("grouped query worker returned an empty stream"));
      if (messages->size() > config_.maximum_response_frames) {
        auto encoded = encode_distributed_grouped_query_response_v1(
            {.source_node_id = config_.local_node_id,
             .target_node_id = request->source_node_id,
             .query_id = query_id,
             .tablet_id = tablet_id,
             .status_code = common::StatusCode::kResourceExhausted});
        if (!encoded.has_value())
          return common::make_unexpected(encoded.error());
        frames.push_back(std::move(*encoded));
        return frames;
      }
      frames.reserve(messages->size());
      for (std::size_t index = 0U; index < messages->size(); ++index) {
        const auto& message = (*messages)[index];
        const bool last = index + 1U == messages->size();
        if (message.query_id != query_id || message.tablet_id != tablet_id ||
            message.sequence != index + 1U || message.terminal != last) {
          return common::make_unexpected(
              invalid("grouped query worker stream is not correlated and contiguous"));
        }
        auto encoded = encode_distributed_grouped_query_response_v1(
            {.source_node_id = config_.local_node_id,
             .target_node_id = request->source_node_id,
             .query_id = query_id,
             .tablet_id = tablet_id,
             .status_code = common::StatusCode::kOk,
             .payload = DistributedGroupedQueryResponsePayload{message}});
        if (!encoded.has_value())
          return common::make_unexpected(encoded.error());
        frames.push_back(std::move(*encoded));
      }
    } else {
      const auto& terminal = std::get<query::GroupedExchangeTerminalMessage>(*result);
      if (terminal.query_id != query_id || terminal.tablet_id != tablet_id ||
          terminal.sequence != 1U) {
        return common::make_unexpected(invalid("grouped query worker terminal is not correlated"));
      }
      auto encoded = encode_distributed_grouped_query_response_v1(
          {.source_node_id = config_.local_node_id,
           .target_node_id = request->source_node_id,
           .query_id = query_id,
           .tablet_id = tablet_id,
           .status_code = common::StatusCode::kOk,
           .payload = DistributedGroupedQueryResponsePayload{terminal}});
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      frames.push_back(std::move(*encoded));
    }
    return frames;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped query response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped query response exceeds limits"));
  }
}

} // namespace chronos::cluster
