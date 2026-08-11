#include "chronos/cluster/distributed_query_transport.hpp"

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

[[nodiscard]] common::Result<std::uint8_t> encode_status(const common::StatusCode code) {
  const auto numeric = static_cast<std::uint8_t>(code);
  if (code < common::StatusCode::kOk || code > common::StatusCode::kInternal)
    return common::make_unexpected(invalid("distributed query response status is invalid"));
  return numeric;
}

[[nodiscard]] common::Result<common::StatusCode> decode_status(const std::uint8_t code) {
  if (code > static_cast<std::uint8_t>(common::StatusCode::kInternal))
    return common::make_unexpected(corruption("distributed query response status is unknown"));
  return static_cast<common::StatusCode>(code);
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

} // namespace chronos::cluster
