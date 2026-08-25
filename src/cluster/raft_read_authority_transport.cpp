#include "chronos/cluster/raft_read_authority_transport.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
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

constexpr std::array<std::byte, 8U> kRequestMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                                  std::byte{'R'}, std::byte{'A'}, std::byte{'U'},
                                                  std::byte{'Q'}, std::byte{'1'}};
constexpr std::array<std::byte, 8U> kResponseMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                                   std::byte{'R'}, std::byte{'A'}, std::byte{'U'},
                                                   std::byte{'R'}, std::byte{'1'}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kRequestHeaderCrcOffset = 76U;
constexpr std::size_t kResponseHeaderCrcOffset = 124U;

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

[[nodiscard]] bool valid_limits(const RaftReadAuthorityTransportLimits& limits) noexcept {
  return limits.observation.maximum_voters_per_set != 0U &&
         limits.observation.maximum_voters_per_set <= kMaximumRaftObservationVotersPerSet;
}

[[nodiscard]] bool zero(const common::ByteView bytes) noexcept {
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
  return common::make_unexpected(invalid("Raft read-authority response status is invalid"));
}

[[nodiscard]] common::Result<common::StatusCode> decode_status(const std::uint8_t raw) {
  switch (raw) {
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
    return common::make_unexpected(corruption("Raft read-authority response status is unknown"));
  }
}

[[nodiscard]] common::Result<raft::GroupId> read_group(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  return raft::GroupId{owned};
}

[[nodiscard]] common::Status
validate_authority(const RaftReadAuthority& authority, const raft::NodeId source_node_id,
                   const raft::GroupId& group_id,
                   const RaftReadAuthorityTransportLimits& limits) noexcept {
  const raft::GroupReadBarrier& barrier = authority.barrier;
  const raft::RaftGroupObservation& observation = authority.observation;
  if (!valid_limits(limits) || barrier.group_id != group_id || observation.group_id != group_id ||
      group_id.is_nil() || barrier.barrier.term == 0U || barrier.barrier.context == 0U ||
      barrier.barrier.read_index == 0U || observation.node_id != source_node_id ||
      observation.role != raft::Role::kLeader || observation.leader_id != source_node_id ||
      observation.current_term != barrier.barrier.term ||
      observation.last_log_index < observation.commit_index ||
      observation.commit_index < observation.applied_index ||
      observation.commit_index < barrier.barrier.read_index || observation.voters.empty() ||
      observation.voters.size() > limits.observation.maximum_voters_per_set ||
      observation.voters != observation.committed_voters ||
      !std::ranges::is_sorted(observation.voters) || observation.voters.front() == 0U ||
      std::ranges::adjacent_find(observation.voters) != observation.voters.end() ||
      !std::ranges::binary_search(observation.voters, source_node_id) ||
      observation.joint_membership_active || observation.joint_membership_can_finalize ||
      observation.final_membership_pending || !observation.joint_old_voters.empty() ||
      !observation.joint_new_voters.empty()) {
    return invalid("Raft read authority is not a stable correlated leader barrier");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<RaftReadAuthority>
acquire_service(RaftReadAuthorityService& service, const raft::GroupId& group_id) noexcept {
  try {
    return service.acquire(group_id);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft read-authority service allocation failed"));
  } catch (...) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "Raft read-authority service threw"});
  }
}

} // namespace

common::Result<std::vector<std::byte>>
encode_raft_read_authority_request_v1(const RaftReadAuthorityRequest& request) {
  if (request.source_node_id == 0U || request.target_node_id == 0U ||
      request.source_node_id == request.target_node_id || request.group_id.is_nil() ||
      request.correlation_id == 0U) {
    return common::make_unexpected(invalid("Raft read-authority request identity is invalid"));
  }
  try {
    std::vector<std::byte> bytes(kRaftReadAuthorityRequestSize);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kRequestMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kRaftReadAuthorityRequestHeaderSize);
    if (status.is_ok())
      status = writer.write_u64_le(kRaftReadAuthorityRequestSize);
    if (status.is_ok())
      status = writer.write_u64_le(request.source_node_id);
    if (status.is_ok())
      status = writer.write_u64_le(request.target_node_id);
    if (status.is_ok())
      status = writer.write_exact(request.group_id.bytes());
    if (status.is_ok())
      status = writer.write_u64_le(request.correlation_id);
    if (status.is_ok())
      status = writer.zero_fill(12U);
    if (status.is_ok())
      status = writer.write_u32_le(
          common::crc32c(common::ByteView{bytes}.first(kRequestHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.write_u32_le(
          common::crc32c(common::ByteView{bytes}.first(kRaftReadAuthorityRequestHeaderSize)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(invalid("Raft read-authority request encoding failed"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft read-authority request allocation failed"));
  }
}

common::Result<RaftReadAuthorityRequest>
decode_raft_read_authority_request_v1(const common::ByteView bytes) {
  if (bytes.size() != kRaftReadAuthorityRequestSize)
    return common::make_unexpected(corruption("Raft read-authority request length is invalid"));
  if (!std::ranges::equal(bytes.first(kRequestMagic.size()), kRequestMagic))
    return common::make_unexpected(corruption("Raft read-authority request magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kRequestHeaderCrcOffset, 4U)};
  common::ByteReader frame_crc_reader{bytes.last(4U)};
  auto header_crc = header_crc_reader.read_u32_le();
  auto frame_crc = frame_crc_reader.read_u32_le();
  if (!header_crc.has_value() ||
      *header_crc != common::crc32c(bytes.first(kRequestHeaderCrcOffset)) ||
      !frame_crc.has_value() ||
      *frame_crc != common::crc32c(bytes.first(kRaftReadAuthorityRequestHeaderSize))) {
    return common::make_unexpected(corruption("Raft read-authority request checksum differs"));
  }
  common::ByteReader reader{bytes};
  if (!reader.skip(kRequestMagic.size()).is_ok())
    return common::make_unexpected(corruption("Raft read-authority request is truncated"));
  auto major = reader.read_u16_le();
  auto minor = reader.read_u16_le();
  auto header_size = reader.read_u32_le();
  auto total_size = reader.read_u64_le();
  auto source = reader.read_u64_le();
  auto target = reader.read_u64_le();
  auto group = read_group(reader);
  auto correlation = reader.read_u64_le();
  auto reserved = reader.read_exact(12U);
  if (!major.has_value() || !minor.has_value() || !header_size.has_value() ||
      !total_size.has_value() || !source.has_value() || !target.has_value() || !group.has_value() ||
      !correlation.has_value() || !reserved.has_value()) {
    return common::make_unexpected(corruption("Raft read-authority request is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("Raft read-authority request version is unsupported"));
  if (*header_size != kRaftReadAuthorityRequestHeaderSize ||
      *total_size != kRaftReadAuthorityRequestSize || *source == 0U || *target == 0U ||
      *source == *target || group->is_nil() || *correlation == 0U || !zero(*reserved)) {
    return common::make_unexpected(corruption("Raft read-authority request semantics are invalid"));
  }
  return RaftReadAuthorityRequest{*source, *target, *group, *correlation};
}

common::Result<std::vector<std::byte>>
encode_raft_read_authority_response_v1(const RaftReadAuthorityResponse& response,
                                       const RaftReadAuthorityTransportLimits limits) {
  if (!valid_limits(limits) || response.source_node_id == 0U || response.target_node_id == 0U ||
      response.source_node_id == response.target_node_id || response.group_id.is_nil() ||
      response.correlation_id == 0U ||
      (response.status_code == common::StatusCode::kOk) != response.authority.has_value()) {
    return common::make_unexpected(invalid("Raft read-authority response identity is invalid"));
  }
  auto raw_status = encode_status(response.status_code);
  if (!raw_status.has_value())
    return common::make_unexpected(raw_status.error());
  std::vector<std::byte> payload;
  if (response.authority.has_value()) {
    const common::Status valid =
        validate_authority(*response.authority, response.source_node_id, response.group_id, limits);
    if (!valid.is_ok())
      return common::make_unexpected(valid);
    auto nested =
        encode_raft_observation_response_v1({.source_node_id = response.source_node_id,
                                             .target_node_id = response.target_node_id,
                                             .group_id = response.group_id,
                                             .correlation_id = response.correlation_id,
                                             .status_code = common::StatusCode::kOk,
                                             .observation = response.authority->observation},
                                            limits.observation);
    if (!nested.has_value())
      return common::make_unexpected(nested.error());
    payload = std::move(*nested);
  }
  constexpr std::size_t kFixedBytes =
      kRaftReadAuthorityResponseHeaderSize + kRaftReadAuthorityFrameTrailerSize;
  if (payload.size() > std::numeric_limits<std::size_t>::max() - kFixedBytes)
    return common::make_unexpected(exhausted("Raft read-authority response length overflows"));
  try {
    const std::size_t total = kFixedBytes + payload.size();
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kResponseMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kRaftReadAuthorityResponseHeaderSize);
    if (status.is_ok())
      status = writer.write_u64_le(total);
    if (status.is_ok())
      status = writer.write_u64_le(response.source_node_id);
    if (status.is_ok())
      status = writer.write_u64_le(response.target_node_id);
    if (status.is_ok())
      status = writer.write_exact(response.group_id.bytes());
    if (status.is_ok())
      status = writer.write_u64_le(response.correlation_id);
    if (status.is_ok())
      status = writer.write_u8(*raw_status);
    if (status.is_ok())
      status = writer.write_u8(response.authority.has_value() ? 1U : 0U);
    if (status.is_ok())
      status = writer.zero_fill(6U);
    if (status.is_ok())
      status = writer.write_u64_le(
          response.authority.has_value() ? response.authority->barrier.barrier.term : 0U);
    if (status.is_ok())
      status = writer.write_u64_le(
          response.authority.has_value() ? response.authority->barrier.barrier.context : 0U);
    if (status.is_ok())
      status = writer.write_u64_le(
          response.authority.has_value() ? response.authority->barrier.barrier.read_index : 0U);
    if (status.is_ok())
      status = writer.write_u64_le(payload.size());
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(payload));
    if (status.is_ok())
      status = writer.zero_fill(16U);
    if (status.is_ok())
      status = writer.write_u32_le(
          common::crc32c(common::ByteView{bytes}.first(kResponseHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.write_exact(payload);
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(invalid("Raft read-authority response encoding failed"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft read-authority response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft read-authority response exceeds containers"));
  }
}

common::Result<RaftReadAuthorityResponse>
decode_raft_read_authority_response_v1(const common::ByteView bytes,
                                       const RaftReadAuthorityTransportLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("Raft read-authority response limits are invalid"));
  constexpr std::size_t kFixedBytes =
      kRaftReadAuthorityResponseHeaderSize + kRaftReadAuthorityFrameTrailerSize;
  if (bytes.size() < kFixedBytes)
    return common::make_unexpected(corruption("Raft read-authority response length is invalid"));
  if (!std::ranges::equal(bytes.first(kResponseMagic.size()), kResponseMagic))
    return common::make_unexpected(corruption("Raft read-authority response magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kResponseHeaderCrcOffset, 4U)};
  common::ByteReader frame_crc_reader{bytes.last(4U)};
  auto header_crc = header_crc_reader.read_u32_le();
  auto frame_crc = frame_crc_reader.read_u32_le();
  if (!header_crc.has_value() ||
      *header_crc != common::crc32c(bytes.first(kResponseHeaderCrcOffset)) ||
      !frame_crc.has_value() || *frame_crc != common::crc32c(bytes.first(bytes.size() - 4U))) {
    return common::make_unexpected(corruption("Raft read-authority response checksum differs"));
  }
  common::ByteReader reader{bytes};
  if (!reader.skip(kResponseMagic.size()).is_ok())
    return common::make_unexpected(corruption("Raft read-authority response is truncated"));
  auto major = reader.read_u16_le();
  auto minor = reader.read_u16_le();
  auto header_size = reader.read_u32_le();
  auto total_size = reader.read_u64_le();
  auto source = reader.read_u64_le();
  auto target = reader.read_u64_le();
  auto group = read_group(reader);
  auto correlation = reader.read_u64_le();
  auto raw_status = reader.read_u8();
  auto authority_present = reader.read_u8();
  auto reserved = reader.read_exact(6U);
  auto term = reader.read_u64_le();
  auto context = reader.read_u64_le();
  auto read_index = reader.read_u64_le();
  auto payload_size = reader.read_u64_le();
  auto payload_crc = reader.read_u32_le();
  auto reserved_tail = reader.read_exact(16U);
  if (!major.has_value() || !minor.has_value() || !header_size.has_value() ||
      !total_size.has_value() || !source.has_value() || !target.has_value() || !group.has_value() ||
      !correlation.has_value() || !raw_status.has_value() || !authority_present.has_value() ||
      !reserved.has_value() || !term.has_value() || !context.has_value() ||
      !read_index.has_value() || !payload_size.has_value() || !payload_crc.has_value() ||
      !reserved_tail.has_value()) {
    return common::make_unexpected(corruption("Raft read-authority response is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("Raft read-authority response version is unsupported"));
  auto status = decode_status(*raw_status);
  if (!status.has_value())
    return common::make_unexpected(status.error());
  constexpr std::size_t kNestedFixedBytes =
      kRaftObservationResponseHeaderSize + kRaftObservationFrameTrailerSize;
  const std::size_t maximum_nested =
      kNestedFixedBytes + kRaftObservationPayloadHeaderSize +
      4U * limits.observation.maximum_voters_per_set * sizeof(raft::NodeId);
  if (*header_size != kRaftReadAuthorityResponseHeaderSize || *total_size != bytes.size() ||
      *source == 0U || *target == 0U || *source == *target || group->is_nil() ||
      *correlation == 0U || *authority_present > 1U || !zero(*reserved) || !zero(*reserved_tail) ||
      *payload_size != bytes.size() - kFixedBytes || *payload_size > maximum_nested ||
      ((*status == common::StatusCode::kOk) != (*authority_present == 1U)) ||
      ((*authority_present == 0U) != (*payload_size == 0U)) ||
      (*authority_present == 1U &&
       (*term == 0U || *context == 0U || *read_index == 0U || *payload_size < kNestedFixedBytes)) ||
      (*authority_present == 0U && (*term != 0U || *context != 0U || *read_index != 0U))) {
    return common::make_unexpected(
        corruption("Raft read-authority response semantics are invalid"));
  }
  const common::ByteView payload =
      bytes.subspan(kRaftReadAuthorityResponseHeaderSize, static_cast<std::size_t>(*payload_size));
  if (*payload_crc != common::crc32c(payload))
    return common::make_unexpected(corruption("Raft read-authority payload checksum differs"));
  std::optional<RaftReadAuthority> authority;
  if (*authority_present == 1U) {
    auto observation_response = decode_raft_observation_response_v1(payload, limits.observation);
    if (!observation_response.has_value())
      return common::make_unexpected(observation_response.error());
    if (observation_response->source_node_id != *source ||
        observation_response->target_node_id != *target ||
        observation_response->group_id != *group ||
        observation_response->correlation_id != *correlation ||
        observation_response->status_code != common::StatusCode::kOk ||
        !observation_response->observation.has_value()) {
      return common::make_unexpected(
          corruption("Raft read-authority nested observation is uncorrelated"));
    }
    raft::RaftGroupObservation observation =
        std::move(observation_response->observation).value_or(raft::RaftGroupObservation{});
    authority = RaftReadAuthority{
        .barrier = {.group_id = *group,
                    .barrier = {.term = *term, .context = *context, .read_index = *read_index}},
        .observation = std::move(observation)};
    const common::Status valid = validate_authority(*authority, *source, *group, limits);
    if (!valid.is_ok())
      return common::make_unexpected(corruption("Raft read-authority proof is invalid"));
  }
  return RaftReadAuthorityResponse{*source,      *target, *group,
                                   *correlation, *status, std::move(authority)};
}

RaftReadAuthorityReceiver::RaftReadAuthorityReceiver(
    RaftReadAuthorityReceiverConfig config) noexcept
    : config_(config) {}

common::Result<RaftReadAuthorityReceiver>
RaftReadAuthorityReceiver::create(const RaftReadAuthorityReceiverConfig config) {
  if (config.local_node_id == 0U || config.authorizer == nullptr || config.service == nullptr ||
      !valid_limits(config.limits)) {
    return common::make_unexpected(
        invalid("Raft read-authority receiver configuration is invalid"));
  }
  return RaftReadAuthorityReceiver{config};
}

common::Result<std::vector<std::byte>>
RaftReadAuthorityReceiver::receive(const common::ByteView request_bytes,
                                   const network::PeerAuthenticationResult& authenticated_peer) {
  if (!authenticated_peer.authorized || authenticated_peer.principal_id == 0U)
    return common::make_unexpected(
        unauthenticated("Raft read-authority peer is not authenticated"));
  auto request = decode_raft_read_authority_request_v1(request_bytes);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  auto authorized =
      config_.authorizer->authorize_node(authenticated_peer.principal_id, request->source_node_id);
  if (!authorized.has_value())
    return common::make_unexpected(authorized.error());
  if (!*authorized)
    return common::make_unexpected(
        unauthenticated("Raft read-authority principal cannot claim the request source"));
  if (request->target_node_id != config_.local_node_id)
    return common::make_unexpected(invalid("Raft read-authority request targets another node"));

  auto acquired = acquire_service(*config_.service, request->group_id);
  RaftReadAuthorityResponse response{.source_node_id = config_.local_node_id,
                                     .target_node_id = request->source_node_id,
                                     .group_id = request->group_id,
                                     .correlation_id = request->correlation_id,
                                     .status_code = acquired.has_value() ? common::StatusCode::kOk
                                                                         : acquired.error().code()};
  if (acquired.has_value()) {
    const common::Status valid =
        validate_authority(*acquired, config_.local_node_id, request->group_id, config_.limits);
    if (!valid.is_ok())
      return common::make_unexpected(invalid("Raft read-authority service result is uncorrelated"));
    response.authority = std::move(*acquired);
  }
  return encode_raft_read_authority_response_v1(response, config_.limits);
}

} // namespace chronos::cluster
