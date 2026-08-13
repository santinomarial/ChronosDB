#include "chronos/cluster/raft_observation_transport.hpp"

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
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

constexpr std::array<std::byte, 8U> kRequestMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                                  std::byte{'O'}, std::byte{'B'}, std::byte{'S'},
                                                  std::byte{'Q'}, std::byte{'1'}};
constexpr std::array<std::byte, 8U> kResponseMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                                   std::byte{'O'}, std::byte{'B'}, std::byte{'S'},
                                                   std::byte{'R'}, std::byte{'1'}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kRequestHeaderCrcOffset = 76U;
constexpr std::size_t kResponseHeaderCrcOffset = 92U;
constexpr std::size_t kMaximumVotersPerSet = 4096U;

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

[[nodiscard]] bool valid_limits(const RaftObservationTransportLimits& limits) noexcept {
  return limits.maximum_voters_per_set != 0U &&
         limits.maximum_voters_per_set <= kMaximumVotersPerSet;
}

[[nodiscard]] bool zero(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0U}; });
}

[[nodiscard]] bool canonical_nodes(const std::vector<raft::NodeId>& nodes,
                                   const std::size_t maximum,
                                   const bool require_nonempty) noexcept {
  return nodes.size() <= maximum && (!require_nonempty || !nodes.empty()) &&
         (nodes.empty() || (nodes.front() != 0U && std::ranges::is_sorted(nodes) &&
                            std::ranges::adjacent_find(nodes) == nodes.end()));
}

[[nodiscard]] common::Status
validate_observation(const raft::RaftGroupObservation& value, const raft::NodeId source_node_id,
                     const raft::GroupId& group_id,
                     const RaftObservationTransportLimits& limits) noexcept {
  if (value.group_id != group_id || value.node_id == 0U || value.node_id != source_node_id ||
      value.current_term == 0U || value.applied_index > value.commit_index ||
      value.commit_index > value.last_log_index ||
      !canonical_nodes(value.voters, limits.maximum_voters_per_set, true) ||
      !canonical_nodes(value.committed_voters, limits.maximum_voters_per_set, true) ||
      !canonical_nodes(value.joint_old_voters, limits.maximum_voters_per_set, false) ||
      !canonical_nodes(value.joint_new_voters, limits.maximum_voters_per_set, false)) {
    return invalid("Raft observation identity, indexes, or voters are invalid");
  }
  if (value.leader_id.has_value() && *value.leader_id == 0U)
    return invalid("Raft observation leader identity is zero");
  if ((value.role == raft::Role::kLeader && value.leader_id != value.node_id) ||
      (value.role == raft::Role::kCandidate && value.leader_id.has_value()) ||
      (value.role == raft::Role::kFollower && value.leader_id == value.node_id) ||
      (value.role != raft::Role::kFollower && value.role != raft::Role::kCandidate &&
       value.role != raft::Role::kLeader)) {
    return invalid("Raft observation role and leader identity are inconsistent");
  }
  if (value.joint_membership_active) {
    if (value.joint_old_voters.empty() || value.joint_new_voters.empty() ||
        (value.joint_membership_can_finalize && value.final_membership_pending)) {
      return invalid("Raft observation joint membership state is inconsistent");
    }
  } else if (!value.joint_old_voters.empty() || !value.joint_new_voters.empty() ||
             value.joint_membership_can_finalize || value.final_membership_pending) {
    return invalid("Raft observation stable membership has joint state");
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
  return common::make_unexpected(invalid("Raft observation response status is invalid"));
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
    return common::make_unexpected(corruption("Raft observation response status is unknown"));
  }
}

[[nodiscard]] common::Result<std::uint8_t> encode_role(const raft::Role role) {
  switch (role) {
  case raft::Role::kFollower:
    return 1U;
  case raft::Role::kCandidate:
    return 2U;
  case raft::Role::kLeader:
    return 3U;
  }
  return common::make_unexpected(invalid("Raft observation role is invalid"));
}

[[nodiscard]] common::Result<raft::Role> decode_role(const std::uint8_t raw) {
  switch (raw) {
  case 1U:
    return raft::Role::kFollower;
  case 2U:
    return raft::Role::kCandidate;
  case 3U:
    return raft::Role::kLeader;
  default:
    return common::make_unexpected(corruption("Raft observation role is unknown"));
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

[[nodiscard]] common::Status write_nodes(common::ByteWriter& writer,
                                         const std::vector<raft::NodeId>& nodes) {
  for (const raft::NodeId node : nodes) {
    const common::Status status = writer.write_u64_le(node);
    if (!status.is_ok())
      return status;
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<std::vector<raft::NodeId>>
read_nodes(common::ByteReader& reader, const std::uint32_t count, const std::size_t maximum) {
  if (count > maximum)
    return common::make_unexpected(exhausted("Raft observation voter count exceeds limits"));
  try {
    std::vector<raft::NodeId> nodes;
    nodes.reserve(count);
    for (std::uint32_t index = 0U; index < count; ++index) {
      auto node = reader.read_u64_le();
      if (!node.has_value())
        return common::make_unexpected(node.error());
      nodes.push_back(*node);
    }
    return nodes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft observation voter allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft observation voter count exceeds containers"));
  }
}

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_observation_payload(const raft::RaftGroupObservation& observation,
                           const RaftObservationTransportLimits& limits) {
  const common::Status valid =
      validate_observation(observation, observation.node_id, observation.group_id, limits);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  auto role = encode_role(observation.role);
  if (!role.has_value())
    return common::make_unexpected(role.error());
  const std::size_t node_count = observation.voters.size() + observation.committed_voters.size() +
                                 observation.joint_old_voters.size() +
                                 observation.joint_new_voters.size();
  if (node_count > (std::numeric_limits<std::size_t>::max() - kRaftObservationPayloadHeaderSize) /
                       sizeof(raft::NodeId)) {
    return common::make_unexpected(exhausted("Raft observation payload length overflows"));
  }
  try {
    std::vector<std::byte> bytes(kRaftObservationPayloadHeaderSize +
                                 node_count * sizeof(raft::NodeId));
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_u64_le(observation.node_id);
    if (status.is_ok())
      status = writer.write_u64_le(observation.current_term);
    if (status.is_ok())
      status = writer.write_u64_le(observation.leader_id.value_or(0U));
    if (status.is_ok())
      status = writer.write_u64_le(observation.last_log_index);
    if (status.is_ok())
      status = writer.write_u64_le(observation.commit_index);
    if (status.is_ok())
      status = writer.write_u64_le(observation.applied_index);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(observation.voters.size()));
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(observation.committed_voters.size()));
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(observation.joint_old_voters.size()));
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(observation.joint_new_voters.size()));
    if (status.is_ok())
      status = writer.write_u8(*role);
    if (status.is_ok())
      status = writer.write_u8(observation.leader_id.has_value() ? 1U : 0U);
    if (status.is_ok())
      status = writer.write_u8(observation.joint_membership_active ? 1U : 0U);
    if (status.is_ok())
      status = writer.write_u8(observation.joint_membership_can_finalize ? 1U : 0U);
    if (status.is_ok())
      status = writer.write_u8(observation.final_membership_pending ? 1U : 0U);
    if (status.is_ok())
      status = writer.zero_fill(3U);
    if (status.is_ok())
      status = write_nodes(writer, observation.voters);
    if (status.is_ok())
      status = write_nodes(writer, observation.committed_voters);
    if (status.is_ok())
      status = write_nodes(writer, observation.joint_old_voters);
    if (status.is_ok())
      status = write_nodes(writer, observation.joint_new_voters);
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(invalid("Raft observation payload encoding is inconsistent"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft observation payload allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft observation payload exceeds containers"));
  }
}

[[nodiscard]] common::Result<raft::RaftGroupObservation>
decode_observation_payload(const common::ByteView bytes, const raft::GroupId& group_id,
                           const raft::NodeId source_node_id,
                           const RaftObservationTransportLimits& limits) {
  if (bytes.size() < kRaftObservationPayloadHeaderSize)
    return common::make_unexpected(corruption("Raft observation payload is truncated"));
  common::ByteReader reader{bytes};
  auto node = reader.read_u64_le();
  auto term = reader.read_u64_le();
  auto leader = reader.read_u64_le();
  auto last_log = reader.read_u64_le();
  auto commit = reader.read_u64_le();
  auto applied = reader.read_u64_le();
  auto voters_count = reader.read_u32_le();
  auto committed_count = reader.read_u32_le();
  auto old_count = reader.read_u32_le();
  auto new_count = reader.read_u32_le();
  auto role = reader.read_u8();
  auto leader_present = reader.read_u8();
  auto joint = reader.read_u8();
  auto can_finalize = reader.read_u8();
  auto final_pending = reader.read_u8();
  auto reserved = reader.read_exact(3U);
  if (!node.has_value() || !term.has_value() || !leader.has_value() || !last_log.has_value() ||
      !commit.has_value() || !applied.has_value() || !voters_count.has_value() ||
      !committed_count.has_value() || !old_count.has_value() || !new_count.has_value() ||
      !role.has_value() || !leader_present.has_value() || !joint.has_value() ||
      !can_finalize.has_value() || !final_pending.has_value() || !reserved.has_value()) {
    return common::make_unexpected(corruption("Raft observation payload header is truncated"));
  }
  if (*leader_present > 1U || *joint > 1U || *can_finalize > 1U || *final_pending > 1U ||
      !zero(*reserved)) {
    return common::make_unexpected(corruption("Raft observation payload flags are invalid"));
  }
  auto decoded_role = decode_role(*role);
  if (!decoded_role.has_value())
    return common::make_unexpected(decoded_role.error());
  auto voters = read_nodes(reader, *voters_count, limits.maximum_voters_per_set);
  auto committed = read_nodes(reader, *committed_count, limits.maximum_voters_per_set);
  auto old_voters = read_nodes(reader, *old_count, limits.maximum_voters_per_set);
  auto new_voters = read_nodes(reader, *new_count, limits.maximum_voters_per_set);
  if (!voters.has_value() || !committed.has_value() || !old_voters.has_value() ||
      !new_voters.has_value()) {
    if (!voters.has_value())
      return common::make_unexpected(voters.error());
    if (!committed.has_value())
      return common::make_unexpected(committed.error());
    if (!old_voters.has_value())
      return common::make_unexpected(old_voters.error());
    return common::make_unexpected(new_voters.error());
  }
  if (!reader.empty())
    return common::make_unexpected(corruption("Raft observation payload has trailing bytes"));
  raft::RaftGroupObservation observation{
      .group_id = group_id,
      .node_id = *node,
      .role = *decoded_role,
      .current_term = *term,
      .leader_id = *leader_present == 1U ? std::optional<raft::NodeId>{*leader} : std::nullopt,
      .last_log_index = *last_log,
      .commit_index = *commit,
      .applied_index = *applied,
      .voters = std::move(*voters),
      .committed_voters = std::move(*committed),
      .joint_old_voters = std::move(*old_voters),
      .joint_new_voters = std::move(*new_voters),
      .joint_membership_active = *joint == 1U,
      .joint_membership_can_finalize = *can_finalize == 1U,
      .final_membership_pending = *final_pending == 1U};
  if ((*leader_present == 0U && *leader != 0U) || (*leader_present == 1U && *leader == 0U))
    return common::make_unexpected(corruption("Raft observation leader encoding is noncanonical"));
  const common::Status valid = validate_observation(observation, source_node_id, group_id, limits);
  if (!valid.is_ok())
    return common::make_unexpected(corruption("Raft observation payload semantics are invalid"));
  return observation;
}

[[nodiscard]] common::Result<raft::RaftGroupObservation>
observe_service(RaftObservationService& service, const raft::GroupId& group_id) noexcept {
  try {
    return service.observe(group_id);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft observation service allocation failed"));
  } catch (...) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "Raft observation service threw"});
  }
}

} // namespace

common::Result<std::vector<std::byte>>
encode_raft_observation_request_v1(const RaftObservationRequest& request) {
  if (request.source_node_id == 0U || request.target_node_id == 0U ||
      request.source_node_id == request.target_node_id || request.group_id.is_nil() ||
      request.correlation_id == 0U) {
    return common::make_unexpected(invalid("Raft observation request identity is invalid"));
  }
  try {
    std::vector<std::byte> bytes(kRaftObservationRequestSize);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kRequestMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kRaftObservationRequestHeaderSize);
    if (status.is_ok())
      status = writer.write_u64_le(kRaftObservationRequestSize);
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
          common::crc32c(common::ByteView{bytes}.first(kRaftObservationRequestHeaderSize)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(invalid("Raft observation request encoding is inconsistent"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft observation request allocation failed"));
  }
}

common::Result<RaftObservationRequest>
decode_raft_observation_request_v1(const common::ByteView bytes) {
  if (bytes.size() != kRaftObservationRequestSize)
    return common::make_unexpected(corruption("Raft observation request length is invalid"));
  if (!std::ranges::equal(bytes.first(kRequestMagic.size()), kRequestMagic))
    return common::make_unexpected(corruption("Raft observation request magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kRequestHeaderCrcOffset, 4U)};
  common::ByteReader frame_crc_reader{bytes.last(4U)};
  auto header_crc = header_crc_reader.read_u32_le();
  auto frame_crc = frame_crc_reader.read_u32_le();
  if (!header_crc.has_value() ||
      *header_crc != common::crc32c(bytes.first(kRequestHeaderCrcOffset)) ||
      !frame_crc.has_value() ||
      *frame_crc != common::crc32c(bytes.first(kRaftObservationRequestHeaderSize))) {
    return common::make_unexpected(corruption("Raft observation request checksum differs"));
  }
  common::ByteReader reader{bytes};
  if (!reader.skip(kRequestMagic.size()).is_ok())
    return common::make_unexpected(corruption("Raft observation request is truncated"));
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
    return common::make_unexpected(corruption("Raft observation request header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("Raft observation request version is unsupported"));
  if (*header_size != kRaftObservationRequestHeaderSize ||
      *total_size != kRaftObservationRequestSize || *source == 0U || *target == 0U ||
      *source == *target || group->is_nil() || *correlation == 0U || !zero(*reserved)) {
    return common::make_unexpected(corruption("Raft observation request semantics are invalid"));
  }
  return RaftObservationRequest{*source, *target, *group, *correlation};
}

common::Result<std::vector<std::byte>>
encode_raft_observation_response_v1(const RaftObservationResponse& response,
                                    const RaftObservationTransportLimits limits) {
  if (!valid_limits(limits) || response.source_node_id == 0U || response.target_node_id == 0U ||
      response.source_node_id == response.target_node_id || response.group_id.is_nil() ||
      response.correlation_id == 0U ||
      (response.status_code == common::StatusCode::kOk) != response.observation.has_value()) {
    return common::make_unexpected(invalid("Raft observation response identity is invalid"));
  }
  auto status_code = encode_status(response.status_code);
  if (!status_code.has_value())
    return common::make_unexpected(status_code.error());
  std::vector<std::byte> payload;
  if (response.observation.has_value()) {
    const common::Status valid = validate_observation(
        *response.observation, response.source_node_id, response.group_id, limits);
    if (!valid.is_ok())
      return common::make_unexpected(valid);
    auto encoded = encode_observation_payload(*response.observation, limits);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    payload = std::move(*encoded);
  }
  if (payload.size() > std::numeric_limits<std::size_t>::max() -
                           kRaftObservationResponseHeaderSize - kRaftObservationFrameTrailerSize) {
    return common::make_unexpected(exhausted("Raft observation response length overflows"));
  }
  try {
    const std::size_t total =
        kRaftObservationResponseHeaderSize + payload.size() + kRaftObservationFrameTrailerSize;
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kResponseMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kRaftObservationResponseHeaderSize);
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
      status = writer.write_u8(*status_code);
    if (status.is_ok())
      status = writer.write_u8(response.observation.has_value() ? 1U : 0U);
    if (status.is_ok())
      status = writer.zero_fill(10U);
    if (status.is_ok())
      status = writer.write_u64_le(payload.size());
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(payload));
    if (status.is_ok())
      status = writer.zero_fill(4U);
    if (status.is_ok())
      status = writer.write_u32_le(
          common::crc32c(common::ByteView{bytes}.first(kResponseHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.write_exact(payload);
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(invalid("Raft observation response encoding is inconsistent"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft observation response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft observation response exceeds containers"));
  }
}

common::Result<RaftObservationResponse>
decode_raft_observation_response_v1(const common::ByteView bytes,
                                    const RaftObservationTransportLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("Raft observation response limits are invalid"));
  if (bytes.size() < kRaftObservationResponseHeaderSize + kRaftObservationFrameTrailerSize)
    return common::make_unexpected(corruption("Raft observation response length is invalid"));
  if (!std::ranges::equal(bytes.first(kResponseMagic.size()), kResponseMagic))
    return common::make_unexpected(corruption("Raft observation response magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kResponseHeaderCrcOffset, 4U)};
  common::ByteReader frame_crc_reader{bytes.last(4U)};
  auto header_crc = header_crc_reader.read_u32_le();
  auto frame_crc = frame_crc_reader.read_u32_le();
  if (!header_crc.has_value() ||
      *header_crc != common::crc32c(bytes.first(kResponseHeaderCrcOffset)) ||
      !frame_crc.has_value() || *frame_crc != common::crc32c(bytes.first(bytes.size() - 4U))) {
    return common::make_unexpected(corruption("Raft observation response checksum differs"));
  }
  common::ByteReader reader{bytes};
  if (!reader.skip(kResponseMagic.size()).is_ok())
    return common::make_unexpected(corruption("Raft observation response is truncated"));
  auto major = reader.read_u16_le();
  auto minor = reader.read_u16_le();
  auto header_size = reader.read_u32_le();
  auto total_size = reader.read_u64_le();
  auto source = reader.read_u64_le();
  auto target = reader.read_u64_le();
  auto group = read_group(reader);
  auto correlation = reader.read_u64_le();
  auto raw_status = reader.read_u8();
  auto observation_present = reader.read_u8();
  auto reserved = reader.read_exact(10U);
  auto payload_size = reader.read_u64_le();
  auto payload_crc = reader.read_u32_le();
  auto reserved_tail = reader.read_exact(4U);
  if (!major.has_value() || !minor.has_value() || !header_size.has_value() ||
      !total_size.has_value() || !source.has_value() || !target.has_value() || !group.has_value() ||
      !correlation.has_value() || !raw_status.has_value() || !observation_present.has_value() ||
      !reserved.has_value() || !payload_size.has_value() || !payload_crc.has_value() ||
      !reserved_tail.has_value()) {
    return common::make_unexpected(corruption("Raft observation response header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("Raft observation response version is unsupported"));
  auto status = decode_status(*raw_status);
  if (!status.has_value())
    return common::make_unexpected(status.error());
  if (*header_size != kRaftObservationResponseHeaderSize || *total_size != bytes.size() ||
      *source == 0U || *target == 0U || *source == *target || group->is_nil() ||
      *correlation == 0U || *observation_present > 1U || !zero(*reserved) ||
      !zero(*reserved_tail) ||
      *payload_size !=
          bytes.size() - kRaftObservationResponseHeaderSize - kRaftObservationFrameTrailerSize ||
      ((*status == common::StatusCode::kOk) != (*observation_present == 1U)) ||
      ((*observation_present == 0U) != (*payload_size == 0U))) {
    return common::make_unexpected(corruption("Raft observation response semantics are invalid"));
  }
  const common::ByteView payload =
      bytes.subspan(kRaftObservationResponseHeaderSize, static_cast<std::size_t>(*payload_size));
  if (*payload_crc != common::crc32c(payload))
    return common::make_unexpected(
        corruption("Raft observation response payload checksum differs"));
  std::optional<raft::RaftGroupObservation> observation;
  if (*observation_present == 1U) {
    auto decoded = decode_observation_payload(payload, *group, *source, limits);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
    observation = std::move(*decoded);
  }
  return RaftObservationResponse{*source,      *target, *group,
                                 *correlation, *status, std::move(observation)};
}

RaftObservationReceiver::RaftObservationReceiver(RaftObservationReceiverConfig config) noexcept
    : config_(config) {}

common::Result<RaftObservationReceiver>
RaftObservationReceiver::create(const RaftObservationReceiverConfig config) {
  if (config.local_node_id == 0U || config.authorizer == nullptr || config.service == nullptr ||
      !valid_limits(config.limits)) {
    return common::make_unexpected(invalid("Raft observation receiver configuration is invalid"));
  }
  return RaftObservationReceiver{config};
}

common::Result<std::vector<std::byte>>
RaftObservationReceiver::receive(const common::ByteView request_bytes,
                                 const network::PeerAuthenticationResult& authenticated_peer) {
  if (!authenticated_peer.authorized || authenticated_peer.principal_id == 0U)
    return common::make_unexpected(unauthenticated("Raft observation peer is not authenticated"));
  auto request = decode_raft_observation_request_v1(request_bytes);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  auto authorized =
      config_.authorizer->authorize_node(authenticated_peer.principal_id, request->source_node_id);
  if (!authorized.has_value())
    return common::make_unexpected(authorized.error());
  if (!*authorized)
    return common::make_unexpected(
        unauthenticated("Raft observation principal cannot claim the request source"));
  if (request->target_node_id != config_.local_node_id)
    return common::make_unexpected(invalid("Raft observation request targets another node"));

  auto observed = observe_service(*config_.service, request->group_id);
  RaftObservationResponse response{.source_node_id = config_.local_node_id,
                                   .target_node_id = request->source_node_id,
                                   .group_id = request->group_id,
                                   .correlation_id = request->correlation_id,
                                   .status_code = observed.has_value() ? common::StatusCode::kOk
                                                                       : observed.error().code()};
  if (observed.has_value()) {
    const common::Status valid =
        validate_observation(*observed, config_.local_node_id, request->group_id, config_.limits);
    if (!valid.is_ok())
      return common::make_unexpected(invalid("Raft observation service result is uncorrelated"));
    response.observation = std::move(*observed);
  }
  return encode_raft_observation_response_v1(response, config_.limits);
}

} // namespace chronos::cluster
