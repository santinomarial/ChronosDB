#include "chronos/cluster/remote_tablet_reconfiguration.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                           std::byte{'R'}, std::byte{'T'}, std::byte{'R'},
                                           std::byte{'Q'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kHeaderCrcOffset = 60U;

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

[[nodiscard]] bool valid_limits(const RemoteTabletReconfigurationCodecLimits& limits) noexcept {
  constexpr std::size_t kActionFramingBytes =
      raft::kTabletReconfigurationActionHeaderSize + raft::kTabletReconfigurationActionTrailerSize;
  constexpr std::size_t kMinimumActionLimit = kActionFramingBytes + 8U + sizeof(raft::NodeId);
  return limits.maximum_request_bytes >= kRemoteTabletReconfigurationHeaderSize +
                                             kActionFramingBytes +
                                             kRemoteTabletReconfigurationTrailerSize &&
         limits.maximum_request_bytes <= kMaximumRemoteTabletReconfigurationRequestSize &&
         limits.action.maximum_action_bytes >= kMinimumActionLimit &&
         limits.action.maximum_action_bytes <= raft::kMaximumTabletReconfigurationActionSize &&
         limits.action.maximum_voters > 0U &&
         limits.action.maximum_voters <= std::numeric_limits<std::uint32_t>::max() &&
         limits.action.maximum_voters <=
             (limits.action.maximum_action_bytes - kActionFramingBytes - 8U) / sizeof(raft::NodeId);
}

void store_u32(const std::span<std::byte> bytes, const std::size_t offset,
               const std::uint32_t value) {
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal)
    bytes[offset + ordinal] = static_cast<std::byte>(value >> (ordinal * 8U));
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes, const std::size_t offset) {
  return static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset])) |
         static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + 1U]) << 8U);
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value{};
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + ordinal]))
             << (ordinal * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes, const std::size_t offset) {
  std::uint64_t value{};
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[offset + ordinal]))
             << (ordinal * 8U);
  }
  return value;
}

[[nodiscard]] std::uint32_t header_crc(const common::ByteView header) {
  std::array<std::byte, kRemoteTabletReconfigurationHeaderSize> copy{};
  std::ranges::copy(header, copy.begin());
  std::fill_n(copy.begin() + static_cast<std::ptrdiff_t>(kHeaderCrcOffset), sizeof(std::uint32_t),
              std::byte{0U});
  return common::crc32c(copy);
}

[[nodiscard]] bool
request_binding_is_valid(const RemoteTabletReconfigurationRequest& request) noexcept {
  return request.source_node_id != 0U && request.target_node_id != 0U &&
         request.source_node_id != request.target_node_id && request.required_leader_term != 0U;
}

[[nodiscard]] common::Status
validate_receiver_config(const RemoteTabletReconfigurationReceiverConfig& config) {
  if (config.local_node_id == 0U || config.tablet_id.uuid().is_nil() ||
      config.tablet_group_id.is_nil() || config.metadata_group_id.is_nil() ||
      config.tablet_group_id == config.metadata_group_id || config.authorizer == nullptr ||
      config.action_ledger == nullptr || config.runtime == nullptr ||
      !valid_limits(config.codec_limits)) {
    return invalid("remote reconfiguration receiver configuration is invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_action_route(const RemoteTabletReconfigurationReceiverConfig& config,
                      const raft::TabletReconfigurationAction& action) {
  if (action.id.tablet_id != config.tablet_id)
    return invalid("remote reconfiguration action targets a different tablet");
  const bool is_placement = action.kind == raft::TabletReconfigurationActionKind::kPublishPlacement;
  const raft::GroupId& expected_group =
      is_placement ? config.metadata_group_id : config.tablet_group_id;
  if (action.request.group_id != expected_group)
    return invalid("remote reconfiguration action targets the wrong Raft group");
  return common::Status::ok();
}

} // namespace

common::Result<std::vector<std::byte>> encode_remote_tablet_reconfiguration_request_v1(
    const RemoteTabletReconfigurationRequest& request,
    const RemoteTabletReconfigurationCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("remote reconfiguration codec limits are invalid"));
  if (!request_binding_is_valid(request))
    return common::make_unexpected(invalid("remote reconfiguration route binding is invalid"));
  auto action = raft::encode_tablet_reconfiguration_action_v1(request.action, limits.action);
  if (!action.has_value())
    return common::make_unexpected(std::move(action).error());
  if (action->size() > limits.maximum_request_bytes - kRemoteTabletReconfigurationHeaderSize -
                           kRemoteTabletReconfigurationTrailerSize) {
    return common::make_unexpected(
        exhausted("remote reconfiguration request exceeds the configured size limit"));
  }
  const std::size_t total_size = kRemoteTabletReconfigurationHeaderSize + action->size() +
                                 kRemoteTabletReconfigurationTrailerSize;
  std::vector<std::byte> output(total_size, std::byte{0U});
  common::ByteWriter header{
      std::span<std::byte>{output}.first(kRemoteTabletReconfigurationHeaderSize)};
  for (const common::Status& status :
       {header.write_exact(kMagic), header.write_u16_le(kMajor), header.write_u16_le(kMinor),
        header.write_u32_le(kRemoteTabletReconfigurationHeaderSize),
        header.write_u64_le(total_size), header.write_u64_le(request.source_node_id),
        header.write_u64_le(request.target_node_id),
        header.write_u64_le(request.required_leader_term), header.write_u64_le(action->size()),
        header.write_u32_le(common::crc32c(*action)), header.write_u32_le(0U),
        header.zero_fill(16U)}) {
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  std::ranges::copy(*action, output.begin() + static_cast<std::ptrdiff_t>(
                                                  kRemoteTabletReconfigurationHeaderSize));
  store_u32(output, kHeaderCrcOffset,
            header_crc(common::ByteView{output}.first(kRemoteTabletReconfigurationHeaderSize)));
  store_u32(output, total_size - kRemoteTabletReconfigurationTrailerSize,
            common::crc32c(common::ByteView{output}.first(
                total_size - kRemoteTabletReconfigurationTrailerSize)));
  return output;
}

common::Result<RemoteTabletReconfigurationRequest> decode_remote_tablet_reconfiguration_request_v1(
    const common::ByteView bytes, const RemoteTabletReconfigurationCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("remote reconfiguration codec limits are invalid"));
  constexpr std::size_t kMinimumSize =
      kRemoteTabletReconfigurationHeaderSize + raft::kTabletReconfigurationActionHeaderSize +
      raft::kTabletReconfigurationActionTrailerSize + kRemoteTabletReconfigurationTrailerSize;
  if (bytes.size() < kMinimumSize || bytes.size() > limits.maximum_request_bytes)
    return common::make_unexpected(corruption("remote reconfiguration request size is invalid"));
  const common::ByteView header = bytes.first(kRemoteTabletReconfigurationHeaderSize);
  if (header_crc(header) != load_u32(header, kHeaderCrcOffset))
    return common::make_unexpected(corruption("remote reconfiguration header checksum mismatch"));
  if (!std::ranges::equal(header.first(kMagic.size()), kMagic) || load_u16(header, 8U) != kMajor ||
      load_u16(header, 10U) != kMinor) {
    return common::make_unexpected(unsupported("remote reconfiguration version is unsupported"));
  }
  common::ByteReader reader{header.subspan(24U)};
  auto source = reader.read_u64_le();
  auto target = reader.read_u64_le();
  auto term = reader.read_u64_le();
  auto action_size = reader.read_u64_le();
  auto action_crc = reader.read_u32_le();
  auto header_crc_field = reader.read_u32_le();
  auto reserved = reader.read_exact(16U);
  if (load_u32(header, 12U) != kRemoteTabletReconfigurationHeaderSize ||
      load_u64(header, 16U) != bytes.size() || !source.has_value() || !target.has_value() ||
      !term.has_value() || !action_size.has_value() || !action_crc.has_value() ||
      !header_crc_field.has_value() || !reserved.has_value() || !reader.empty() ||
      *header_crc_field != load_u32(header, kHeaderCrcOffset) ||
      *action_size != bytes.size() - kRemoteTabletReconfigurationHeaderSize -
                          kRemoteTabletReconfigurationTrailerSize ||
      *action_size > limits.action.maximum_action_bytes ||
      std::ranges::any_of(*reserved,
                          [](const std::byte value) { return value != std::byte{0U}; })) {
    return common::make_unexpected(corruption("remote reconfiguration header is invalid"));
  }
  if (load_u32(bytes, bytes.size() - kRemoteTabletReconfigurationTrailerSize) !=
      common::crc32c(bytes.first(bytes.size() - kRemoteTabletReconfigurationTrailerSize))) {
    return common::make_unexpected(corruption("remote reconfiguration checksum mismatch"));
  }
  const common::ByteView action_bytes =
      bytes.subspan(kRemoteTabletReconfigurationHeaderSize, static_cast<std::size_t>(*action_size));
  if (common::crc32c(action_bytes) != *action_crc)
    return common::make_unexpected(corruption("remote reconfiguration action checksum mismatch"));
  auto action = raft::decode_tablet_reconfiguration_action_v1(action_bytes, limits.action);
  if (!action.has_value())
    return common::make_unexpected(std::move(action).error());
  RemoteTabletReconfigurationRequest request{*source, *target, *term, std::move(*action)};
  if (!request_binding_is_valid(request))
    return common::make_unexpected(corruption("remote reconfiguration route binding is invalid"));
  return request;
}

RemoteTabletReconfigurationReceiver::RemoteTabletReconfigurationReceiver(
    RemoteTabletReconfigurationReceiverConfig config) noexcept
    : config_(config) {}

common::Result<RemoteTabletReconfigurationReceiver>
RemoteTabletReconfigurationReceiver::create(RemoteTabletReconfigurationReceiverConfig config) {
  common::Status valid = validate_receiver_config(config);
  if (!valid.is_ok())
    return common::make_unexpected(std::move(valid));
  return RemoteTabletReconfigurationReceiver{config};
}

common::Result<RemoteTabletReconfigurationAdmission>
RemoteTabletReconfigurationReceiver::try_receive(
    const common::ByteView request_bytes,
    const network::PeerAuthenticationResult& authenticated_peer) {
  if (!authenticated_peer.authorized || authenticated_peer.principal_id == 0U) {
    return common::make_unexpected(
        unauthenticated("remote reconfiguration requires an authenticated principal"));
  }
  try {
    auto request =
        decode_remote_tablet_reconfiguration_request_v1(request_bytes, config_.codec_limits);
    if (!request.has_value())
      return common::make_unexpected(std::move(request).error());
    auto authorized = config_.authorizer->authorize_node(authenticated_peer.principal_id,
                                                         request->source_node_id);
    if (!authorized.has_value())
      return common::make_unexpected(std::move(authorized).error());
    if (!*authorized)
      return common::make_unexpected(
          unauthenticated("authenticated principal cannot claim the source node"));
    if (request->target_node_id != config_.local_node_id) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kUnavailable, "remote reconfiguration targets a different node"});
    }
    common::Status route = validate_action_route(config_, request->action);
    if (!route.is_ok())
      return common::make_unexpected(std::move(route));
    auto prepared = raft::prepare_received_tablet_reconfiguration_action(std::move(request->action),
                                                                         *config_.action_ledger);
    if (!prepared.has_value())
      return common::make_unexpected(std::move(prepared).error());
    const raft::TabletReconfigurationActionId action_id = prepared->action().id;
    const bool already_prepared = prepared->preparation().already_present;
    auto completion = raft::try_submit_current_leader_prepared_tablet_reconfiguration(
        *prepared, request->required_leader_term, *config_.runtime);
    if (!completion.has_value())
      return common::make_unexpected(std::move(completion).error());
    return RemoteTabletReconfigurationAdmission{action_id, already_prepared,
                                                std::move(*completion)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("remote reconfiguration receive allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("remote reconfiguration receive exceeded container limits"));
  }
}

} // namespace chronos::cluster
