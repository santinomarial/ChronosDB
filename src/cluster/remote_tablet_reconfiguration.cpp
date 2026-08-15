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
constexpr std::array<std::byte, 8U> kResponseMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                                   std::byte{'R'}, std::byte{'T'}, std::byte{'R'},
                                                   std::byte{'S'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kHeaderCrcOffset = 60U;
constexpr std::size_t kResponseHeaderCrcOffset = 96U;
constexpr std::uint16_t kAlreadyPreparedFlag = 1U << 0U;
constexpr std::uint16_t kLeaderHintFlag = 1U << 1U;
constexpr std::uint16_t kKnownResponseFlags = kAlreadyPreparedFlag | kLeaderHintFlag;

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

[[nodiscard]] std::uint32_t response_header_crc(const common::ByteView header) {
  std::array<std::byte, kRemoteTabletReconfigurationResponseHeaderSize> copy{};
  std::ranges::copy(header, copy.begin());
  std::fill_n(copy.begin() + static_cast<std::ptrdiff_t>(kResponseHeaderCrcOffset),
              sizeof(std::uint32_t), std::byte{0U});
  return common::crc32c(copy);
}

[[nodiscard]] bool valid_action_kind(const raft::TabletReconfigurationActionKind kind) noexcept {
  switch (kind) {
  case raft::TabletReconfigurationActionKind::kBeginJointMembership:
  case raft::TabletReconfigurationActionKind::kFinalizeJointMembership:
  case raft::TabletReconfigurationActionKind::kPublishPlacement:
    return true;
  }
  return false;
}

[[nodiscard]] common::Result<std::uint8_t> encode_status_code(const common::StatusCode code) {
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
  return common::make_unexpected(invalid("remote reconfiguration response status is invalid"));
}

[[nodiscard]] common::Result<common::StatusCode> decode_status_code(const std::uint8_t code) {
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
    return common::make_unexpected(corruption("remote reconfiguration response status is unknown"));
  }
}

[[nodiscard]] common::Result<schema::TabletId> read_tablet_id(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  return schema::TabletId::from_bytes(owned);
}

[[nodiscard]] bool retryable_status(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable ||
         code == common::StatusCode::kResourceExhausted || code == common::StatusCode::kIoError;
}

[[nodiscard]] RemoteTabletReconfigurationSender::TimePoint
saturating_add(const RemoteTabletReconfigurationSender::TimePoint now,
               const std::chrono::milliseconds delay) noexcept {
  const auto converted =
      std::chrono::duration_cast<RemoteTabletReconfigurationSender::TimePoint::duration>(delay);
  if (now > RemoteTabletReconfigurationSender::TimePoint::max() - converted)
    return RemoteTabletReconfigurationSender::TimePoint::max();
  return now + converted;
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

common::Result<std::vector<std::byte>> encode_remote_tablet_reconfiguration_response_v1(
    const RemoteTabletReconfigurationResponse& response) {
  if (response.source_node_id == 0U || response.target_node_id == 0U ||
      response.source_node_id == response.target_node_id || response.required_leader_term == 0U ||
      response.action_id.tablet_id.uuid().is_nil() || response.action_id.movement_epoch == 0U ||
      !valid_action_kind(response.action_id.kind)) {
    return common::make_unexpected(invalid("remote reconfiguration response identity is invalid"));
  }
  auto status = encode_status_code(response.status_code);
  if (!status.has_value())
    return common::make_unexpected(std::move(status).error());
  std::uint16_t flags = response.already_prepared ? kAlreadyPreparedFlag : 0U;
  raft::Term hinted_term = 0U;
  raft::NodeId hinted_node = 0U;
  if (response.leader_hint.has_value()) {
    if (response.leader_hint->node_id == 0U || response.leader_hint->term == 0U)
      return common::make_unexpected(invalid("remote reconfiguration leader hint is invalid"));
    flags |= kLeaderHintFlag;
    hinted_term = response.leader_hint->term;
    hinted_node = response.leader_hint->node_id;
  }
  std::vector<std::byte> output(kRemoteTabletReconfigurationResponseSize, std::byte{0U});
  common::ByteWriter header{
      std::span<std::byte>{output}.first(kRemoteTabletReconfigurationResponseHeaderSize)};
  for (const common::Status& write :
       {header.write_exact(kResponseMagic), header.write_u16_le(kMajor),
        header.write_u16_le(kMinor),
        header.write_u32_le(kRemoteTabletReconfigurationResponseHeaderSize),
        header.write_u64_le(kRemoteTabletReconfigurationResponseSize),
        header.write_u64_le(response.source_node_id), header.write_u64_le(response.target_node_id),
        header.write_u64_le(response.required_leader_term),
        header.write_exact(response.action_id.tablet_id.bytes()),
        header.write_u64_le(response.action_id.movement_epoch),
        header.write_u8(static_cast<std::uint8_t>(response.action_id.kind)),
        header.write_u8(*status), header.write_u16_le(flags), header.zero_fill(4U),
        header.write_u64_le(hinted_term), header.write_u64_le(hinted_node), header.write_u32_le(0U),
        header.zero_fill(12U)}) {
    if (!write.is_ok())
      return common::make_unexpected(write);
  }
  store_u32(output, kResponseHeaderCrcOffset,
            response_header_crc(
                common::ByteView{output}.first(kRemoteTabletReconfigurationResponseHeaderSize)));
  store_u32(output,
            kRemoteTabletReconfigurationResponseSize -
                kRemoteTabletReconfigurationResponseTrailerSize,
            common::crc32c(
                common::ByteView{output}.first(kRemoteTabletReconfigurationResponseSize -
                                               kRemoteTabletReconfigurationResponseTrailerSize)));
  return output;
}

common::Result<RemoteTabletReconfigurationResponse>
decode_remote_tablet_reconfiguration_response_v1(const common::ByteView bytes) {
  if (bytes.size() != kRemoteTabletReconfigurationResponseSize)
    return common::make_unexpected(corruption("remote reconfiguration response size is invalid"));
  const common::ByteView header = bytes.first(kRemoteTabletReconfigurationResponseHeaderSize);
  if (response_header_crc(header) != load_u32(header, kResponseHeaderCrcOffset)) {
    return common::make_unexpected(
        corruption("remote reconfiguration response header checksum mismatch"));
  }
  if (!std::ranges::equal(header.first(kResponseMagic.size()), kResponseMagic) ||
      load_u16(header, 8U) != kMajor || load_u16(header, 10U) != kMinor) {
    return common::make_unexpected(
        unsupported("remote reconfiguration response version is unsupported"));
  }
  if (load_u32(bytes, bytes.size() - kRemoteTabletReconfigurationResponseTrailerSize) !=
      common::crc32c(bytes.first(bytes.size() - kRemoteTabletReconfigurationResponseTrailerSize))) {
    return common::make_unexpected(corruption("remote reconfiguration response checksum mismatch"));
  }
  common::ByteReader reader{header.subspan(24U)};
  auto source = reader.read_u64_le();
  auto target = reader.read_u64_le();
  auto term = reader.read_u64_le();
  auto tablet = read_tablet_id(reader);
  auto epoch = reader.read_u64_le();
  auto kind_byte = reader.read_u8();
  auto status_byte = reader.read_u8();
  auto flags = reader.read_u16_le();
  auto reserved = reader.read_exact(4U);
  auto hinted_term = reader.read_u64_le();
  auto hinted_node = reader.read_u64_le();
  auto header_crc_field = reader.read_u32_le();
  auto tail_reserved = reader.read_exact(12U);
  if (load_u32(header, 12U) != kRemoteTabletReconfigurationResponseHeaderSize ||
      load_u64(header, 16U) != kRemoteTabletReconfigurationResponseSize || !source.has_value() ||
      !target.has_value() || !term.has_value() || !tablet.has_value() || !epoch.has_value() ||
      !kind_byte.has_value() || !status_byte.has_value() || !flags.has_value() ||
      !reserved.has_value() || !hinted_term.has_value() || !hinted_node.has_value() ||
      !header_crc_field.has_value() || !tail_reserved.has_value() || !reader.empty() ||
      *header_crc_field != load_u32(header, kResponseHeaderCrcOffset) ||
      (*flags & static_cast<std::uint16_t>(~kKnownResponseFlags)) != 0U ||
      std::ranges::any_of(*reserved,
                          [](const std::byte value) { return value != std::byte{0U}; }) ||
      std::ranges::any_of(*tail_reserved,
                          [](const std::byte value) { return value != std::byte{0U}; })) {
    return common::make_unexpected(corruption("remote reconfiguration response header is invalid"));
  }
  const auto kind = static_cast<raft::TabletReconfigurationActionKind>(*kind_byte);
  auto status = decode_status_code(*status_byte);
  const bool has_hint = (*flags & kLeaderHintFlag) != 0U;
  if (*source == 0U || *target == 0U || *source == *target || *term == 0U ||
      tablet->uuid().is_nil() || *epoch == 0U || !valid_action_kind(kind) || !status.has_value() ||
      (has_hint && (*hinted_term == 0U || *hinted_node == 0U)) ||
      (!has_hint && (*hinted_term != 0U || *hinted_node != 0U))) {
    return common::make_unexpected(
        corruption("remote reconfiguration response identity is invalid"));
  }
  std::optional<RemoteTabletReconfigurationLeaderHint> leader_hint;
  if (has_hint)
    leader_hint = RemoteTabletReconfigurationLeaderHint{*hinted_node, *hinted_term};
  return RemoteTabletReconfigurationResponse{
      *source,    *target,
      *term,      raft::TabletReconfigurationActionId{*tablet, *epoch, kind},
      *status,    (*flags & kAlreadyPreparedFlag) != 0U,
      leader_hint};
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
    return RemoteTabletReconfigurationAdmission{.source_node_id = request->source_node_id,
                                                .target_node_id = request->target_node_id,
                                                .required_leader_term =
                                                    request->required_leader_term,
                                                .action_id = action_id,
                                                .already_prepared = already_prepared,
                                                .completion = std::move(*completion)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("remote reconfiguration receive allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("remote reconfiguration receive exceeded container limits"));
  }
}

common::Result<std::optional<std::vector<std::byte>>>
try_finish_remote_tablet_reconfiguration_admission(
    RemoteTabletReconfigurationAdmission& admission,
    std::optional<RemoteTabletReconfigurationLeaderHint> leader_hint) {
  if (admission.response_finished)
    return common::make_unexpected(invalid("remote reconfiguration response is already finished"));
  if (!admission.completion.is_valid()) {
    return common::make_unexpected(
        invalid("remote reconfiguration admission has no valid completion"));
  }
  if (!admission.completion.is_ready())
    return std::optional<std::vector<std::byte>>{};
  auto completed = admission.completion.wait();
  admission.response_finished = true;
  common::StatusCode response_code = common::StatusCode::kInternal;
  if (!completed.has_value()) {
    response_code = completed.error().code();
  } else {
    if (completed->size() != 1U) {
      return common::make_unexpected(
          corruption("remote reconfiguration completion result count is invalid"));
    }
    response_code = completed->front().status.code();
  }
  auto response =
      encode_remote_tablet_reconfiguration_response_v1(RemoteTabletReconfigurationResponse{
          admission.target_node_id, admission.source_node_id, admission.required_leader_term,
          admission.action_id, response_code, admission.already_prepared, leader_hint});
  if (!response.has_value())
    return common::make_unexpected(std::move(response).error());
  return std::optional<std::vector<std::byte>>{std::move(*response)};
}

RemoteTabletReconfigurationSender::RemoteTabletReconfigurationSender(
    const raft::NodeId source_node_id, raft::PreparedTabletReconfigurationDispatch dispatch,
    const RemoteTabletReconfigurationRetryLimits limits) noexcept
    : source_node_id_(source_node_id), dispatch_(std::move(dispatch)), limits_(limits),
      next_backoff_(limits.initial_backoff) {}

common::Result<RemoteTabletReconfigurationSender>
RemoteTabletReconfigurationSender::create(const raft::NodeId source_node_id,
                                          raft::PreparedTabletReconfigurationDispatch dispatch,
                                          const RemoteTabletReconfigurationRetryLimits limits) {
  const auto maximum_supported_backoff =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  if (source_node_id == 0U || !dispatch.is_valid() || limits.maximum_attempts == 0U ||
      limits.maximum_attempts > 1024U || limits.initial_backoff.count() <= 0 ||
      limits.maximum_backoff < limits.initial_backoff ||
      limits.maximum_backoff > maximum_supported_backoff) {
    return common::make_unexpected(
        invalid("remote reconfiguration retry configuration is invalid"));
  }
  return RemoteTabletReconfigurationSender{source_node_id, std::move(dispatch), limits};
}

common::Result<RemoteTabletReconfigurationAttempt>
RemoteTabletReconfigurationSender::begin_attempt(const RemoteTabletReconfigurationLeaderHint route,
                                                 const TimePoint now) {
  if (state_ == RemoteTabletReconfigurationSenderState::kLocallyAccepted ||
      state_ == RemoteTabletReconfigurationSenderState::kFailed) {
    return common::make_unexpected(invalid("remote reconfiguration sender is terminal"));
  }
  if (state_ == RemoteTabletReconfigurationSenderState::kWaitingForResponse)
    return common::make_unexpected(unavailable("remote reconfiguration response is pending"));
  if (state_ == RemoteTabletReconfigurationSenderState::kBackoff &&
      now < *next_attempt_not_before_) {
    return common::make_unexpected(unavailable("remote reconfiguration retry backoff is active"));
  }
  if (route.node_id == 0U || route.node_id == source_node_id_ || route.term == 0U)
    return common::make_unexpected(invalid("remote reconfiguration leader route is invalid"));
  if (attempts_started_ >= limits_.maximum_attempts)
    return common::make_unexpected(invalid("remote reconfiguration retry budget is exhausted"));
  RemoteTabletReconfigurationRequest request{source_node_id_, route.node_id, route.term,
                                             dispatch_.action()};
  auto bytes = encode_remote_tablet_reconfiguration_request_v1(request);
  if (!bytes.has_value())
    return common::make_unexpected(std::move(bytes).error());
  ++attempts_started_;
  state_ = RemoteTabletReconfigurationSenderState::kWaitingForResponse;
  active_route_ = route;
  suggested_leader_.reset();
  next_attempt_not_before_.reset();
  return RemoteTabletReconfigurationAttempt{attempts_started_, route.node_id, route.term,
                                            std::move(*bytes)};
}

common::Status
RemoteTabletReconfigurationSender::accept_response(const common::ByteView response_bytes,
                                                   const TimePoint now) {
  if (state_ != RemoteTabletReconfigurationSenderState::kWaitingForResponse ||
      !active_route_.has_value()) {
    return invalid("remote reconfiguration sender has no pending response");
  }
  auto response = decode_remote_tablet_reconfiguration_response_v1(response_bytes);
  if (!response.has_value())
    return response.error();
  if (response->source_node_id != active_route_->node_id ||
      response->target_node_id != source_node_id_ ||
      response->required_leader_term != active_route_->term ||
      response->action_id != dispatch_.action().id) {
    return invalid("remote reconfiguration response correlation mismatch");
  }
  suggested_leader_ = response->leader_hint;
  if (response->status_code == common::StatusCode::kOk) {
    last_status_code_ = common::StatusCode::kOk;
    state_ = RemoteTabletReconfigurationSenderState::kLocallyAccepted;
    active_route_.reset();
    next_attempt_not_before_.reset();
    return common::Status::ok();
  }
  return schedule(response->status_code, now);
}

common::Status
RemoteTabletReconfigurationSender::record_transport_failure(const common::StatusCode code,
                                                            const TimePoint now) {
  if (state_ != RemoteTabletReconfigurationSenderState::kWaitingForResponse ||
      !active_route_.has_value()) {
    return invalid("remote reconfiguration sender has no active transport attempt");
  }
  if (code == common::StatusCode::kOk)
    return invalid("remote reconfiguration transport failure cannot be OK");
  suggested_leader_.reset();
  return schedule(code, now);
}

common::Status RemoteTabletReconfigurationSender::schedule(const common::StatusCode code,
                                                           const TimePoint now) {
  last_status_code_ = code;
  active_route_.reset();
  if (!retryable_status(code) || attempts_started_ >= limits_.maximum_attempts) {
    state_ = RemoteTabletReconfigurationSenderState::kFailed;
    next_attempt_not_before_.reset();
    return common::Status::ok();
  }
  state_ = RemoteTabletReconfigurationSenderState::kBackoff;
  next_attempt_not_before_ = saturating_add(now, next_backoff_);
  if (next_backoff_ < limits_.maximum_backoff) {
    const auto current = next_backoff_.count();
    const auto maximum = limits_.maximum_backoff.count();
    next_backoff_ = current > maximum / 2 ? limits_.maximum_backoff
                                          : std::min(next_backoff_ * 2, limits_.maximum_backoff);
  }
  return common::Status::ok();
}

RemoteTabletReconfigurationSenderState RemoteTabletReconfigurationSender::state() const noexcept {
  return state_;
}

std::size_t RemoteTabletReconfigurationSender::attempts_started() const noexcept {
  return attempts_started_;
}

std::optional<RemoteTabletReconfigurationSender::TimePoint>
RemoteTabletReconfigurationSender::next_attempt_not_before() const noexcept {
  return next_attempt_not_before_;
}

std::optional<common::StatusCode>
RemoteTabletReconfigurationSender::last_status_code() const noexcept {
  return last_status_code_;
}

std::optional<RemoteTabletReconfigurationLeaderHint>
RemoteTabletReconfigurationSender::suggested_leader() const noexcept {
  return suggested_leader_;
}

const raft::TabletReconfigurationActionId&
RemoteTabletReconfigurationSender::action_id() const noexcept {
  return dispatch_.action().id;
}

} // namespace chronos::cluster
