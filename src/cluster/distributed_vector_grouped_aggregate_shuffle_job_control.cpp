#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

using namespace distributed_vector_grouped_aggregate_shuffle_job_control_format;

inline constexpr std::size_t kHeaderCrcOffset = 124U;
inline constexpr std::size_t kResponseHeaderCrcOffset = 92U;
inline constexpr std::uint16_t kResponseEndpointFlag = 1U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] bool all_zero(const common::ByteView bytes) {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{}; });
}

[[nodiscard]] bool valid_endpoint(const network::Ipv4Endpoint endpoint) noexcept {
  return endpoint.port != 0U && std::ranges::any_of(endpoint.address, [](const std::uint8_t value) {
           return value != 0U;
         });
}

[[nodiscard]] bool valid_limits(
    const DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits& limits) noexcept {
  constexpr std::size_t kMinimumSchemaFrameLength =
      query::distributed_vector_result_schema_format::kHeaderLength +
      query::distributed_vector_result_schema_format::kDescriptorFixedLength + 1U +
      query::distributed_vector_result_schema_format::kTrailerLength;
  return limits.maximum_frame_length >= kHeaderLength + kTrailerLength &&
         limits.maximum_frame_length <= kMaximumFrameLength &&
         limits.maximum_execution_timeout.count() > 0 &&
         limits.maximum_execution_timeout <= kMaximumExecutionTimeout &&
         limits.authority.maximum_frame_length >=
             distributed_vector_grouped_aggregate_shuffle_authority_format::kMinimumFrameLength &&
         limits.authority.maximum_frame_length <=
             distributed_vector_grouped_aggregate_shuffle_authority_format::kMaximumFrameLength &&
         limits.authority.authority.maximum_sources > 0U &&
         limits.authority.authority.maximum_sources <=
             kMaximumDistributedVectorGroupedAggregateShuffleSources &&
         limits.authority.authority.maximum_partitions > 0U &&
         limits.authority.authority.maximum_partitions <=
             query::kMaximumDistributedVectorGroupedAggregatePartitions &&
         limits.authority.authority.maximum_retained_configuration_bytes > 0U &&
         limits.authority.authority.maximum_retained_configuration_bytes <=
             kMaximumDistributedVectorGroupedAggregateShuffleAuthorityBytes &&
         limits.result_schema.maximum_frame_length >= kMinimumSchemaFrameLength &&
         limits.result_schema.maximum_frame_length <=
             query::distributed_vector_result_schema_format::kMaximumFrameLength &&
         limits.result_schema.maximum_columns > 0U &&
         limits.result_schema.maximum_columns <=
             query::distributed_vector_result_schema_format::kMaximumColumns &&
         limits.result_schema.maximum_name_length > 0U &&
         limits.result_schema.maximum_name_length <=
             query::distributed_vector_result_schema_format::kMaximumNameLength &&
         limits.maximum_routes > 0U &&
         limits.maximum_routes <=
             distributed_vector_grouped_aggregate_shuffle_job_control_v2_format::kMaximumRoutes;
}

[[nodiscard]] bool has_target(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
                              const raft::NodeId target_node_id) noexcept {
  return std::ranges::any_of(authority.destinations(), [&](const auto& destination) {
    return destination.node_id == target_node_id;
  });
}

[[nodiscard]] bool
raw_schema_matches(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
                   const query::DistributedVectorResultSchema& result_schema) {
  const auto keys = authority.key_definitions();
  const auto aggregates = authority.aggregate_definitions();
  if (result_schema.columns.size() != keys.size() + aggregates.size())
    return false;
  for (std::size_t ordinal = 0U; ordinal < keys.size(); ++ordinal) {
    if (result_schema.columns[ordinal].type != keys[ordinal].type ||
        result_schema.columns[ordinal].nullable != keys[ordinal].nullable) {
      return false;
    }
  }
  for (std::size_t ordinal = 0U; ordinal < aggregates.size(); ++ordinal) {
    const auto shape = query::vector_aggregate_output_shape(aggregates[ordinal]);
    const auto& column = result_schema.columns[keys.size() + ordinal];
    if (!shape.has_value() || column.type != shape->type || column.nullable != shape->nullable)
      return false;
  }
  return true;
}

[[nodiscard]] common::Status
validate_prepare(const DistributedVectorGroupedAggregateShuffleJobPrepare& request,
                 const std::chrono::milliseconds maximum_timeout) {
  if (request.coordinator_node_id == 0U || request.target_node_id == 0U ||
      request.coordinator_node_id == request.target_node_id ||
      !valid_endpoint(request.coordinator_result_endpoint) ||
      request.execution_timeout.count() <= 0 || request.execution_timeout > maximum_timeout ||
      !has_target(request.authority, request.target_node_id) ||
      !query::validate_distributed_vector_result_schema_value(request.result_schema).is_ok() ||
      !raw_schema_matches(request.authority, request.result_schema)) {
    return invalid("grouped shuffle reducer-job prepare request is invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_seal(const DistributedVectorGroupedAggregateShuffleJobSeal& request) {
  if (request.query_id.is_nil() || request.coordinator_node_id == 0U ||
      request.target_node_id == 0U || request.coordinator_node_id == request.target_node_id) {
    return invalid("grouped shuffle reducer-job seal request is invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<std::uint8_t>
action_code(const DistributedVectorGroupedAggregateShuffleJobControlAction action) {
  switch (action) {
  case DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare:
    return 1U;
  case DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal:
    return 2U;
  case DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes:
    break;
  }
  return common::make_unexpected(invalid("grouped shuffle reducer-job action is invalid"));
}

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlAction>
decode_action(const std::uint8_t code) {
  if (code == 1U)
    return DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare;
  if (code == 2U)
    return DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal;
  return common::make_unexpected(corruption("grouped shuffle reducer-job action is unknown"));
}

[[nodiscard]] common::Result<std::uint8_t> status_code(const common::StatusCode code) {
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
  return common::make_unexpected(invalid("grouped shuffle reducer-job response status is invalid"));
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
        corruption("grouped shuffle reducer-job response status is unknown"));
  }
}

[[nodiscard]] common::Status
validate_response(const DistributedVectorGroupedAggregateShuffleJobControlResponse& response) {
  const bool prepare_success =
      response.action == DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare &&
      response.status_code == common::StatusCode::kOk;
  if (response.query_id.is_nil() || response.coordinator_node_id == 0U ||
      response.target_node_id == 0U || response.coordinator_node_id == response.target_node_id ||
      (!prepare_success && response.reducer_shuffle_endpoint.has_value()) ||
      (response.reducer_shuffle_endpoint.has_value() &&
       !valid_endpoint(*response.reducer_shuffle_endpoint))) {
    return invalid("grouped shuffle reducer-job response is invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_request_bytes(const DistributedVectorGroupedAggregateShuffleJobControlAction action,
                     const common::Uuid& query_id, const raft::NodeId coordinator_node_id,
                     const raft::NodeId target_node_id, const network::Ipv4Endpoint endpoint,
                     const std::chrono::milliseconds timeout, const common::ByteView authority,
                     const common::ByteView schema) {
  const auto code = action_code(action);
  if (!code.has_value())
    return common::make_unexpected(code.error());
  auto payload_length = common::checked_add(authority.size(), schema.size());
  auto frame_length = payload_length.has_value()
                          ? common::checked_add(kHeaderLength + kTrailerLength, *payload_length)
                          : std::nullopt;
  if (!frame_length.has_value() || *frame_length > kMaximumFrameLength)
    return common::make_unexpected(exhausted("grouped shuffle reducer-job frame is too large"));
  try {
    std::vector<std::byte> bytes(*frame_length);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kRequestMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kHeaderLength);
    if (write.is_ok())
      write = writer.write_u64_le(*frame_length);
    if (write.is_ok())
      write = writer.write_u8(*code);
    if (write.is_ok())
      write = writer.zero_fill(7U);
    if (write.is_ok())
      write = writer.write_u64_le(coordinator_node_id);
    if (write.is_ok())
      write = writer.write_u64_le(target_node_id);
    if (write.is_ok())
      write = writer.write_exact(query_id.bytes());
    if (write.is_ok())
      write = writer.write_exact(std::as_bytes(std::span{endpoint.address}));
    if (write.is_ok())
      write = writer.write_u16_le(endpoint.port);
    if (write.is_ok())
      write = writer.zero_fill(2U);
    if (write.is_ok())
      write = writer.write_u64_le(static_cast<std::uint64_t>(timeout.count()));
    if (write.is_ok())
      write = writer.write_u64_le(authority.size());
    if (write.is_ok())
      write = writer.write_u64_le(schema.size());
    if (write.is_ok())
      write = writer.write_u32_le(authority.empty() ? 0U : common::crc32c(authority));
    if (write.is_ok())
      write = writer.write_u32_le(schema.empty() ? 0U : common::crc32c(schema));
    if (write.is_ok())
      write = writer.zero_fill(20U);
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_exact(authority);
    if (write.is_ok())
      write = writer.write_exact(schema);
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("grouped shuffle reducer-job layout failed"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle reducer-job encoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("grouped shuffle reducer-job encoding exceeds limits"));
  }
}

} // namespace

common::Status validate_distributed_vector_grouped_aggregate_shuffle_job_control_decode_limits(
    const DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits& limits) noexcept {
  return valid_limits(limits) ? common::Status::ok()
                              : invalid("grouped shuffle reducer-job decode limits are invalid");
}

EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest::
    EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest(
        std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView
EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest::bytes() const noexcept {
  return bytes_;
}

EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse::
    EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse(
        std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView
EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(
    const DistributedVectorGroupedAggregateShuffleJobPrepare& request) {
  const common::Status valid = validate_prepare(request, kMaximumExecutionTimeout);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  auto authority = encode_distributed_vector_grouped_aggregate_shuffle_authority(request.authority);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
  auto schema = query::encode_distributed_vector_result_schema(request.result_schema);
  if (!schema.has_value())
    return common::make_unexpected(schema.error());
  auto encoded =
      encode_request_bytes(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                           request.authority.query_id(), request.coordinator_node_id,
                           request.target_node_id, request.coordinator_result_endpoint,
                           request.execution_timeout, authority->bytes(), schema->bytes());
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest{std::move(*encoded)};
}

common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
encode_distributed_vector_grouped_aggregate_shuffle_job_seal_v1(
    const DistributedVectorGroupedAggregateShuffleJobSeal& request) {
  const common::Status valid = validate_seal(request);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  auto encoded = encode_request_bytes(
      DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal, request.query_id,
      request.coordinator_node_id, request.target_node_id, {}, {}, {}, {});
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest{std::move(*encoded)};
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlRequest>
decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(
    const common::ByteView bytes,
    const DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits) {
  const auto minimum_prepare =
      kHeaderLength +
      distributed_vector_grouped_aggregate_shuffle_authority_format::kMinimumFrameLength +
      query::distributed_vector_result_schema_format::kHeaderLength +
      query::distributed_vector_result_schema_format::kDescriptorFixedLength + 1U +
      query::distributed_vector_result_schema_format::kTrailerLength + kTrailerLength;
  const common::Status valid_decode_limits =
      validate_distributed_vector_grouped_aggregate_shuffle_job_control_decode_limits(limits);
  if (!valid_decode_limits.is_ok())
    return common::make_unexpected(valid_decode_limits);
  if (bytes.size() < kHeaderLength + kTrailerLength || bytes.size() > kMaximumFrameLength)
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job frame length is invalid"));
  if (bytes.size() > limits.maximum_frame_length)
    return common::make_unexpected(exhausted("grouped shuffle reducer-job exceeds caller limit"));
  if (!std::ranges::equal(bytes.first(kRequestMagic.size()), kRequestMagic))
    return common::make_unexpected(corruption("grouped shuffle reducer-job magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job header checksum differs"));
  }
  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kRequestMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto action_value = reader.read_u8();
  const auto action_reserved = reader.read_exact(7U);
  const auto coordinator_node_id = reader.read_u64_le();
  const auto target_node_id = reader.read_u64_le();
  const auto query_id_bytes = reader.read_exact(common::Uuid::kSize);
  const auto address_bytes = reader.read_exact(4U);
  const auto port = reader.read_u16_le();
  const auto route_reserved = reader.read_exact(2U);
  const auto timeout = reader.read_u64_le();
  const auto authority_length = reader.read_u64_le();
  const auto schema_length = reader.read_u64_le();
  const auto authority_crc = reader.read_u32_le();
  const auto schema_crc = reader.read_u32_le();
  const auto reserved = reader.read_exact(20U);
  static_cast<void>(reader.skip(4U));
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !action_value.has_value() || !action_reserved.has_value() ||
      !coordinator_node_id.has_value() || !target_node_id.has_value() ||
      !query_id_bytes.has_value() || !address_bytes.has_value() || !port.has_value() ||
      !route_reserved.has_value() || !timeout.has_value() || !authority_length.has_value() ||
      !schema_length.has_value() || !authority_crc.has_value() || !schema_crc.has_value() ||
      !reserved.has_value()) {
    return common::make_unexpected(corruption("grouped shuffle reducer-job header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("grouped shuffle reducer-job version is unsupported"));
  const auto action = decode_action(*action_value);
  if (!action.has_value())
    return common::make_unexpected(action.error());
  if (*authority_length > std::numeric_limits<std::size_t>::max() ||
      *schema_length > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(corruption("grouped shuffle reducer-job lengths overflow"));
  }
  const auto authority_size = static_cast<std::size_t>(*authority_length);
  const auto schema_size = static_cast<std::size_t>(*schema_length);
  auto payload_length = common::checked_add(authority_size, schema_size);
  auto expected_length = payload_length.has_value()
                             ? common::checked_add(kHeaderLength + kTrailerLength, *payload_length)
                             : std::nullopt;
  if (*header_length != kHeaderLength || *frame_length != bytes.size() ||
      !all_zero(*action_reserved) || !all_zero(*route_reserved) || !all_zero(*reserved) ||
      !expected_length.has_value() || *expected_length != bytes.size()) {
    return common::make_unexpected(corruption("grouped shuffle reducer-job header is invalid"));
  }
  common::ByteReader trailer{bytes.last(kTrailerLength)};
  const auto stored_frame_crc = trailer.read_u32_le();
  if (!stored_frame_crc.has_value() ||
      *stored_frame_crc != common::crc32c(bytes.first(bytes.size() - kTrailerLength))) {
    return common::make_unexpected(corruption("grouped shuffle reducer-job checksum differs"));
  }
  common::Uuid::Bytes query_owned{};
  std::ranges::copy(*query_id_bytes, query_owned.begin());
  const common::Uuid query_id{query_owned};
  if (*action == DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal) {
    if (*authority_length != 0U || *schema_length != 0U || *authority_crc != 0U ||
        *schema_crc != 0U || !all_zero(*address_bytes) || *port != 0U || *timeout != 0U) {
      return common::make_unexpected(
          corruption("grouped shuffle reducer-job seal request is noncanonical"));
    }
    DistributedVectorGroupedAggregateShuffleJobSeal seal{query_id, *coordinator_node_id,
                                                         *target_node_id};
    if (!validate_seal(seal).is_ok())
      return common::make_unexpected(
          corruption("grouped shuffle reducer-job seal identity is invalid"));
    return DistributedVectorGroupedAggregateShuffleJobControlRequest{seal};
  }
  std::array<std::uint8_t, 4U> address{};
  for (std::size_t index = 0U; index < address.size(); ++index)
    address[index] = std::to_integer<std::uint8_t>((*address_bytes)[index]);
  const network::Ipv4Endpoint endpoint{.address = address, .port = *port};
  if (bytes.size() < minimum_prepare || *authority_length == 0U || *schema_length == 0U ||
      *authority_length >
          distributed_vector_grouped_aggregate_shuffle_authority_format::kMaximumFrameLength ||
      *schema_length > query::distributed_vector_result_schema_format::kMaximumFrameLength ||
      query_id.is_nil() || *coordinator_node_id == 0U || *target_node_id == 0U ||
      *coordinator_node_id == *target_node_id || !valid_endpoint(endpoint) || *timeout == 0U ||
      *timeout > static_cast<std::uint64_t>(kMaximumExecutionTimeout.count())) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job prepare header is invalid"));
  }
  if (*authority_length > limits.authority.maximum_frame_length ||
      *schema_length > limits.result_schema.maximum_frame_length ||
      *timeout > static_cast<std::uint64_t>(limits.maximum_execution_timeout.count())) {
    return common::make_unexpected(exhausted("grouped shuffle reducer-job prepare exceeds limits"));
  }
  const common::ByteView authority_bytes = bytes.subspan(kHeaderLength, authority_size);
  const common::ByteView schema_bytes = bytes.subspan(kHeaderLength + authority_size, schema_size);
  if (*authority_crc != common::crc32c(authority_bytes) ||
      *schema_crc != common::crc32c(schema_bytes))
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job nested checksum differs"));
  auto authority = decode_distributed_vector_grouped_aggregate_shuffle_authority_exact(
      authority_bytes, limits.authority);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
  auto result_schema =
      query::decode_distributed_vector_result_schema_exact(schema_bytes, limits.result_schema);
  if (!result_schema.has_value())
    return common::make_unexpected(result_schema.error());
  DistributedVectorGroupedAggregateShuffleJobPrepare prepare{
      .coordinator_node_id = *coordinator_node_id,
      .target_node_id = *target_node_id,
      .coordinator_result_endpoint = endpoint,
      .execution_timeout = std::chrono::milliseconds{*timeout},
      .authority = std::move(*authority),
      .result_schema = std::move(*result_schema)};
  if (prepare.authority.query_id() != query_id ||
      !validate_prepare(prepare, limits.maximum_execution_timeout).is_ok()) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job prepare value is invalid"));
  }
  return DistributedVectorGroupedAggregateShuffleJobControlRequest{std::move(prepare)};
}

common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse>
encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(
    const DistributedVectorGroupedAggregateShuffleJobControlResponse& response) {
  const common::Status valid = validate_response(response);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  const auto action = action_code(response.action);
  if (!action.has_value())
    return common::make_unexpected(action.error());
  const auto status = status_code(response.status_code);
  if (!status.has_value())
    return common::make_unexpected(status.error());
  try {
    std::vector<std::byte> bytes(kResponseFrameLength);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kResponseMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kResponseHeaderLength);
    if (write.is_ok())
      write = writer.write_u64_le(kResponseFrameLength);
    if (write.is_ok())
      write = writer.write_u8(*action);
    if (write.is_ok())
      write = writer.write_u8(*status);
    if (write.is_ok())
      write = writer.write_u16_le(
          response.reducer_shuffle_endpoint.has_value() ? kResponseEndpointFlag : 0U);
    if (write.is_ok())
      write = writer.zero_fill(4U);
    if (write.is_ok())
      write = writer.write_u64_le(response.coordinator_node_id);
    if (write.is_ok())
      write = writer.write_u64_le(response.target_node_id);
    if (write.is_ok())
      write = writer.write_exact(response.query_id.bytes());
    const auto endpoint = response.reducer_shuffle_endpoint.value_or(network::Ipv4Endpoint{});
    if (write.is_ok())
      write = writer.write_exact(std::as_bytes(std::span{endpoint.address}));
    if (write.is_ok())
      write = writer.write_u16_le(endpoint.port);
    if (write.is_ok())
      write = writer.zero_fill(22U);
    if (write.is_ok())
      write = writer.write_u32_le(
          common::crc32c(common::ByteView{bytes}.first(kResponseHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_u32_le(
          common::crc32c(common::ByteView{bytes}.first(bytes.size() - kTrailerLength)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("grouped shuffle reducer-job response layout failed"));
    return EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle reducer-job response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("grouped shuffle reducer-job response exceeds limits"));
  }
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1_exact(
    const common::ByteView bytes) {
  if (bytes.size() != kResponseFrameLength)
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job response length is invalid"));
  if (!std::ranges::equal(bytes.first(kResponseMagic.size()), kResponseMagic))
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job response magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kResponseHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kResponseHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job response header checksum differs"));
  }
  common::ByteReader trailer{bytes.last(kTrailerLength)};
  const auto stored_frame_crc = trailer.read_u32_le();
  if (!stored_frame_crc.has_value() ||
      *stored_frame_crc != common::crc32c(bytes.first(bytes.size() - kTrailerLength))) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job response checksum differs"));
  }
  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kResponseMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto action_value = reader.read_u8();
  const auto status_value = reader.read_u8();
  const auto flags = reader.read_u16_le();
  const auto prefix_reserved = reader.read_exact(4U);
  const auto coordinator_node_id = reader.read_u64_le();
  const auto target_node_id = reader.read_u64_le();
  const auto query_id_bytes = reader.read_exact(common::Uuid::kSize);
  const auto address_bytes = reader.read_exact(4U);
  const auto port = reader.read_u16_le();
  const auto reserved = reader.read_exact(22U);
  static_cast<void>(reader.skip(4U + kTrailerLength));
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !action_value.has_value() || !status_value.has_value() ||
      !flags.has_value() || !prefix_reserved.has_value() || !coordinator_node_id.has_value() ||
      !target_node_id.has_value() || !query_id_bytes.has_value() || !address_bytes.has_value() ||
      !port.has_value() || !reserved.has_value()) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job response header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("grouped shuffle reducer-job response version is unsupported"));
  if (*header_length != kResponseHeaderLength || *frame_length != kResponseFrameLength ||
      (*flags & ~kResponseEndpointFlag) != 0U || !all_zero(*prefix_reserved) ||
      !all_zero(*reserved)) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job response header is invalid"));
  }
  const auto action = decode_action(*action_value);
  if (!action.has_value())
    return common::make_unexpected(action.error());
  const auto status = decode_status(*status_value);
  if (!status.has_value())
    return common::make_unexpected(status.error());
  common::Uuid::Bytes query_owned{};
  std::ranges::copy(*query_id_bytes, query_owned.begin());
  std::array<std::uint8_t, 4U> address{};
  for (std::size_t index = 0U; index < address.size(); ++index)
    address[index] = std::to_integer<std::uint8_t>((*address_bytes)[index]);
  const bool has_endpoint = (*flags & kResponseEndpointFlag) != 0U;
  if (!has_endpoint && (!all_zero(*address_bytes) || *port != 0U))
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job response endpoint is noncanonical"));
  DistributedVectorGroupedAggregateShuffleJobControlResponse response{
      .action = *action,
      .status_code = *status,
      .query_id = common::Uuid{query_owned},
      .coordinator_node_id = *coordinator_node_id,
      .target_node_id = *target_node_id,
      .reducer_shuffle_endpoint = has_endpoint
                                      ? std::optional<network::Ipv4Endpoint>{network::Ipv4Endpoint{
                                            .address = address, .port = *port}}
                                      : std::nullopt};
  if (!validate_response(response).is_ok())
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job response value is invalid"));
  return response;
}

} // namespace chronos::cluster
