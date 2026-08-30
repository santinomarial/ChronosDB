#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <new>
#include <ranges>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

using namespace distributed_vector_grouped_aggregate_shuffle_job_control_v4_format;

inline constexpr std::size_t kHeaderCrcOffset = 124U;
inline constexpr std::size_t kResponseHeaderCrcOffset = 92U;

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

[[nodiscard]] bool all_zero(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{}; });
}

[[nodiscard]] common::Status
validate_renewal(const DistributedVectorGroupedAggregateShuffleJobRenewLease& request) {
  if (request.query_id.is_nil() || request.coordinator_node_id == 0U ||
      request.target_node_id == 0U || request.coordinator_node_id == request.target_node_id ||
      request.lease_duration.count() <= 0 ||
      request.lease_duration > distributed_vector_grouped_aggregate_shuffle_job_control_format::
                                   kMaximumExecutionTimeout) {
    return invalid("grouped shuffle reducer-job lease renewal is invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<std::uint8_t> encode_status(const common::StatusCode code) {
  const auto value = static_cast<std::uint8_t>(code);
  if (value > static_cast<std::uint8_t>(common::StatusCode::kInternal))
    return common::make_unexpected(invalid("grouped shuffle lease-renewal status is invalid"));
  return value;
}

[[nodiscard]] common::Result<common::StatusCode> decode_status(const std::uint8_t value) {
  if (value > static_cast<std::uint8_t>(common::StatusCode::kInternal)) {
    return common::make_unexpected(
        corruption("grouped shuffle lease-renewal response status is unknown"));
  }
  return static_cast<common::StatusCode>(value);
}

[[nodiscard]] common::Result<common::Uuid> read_uuid(common::ByteReader& reader) {
  const auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  return common::Uuid{owned};
}

} // namespace

common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
encode_distributed_vector_grouped_aggregate_shuffle_job_renew_lease_v4(
    const DistributedVectorGroupedAggregateShuffleJobRenewLease& request) {
  const common::Status valid = validate_renewal(request);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  try {
    std::vector<std::byte> bytes(kFrameLength);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kRequestMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kHeaderLength);
    if (write.is_ok())
      write = writer.write_u64_le(kFrameLength);
    if (write.is_ok())
      write = writer.write_u8(static_cast<std::uint8_t>(
          DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease));
    if (write.is_ok())
      write = writer.zero_fill(7U);
    if (write.is_ok())
      write = writer.write_u64_le(request.coordinator_node_id);
    if (write.is_ok())
      write = writer.write_u64_le(request.target_node_id);
    if (write.is_ok())
      write = writer.write_exact(request.query_id.bytes());
    if (write.is_ok())
      write = writer.write_u64_le(static_cast<std::uint64_t>(request.lease_duration.count()));
    if (write.is_ok())
      write = writer.zero_fill(52U);
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("grouped shuffle lease-renewal layout failed"));
    return EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle lease-renewal encoding allocation failed"));
  }
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlRequest>
decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v4_exact(
    const common::ByteView bytes,
    const DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits) {
  const common::Status valid_limits =
      validate_distributed_vector_grouped_aggregate_shuffle_job_control_decode_limits(limits);
  if (!valid_limits.is_ok())
    return common::make_unexpected(valid_limits);
  if (bytes.size() != kFrameLength || bytes.size() > limits.maximum_frame_length ||
      !std::ranges::equal(bytes.first(kRequestMagic.size()), kRequestMagic)) {
    return common::make_unexpected(corruption("grouped shuffle lease-renewal framing is invalid"));
  }
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto header_crc = header_crc_reader.read_u32_le();
  common::ByteReader trailer{bytes.last(kTrailerLength)};
  const auto frame_crc = trailer.read_u32_le();
  if (!header_crc.has_value() || !frame_crc.has_value() ||
      *header_crc != common::crc32c(bytes.first(kHeaderCrcOffset)) ||
      *frame_crc != common::crc32c(bytes.first(bytes.size() - kTrailerLength))) {
    return common::make_unexpected(corruption("grouped shuffle lease-renewal checksum differs"));
  }

  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kRequestMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto action = reader.read_u8();
  const auto action_reserved = reader.read_exact(7U);
  const auto coordinator = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto query_id = read_uuid(reader);
  const auto lease_duration = reader.read_u64_le();
  const auto reserved = reader.read_exact(52U);
  static_cast<void>(reader.skip(4U + kTrailerLength));
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !action.has_value() || !action_reserved.has_value() ||
      !coordinator.has_value() || !target.has_value() || !query_id.has_value() ||
      !lease_duration.has_value() || !reserved.has_value()) {
    return common::make_unexpected(corruption("grouped shuffle lease renewal is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("grouped shuffle lease-renewal version is unsupported"));
  if (*lease_duration >
      static_cast<std::uint64_t>(
          distributed_vector_grouped_aggregate_shuffle_job_control_format::kMaximumExecutionTimeout
              .count())) {
    return common::make_unexpected(
        corruption("grouped shuffle lease-renewal duration is noncanonical"));
  }
  DistributedVectorGroupedAggregateShuffleJobRenewLease request{
      .query_id = *query_id,
      .coordinator_node_id = *coordinator,
      .target_node_id = *target,
      .lease_duration = std::chrono::milliseconds{*lease_duration}};
  if (*header_length != kHeaderLength || *frame_length != kFrameLength ||
      *action != static_cast<std::uint8_t>(
                     DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease) ||
      !all_zero(*action_reserved) || !all_zero(*reserved) || !validate_renewal(request).is_ok()) {
    return common::make_unexpected(corruption("grouped shuffle lease renewal is noncanonical"));
  }
  return DistributedVectorGroupedAggregateShuffleJobControlRequest{request};
}

common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse>
encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v4(
    const DistributedVectorGroupedAggregateShuffleJobControlResponse& response) {
  if (response.action != DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease ||
      response.query_id.is_nil() || response.coordinator_node_id == 0U ||
      response.target_node_id == 0U || response.coordinator_node_id == response.target_node_id ||
      response.reducer_shuffle_endpoint.has_value()) {
    return common::make_unexpected(invalid("grouped shuffle lease-renewal response is invalid"));
  }
  const auto status = encode_status(response.status_code);
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
      write = writer.write_u8(static_cast<std::uint8_t>(response.action));
    if (write.is_ok())
      write = writer.write_u8(*status);
    if (write.is_ok())
      write = writer.zero_fill(6U);
    if (write.is_ok())
      write = writer.write_u64_le(response.coordinator_node_id);
    if (write.is_ok())
      write = writer.write_u64_le(response.target_node_id);
    if (write.is_ok())
      write = writer.write_exact(response.query_id.bytes());
    if (write.is_ok())
      write = writer.zero_fill(28U);
    if (write.is_ok())
      write = writer.write_u32_le(
          common::crc32c(common::ByteView{bytes}.first(kResponseHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(
          invalid("grouped shuffle lease-renewal response layout failed"));
    return EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle lease-renewal response allocation failed"));
  }
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v4_exact(
    const common::ByteView bytes) {
  if (bytes.size() != kResponseFrameLength ||
      !std::ranges::equal(bytes.first(kResponseMagic.size()), kResponseMagic)) {
    return common::make_unexpected(
        corruption("grouped shuffle lease-renewal response framing is invalid"));
  }
  common::ByteReader header_crc_reader{bytes.subspan(kResponseHeaderCrcOffset, 4U)};
  const auto header_crc = header_crc_reader.read_u32_le();
  common::ByteReader trailer{bytes.last(kTrailerLength)};
  const auto frame_crc = trailer.read_u32_le();
  if (!header_crc.has_value() || !frame_crc.has_value() ||
      *header_crc != common::crc32c(bytes.first(kResponseHeaderCrcOffset)) ||
      *frame_crc != common::crc32c(bytes.first(bytes.size() - kTrailerLength))) {
    return common::make_unexpected(
        corruption("grouped shuffle lease-renewal response checksum differs"));
  }
  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kResponseMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto action = reader.read_u8();
  const auto status = reader.read_u8();
  const auto reserved = reader.read_exact(6U);
  const auto coordinator = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto query_id = read_uuid(reader);
  const auto suffix_reserved = reader.read_exact(28U);
  static_cast<void>(reader.skip(4U + kTrailerLength));
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !action.has_value() || !status.has_value() ||
      !reserved.has_value() || !coordinator.has_value() || !target.has_value() ||
      !query_id.has_value() || !suffix_reserved.has_value()) {
    return common::make_unexpected(
        corruption("grouped shuffle lease-renewal response is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("grouped shuffle lease-renewal response version is unsupported"));
  const auto decoded_status = decode_status(*status);
  if (!decoded_status.has_value())
    return common::make_unexpected(decoded_status.error());
  if (*header_length != kResponseHeaderLength || *frame_length != kResponseFrameLength ||
      *action != static_cast<std::uint8_t>(
                     DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease) ||
      !all_zero(*reserved) || !all_zero(*suffix_reserved) || query_id->is_nil() ||
      *coordinator == 0U || *target == 0U || *coordinator == *target) {
    return common::make_unexpected(
        corruption("grouped shuffle lease-renewal response is noncanonical"));
  }
  return DistributedVectorGroupedAggregateShuffleJobControlResponse{
      .action = DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease,
      .status_code = *decoded_status,
      .query_id = *query_id,
      .coordinator_node_id = *coordinator,
      .target_node_id = *target};
}

} // namespace chronos::cluster
