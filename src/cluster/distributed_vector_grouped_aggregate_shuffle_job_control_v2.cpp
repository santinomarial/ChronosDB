#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

using namespace distributed_vector_grouped_aggregate_shuffle_job_control_v2_format;

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

[[nodiscard]] bool valid_endpoint(const network::Ipv4Endpoint endpoint) noexcept {
  return endpoint.port != 0U && std::ranges::any_of(endpoint.address, [](const std::uint8_t value) {
           return value != 0U;
         });
}

[[nodiscard]] common::Status
validate_routes(const DistributedVectorGroupedAggregateShuffleJobInstallRoutes& request,
                const std::size_t maximum_routes) noexcept {
  if (request.query_id.is_nil() || request.coordinator_node_id == 0U ||
      request.target_node_id == 0U || request.coordinator_node_id == request.target_node_id ||
      request.routes.size() > maximum_routes ||
      !std::ranges::is_sorted(request.routes, {},
                              &DistributedVectorGroupedAggregateShuffleJobRoute::node_id) ||
      std::ranges::adjacent_find(request.routes, {},
                                 &DistributedVectorGroupedAggregateShuffleJobRoute::node_id) !=
          request.routes.end() ||
      std::ranges::any_of(request.routes, [](const auto& route) {
        return route.node_id == 0U || !valid_endpoint(route.endpoint);
      })) {
    return invalid("grouped shuffle reducer-job route installation is invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<std::uint8_t> encode_status(const common::StatusCode code) {
  const auto value = static_cast<std::uint8_t>(code);
  if (value > static_cast<std::uint8_t>(common::StatusCode::kInternal))
    return common::make_unexpected(invalid("grouped shuffle reducer-job status is invalid"));
  return value;
}

[[nodiscard]] common::Result<common::StatusCode> decode_status(const std::uint8_t value) {
  if (value > static_cast<std::uint8_t>(common::StatusCode::kInternal)) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job response status is unknown"));
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
encode_distributed_vector_grouped_aggregate_shuffle_job_install_routes_v2(
    const DistributedVectorGroupedAggregateShuffleJobInstallRoutes& request) {
  const common::Status valid = validate_routes(request, kMaximumRoutes);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  const auto routes_length =
      common::checked_multiply(request.routes.size(), kRouteDescriptorLength);
  if (!routes_length.has_value())
    return common::make_unexpected(exhausted("grouped shuffle route payload is too large"));
  const auto route_payload_length = *routes_length;
  const auto frame_length =
      common::checked_add(kHeaderLength + kTrailerLength, route_payload_length);
  if (!frame_length.has_value() || *frame_length > kMaximumFrameLength)
    return common::make_unexpected(exhausted("grouped shuffle route frame is too large"));
  try {
    std::vector<std::byte> bytes(*frame_length);
    common::ByteWriter route_writer{
        common::MutableByteView{bytes}.subspan(kHeaderLength, route_payload_length)};
    common::Status route_write = common::Status::ok();
    for (const auto& route : request.routes) {
      if (route_write.is_ok())
        route_write = route_writer.write_u64_le(route.node_id);
      if (route_write.is_ok())
        route_write = route_writer.write_exact(std::as_bytes(std::span{route.endpoint.address}));
      if (route_write.is_ok())
        route_write = route_writer.write_u16_le(route.endpoint.port);
      if (route_write.is_ok())
        route_write = route_writer.zero_fill(2U);
    }
    if (!route_write.is_ok() || !route_writer.full())
      return common::make_unexpected(invalid("grouped shuffle route payload layout failed"));
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
      write = writer.write_u8(static_cast<std::uint8_t>(
          DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes));
    if (write.is_ok())
      write = writer.zero_fill(7U);
    if (write.is_ok())
      write = writer.write_u64_le(request.coordinator_node_id);
    if (write.is_ok())
      write = writer.write_u64_le(request.target_node_id);
    if (write.is_ok())
      write = writer.write_exact(request.query_id.bytes());
    if (write.is_ok())
      write = writer.zero_fill(16U);
    if (write.is_ok())
      write = writer.write_u64_le(route_payload_length);
    if (write.is_ok())
      write = writer.write_u64_le(request.routes.size());
    const common::ByteView route_bytes =
        common::ByteView{bytes}.subspan(kHeaderLength, route_payload_length);
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(route_bytes));
    if (write.is_ok())
      write = writer.zero_fill(24U);
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_exact(route_bytes);
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("grouped shuffle route frame layout failed"));
    return EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle route encoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle route encoding exceeds limits"));
  }
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlRequest>
decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v2_exact(
    const common::ByteView bytes,
    const DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits) {
  const common::Status valid_limits =
      validate_distributed_vector_grouped_aggregate_shuffle_job_control_decode_limits(limits);
  if (!valid_limits.is_ok())
    return common::make_unexpected(valid_limits);
  if (bytes.size() < kHeaderLength + kTrailerLength || bytes.size() > kMaximumFrameLength)
    return common::make_unexpected(corruption("grouped shuffle route frame length is invalid"));
  if (bytes.size() > limits.maximum_frame_length)
    return common::make_unexpected(exhausted("grouped shuffle route frame exceeds caller limit"));
  if (!std::ranges::equal(bytes.first(kRequestMagic.size()), kRequestMagic))
    return common::make_unexpected(corruption("grouped shuffle route frame magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kHeaderCrcOffset))) {
    return common::make_unexpected(corruption("grouped shuffle route header checksum differs"));
  }
  common::ByteReader trailer{bytes.last(kTrailerLength)};
  const auto stored_frame_crc = trailer.read_u32_le();
  if (!stored_frame_crc.has_value() ||
      *stored_frame_crc != common::crc32c(bytes.first(bytes.size() - kTrailerLength))) {
    return common::make_unexpected(corruption("grouped shuffle route frame checksum differs"));
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
  const auto fixed_reserved = reader.read_exact(16U);
  const auto routes_length = reader.read_u64_le();
  const auto route_count = reader.read_u64_le();
  const auto routes_crc = reader.read_u32_le();
  const auto reserved = reader.read_exact(24U);
  static_cast<void>(reader.skip(4U));
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !action.has_value() || !action_reserved.has_value() ||
      !coordinator.has_value() || !target.has_value() || !query_id.has_value() ||
      !fixed_reserved.has_value() || !routes_length.has_value() || !route_count.has_value() ||
      !routes_crc.has_value() || !reserved.has_value()) {
    return common::make_unexpected(corruption("grouped shuffle route header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("grouped shuffle route version is unsupported"));
  if (*routes_length > std::numeric_limits<std::size_t>::max() ||
      *route_count > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(corruption("grouped shuffle route lengths overflow"));
  }
  const auto route_size = static_cast<std::size_t>(*routes_length);
  const auto count = static_cast<std::size_t>(*route_count);
  const auto expected_route_size = common::checked_multiply(count, kRouteDescriptorLength);
  const auto expected_frame =
      expected_route_size.has_value()
          ? common::checked_add(kHeaderLength + kTrailerLength, *expected_route_size)
          : std::nullopt;
  if (*header_length != kHeaderLength || *frame_length != bytes.size() ||
      *action != static_cast<std::uint8_t>(
                     DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes) ||
      !all_zero(*action_reserved) || !all_zero(*fixed_reserved) || !all_zero(*reserved) ||
      !expected_route_size.has_value() || route_size != *expected_route_size ||
      !expected_frame.has_value() || *expected_frame != bytes.size()) {
    return common::make_unexpected(corruption("grouped shuffle route header is noncanonical"));
  }
  if (count > limits.maximum_routes)
    return common::make_unexpected(exhausted("grouped shuffle route count exceeds caller limit"));
  const common::ByteView route_bytes = bytes.subspan(kHeaderLength, route_size);
  if (*routes_crc != common::crc32c(route_bytes))
    return common::make_unexpected(corruption("grouped shuffle route payload checksum differs"));

  try {
    std::vector<DistributedVectorGroupedAggregateShuffleJobRoute> routes;
    routes.reserve(count);
    common::ByteReader route_reader{route_bytes};
    for (std::size_t index = 0U; index < count; ++index) {
      const auto node = route_reader.read_u64_le();
      const auto address_bytes = route_reader.read_exact(4U);
      const auto port = route_reader.read_u16_le();
      const auto descriptor_reserved = route_reader.read_exact(2U);
      if (!node.has_value() || !address_bytes.has_value() || !port.has_value() ||
          !descriptor_reserved.has_value() || !all_zero(*descriptor_reserved)) {
        return common::make_unexpected(corruption("grouped shuffle route descriptor is invalid"));
      }
      std::array<std::uint8_t, 4U> address{};
      for (std::size_t byte = 0U; byte < address.size(); ++byte)
        address[byte] = std::to_integer<std::uint8_t>((*address_bytes)[byte]);
      routes.push_back({.node_id = *node, .endpoint = {.address = address, .port = *port}});
    }
    DistributedVectorGroupedAggregateShuffleJobInstallRoutes request{.query_id = *query_id,
                                                                     .coordinator_node_id =
                                                                         *coordinator,
                                                                     .target_node_id = *target,
                                                                     .routes = std::move(routes)};
    if (!validate_routes(request, limits.maximum_routes).is_ok())
      return common::make_unexpected(corruption("grouped shuffle route value is invalid"));
    return DistributedVectorGroupedAggregateShuffleJobControlRequest{std::move(request)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle route decoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle route decoding exceeds limits"));
  }
}

common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse>
encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v2(
    const DistributedVectorGroupedAggregateShuffleJobControlResponse& response) {
  if (response.action != DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes ||
      response.query_id.is_nil() || response.coordinator_node_id == 0U ||
      response.target_node_id == 0U || response.coordinator_node_id == response.target_node_id ||
      response.reducer_shuffle_endpoint.has_value()) {
    return common::make_unexpected(invalid("grouped shuffle route response is invalid"));
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
      return common::make_unexpected(invalid("grouped shuffle route response layout failed"));
    return EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle route response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle route response exceeds limits"));
  }
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v2_exact(
    const common::ByteView bytes) {
  if (bytes.size() != kResponseFrameLength ||
      !std::ranges::equal(bytes.first(kResponseMagic.size()), kResponseMagic)) {
    return common::make_unexpected(corruption("grouped shuffle route response framing is invalid"));
  }
  common::ByteReader header_crc_reader{bytes.subspan(kResponseHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  common::ByteReader trailer{bytes.last(kTrailerLength)};
  const auto stored_frame_crc = trailer.read_u32_le();
  if (!stored_header_crc.has_value() || !stored_frame_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kResponseHeaderCrcOffset)) ||
      *stored_frame_crc != common::crc32c(bytes.first(bytes.size() - kTrailerLength))) {
    return common::make_unexpected(corruption("grouped shuffle route response checksum differs"));
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
    return common::make_unexpected(corruption("grouped shuffle route response is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("grouped shuffle route response version is unsupported"));
  const auto decoded_status = decode_status(*status);
  if (!decoded_status.has_value())
    return common::make_unexpected(decoded_status.error());
  if (*header_length != kResponseHeaderLength || *frame_length != kResponseFrameLength ||
      *action != static_cast<std::uint8_t>(
                     DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes) ||
      !all_zero(*reserved) || !all_zero(*suffix_reserved) || query_id->is_nil() ||
      *coordinator == 0U || *target == 0U || *coordinator == *target) {
    return common::make_unexpected(corruption("grouped shuffle route response is noncanonical"));
  }
  return DistributedVectorGroupedAggregateShuffleJobControlResponse{
      .action = DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
      .status_code = *decoded_status,
      .query_id = *query_id,
      .coordinator_node_id = *coordinator,
      .target_node_id = *target};
}

} // namespace chronos::cluster
