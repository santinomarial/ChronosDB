#include "chronos/query/distributed_vector_fragment_v2.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace chronos::query {
namespace {
inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'F'}, std::byte{'D'},
                                                  std::byte{'S'}, std::byte{'2'}};
inline constexpr std::size_t kHeaderCrcOffset = 48U;
[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}
[[nodiscard]] common::Status corrupt(const char* message) {
  return {common::StatusCode::kCorruption, message};
}
[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}
} // namespace

EncodedDistributedVectorFragmentDispatchV2::EncodedDistributedVectorFragmentDispatchV2(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}
common::ByteView EncodedDistributedVectorFragmentDispatchV2::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedVectorFragmentDispatchV2>
encode_distributed_vector_fragment_dispatch_v2(const DistributedVectorFragmentDispatchV2& value) {
  auto dispatch = encode_distributed_vector_fragment_dispatch(value.dispatch);
  if (!dispatch.has_value())
    return common::make_unexpected(dispatch.error());
  auto result_schema = encode_distributed_vector_result_schema(value.result_schema);
  if (!result_schema.has_value())
    return common::make_unexpected(result_schema.error());
  const std::size_t total = distributed_vector_fragment_v2_format::kHeaderLength +
                            dispatch->bytes().size() + result_schema->bytes().size() +
                            distributed_vector_fragment_v2_format::kTrailerLength;
  try {
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_fragment_v2_format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_fragment_v2_format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(distributed_vector_fragment_v2_format::kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(total);
    if (status.is_ok())
      status = writer.write_u64_le(dispatch->bytes().size());
    if (status.is_ok())
      status = writer.write_u64_le(result_schema->bytes().size());
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(dispatch->bytes()));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(result_schema->bytes()));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.zero_fill(12U);
    if (status.is_ok())
      status = writer.write_exact(dispatch->bytes());
    if (status.is_ok())
      status = writer.write_exact(result_schema->bytes());
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(invalid("distributed vector fragment v2 layout failed"));
    return EncodedDistributedVectorFragmentDispatchV2{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed vector fragment v2 allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("distributed vector fragment v2 exceeds limits"));
  }
}

common::Result<DistributedVectorFragmentDispatchV2>
decode_distributed_vector_fragment_dispatch_v2_exact(
    const common::ByteView bytes, const DistributedVectorFragmentV2DecodeLimits limits) {
  constexpr std::size_t kMinimum = distributed_vector_fragment_v2_format::kHeaderLength +
                                   distributed_vector_fragment_format::kHeaderLength +
                                   distributed_vector_fragment_format::kTrailerLength +
                                   distributed_vector_result_schema_format::kHeaderLength +
                                   distributed_vector_result_schema_format::kDescriptorFixedLength +
                                   1U + distributed_vector_result_schema_format::kTrailerLength +
                                   distributed_vector_fragment_v2_format::kTrailerLength;
  if (limits.maximum_frame_length < kMinimum ||
      limits.maximum_frame_length > distributed_vector_fragment_v2_format::kMaximumFrameLength)
    return common::make_unexpected(invalid("distributed vector fragment v2 limits are invalid"));
  if (bytes.size() < kMinimum ||
      bytes.size() > distributed_vector_fragment_v2_format::kMaximumFrameLength)
    return common::make_unexpected(corrupt("distributed vector fragment v2 length is invalid"));
  if (bytes.size() > limits.maximum_frame_length)
    return common::make_unexpected(
        exhausted("distributed vector fragment v2 exceeds caller limit"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corrupt("distributed vector fragment v2 magic is invalid"));
  common::ByteReader header_crc{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kHeaderCrcOffset)))
    return common::make_unexpected(
        corrupt("distributed vector fragment v2 header checksum differs"));
  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(8U));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total = reader.read_u64_le();
  const auto dispatch_length = reader.read_u64_le();
  const auto schema_length = reader.read_u64_le();
  const auto dispatch_crc = reader.read_u32_le();
  const auto schema_crc = reader.read_u32_le();
  static_cast<void>(reader.skip(4U));
  const auto reserved = reader.read_exact(12U);
  if (!major || !minor || !header_length || !total || !dispatch_length || !schema_length ||
      !dispatch_crc || !schema_crc || !reserved)
    return common::make_unexpected(corrupt("distributed vector fragment v2 header is truncated"));
  if (*major != distributed_vector_fragment_v2_format::kMajor || *minor != 0U)
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported,
                       "distributed vector fragment v2 version is unsupported"});
  if (*header_length != distributed_vector_fragment_v2_format::kHeaderLength ||
      *total != bytes.size() ||
      *dispatch_length > distributed_vector_fragment_format::kMaximumFrameLength ||
      *schema_length > distributed_vector_result_schema_format::kMaximumFrameLength ||
      *dispatch_length + *schema_length + distributed_vector_fragment_v2_format::kHeaderLength +
              distributed_vector_fragment_v2_format::kTrailerLength !=
          *total ||
      !std::ranges::all_of(*reserved, [](std::byte value) { return value == std::byte{}; }))
    return common::make_unexpected(corrupt("distributed vector fragment v2 header is invalid"));
  common::ByteReader trailer{bytes.last(4U)};
  const auto frame_crc = trailer.read_u32_le();
  if (!frame_crc || *frame_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corrupt("distributed vector fragment v2 checksum differs"));
  const auto dispatch_bytes = bytes.subspan(distributed_vector_fragment_v2_format::kHeaderLength,
                                            static_cast<std::size_t>(*dispatch_length));
  const auto schema_bytes = bytes.subspan(distributed_vector_fragment_v2_format::kHeaderLength +
                                              static_cast<std::size_t>(*dispatch_length),
                                          static_cast<std::size_t>(*schema_length));
  if (*dispatch_crc != common::crc32c(dispatch_bytes) ||
      *schema_crc != common::crc32c(schema_bytes))
    return common::make_unexpected(
        corrupt("distributed vector fragment v2 payload checksum differs"));
  auto dispatch =
      decode_distributed_vector_fragment_dispatch_exact(dispatch_bytes, limits.dispatch);
  if (!dispatch)
    return common::make_unexpected(dispatch.error());
  auto result_schema =
      decode_distributed_vector_result_schema_exact(schema_bytes, limits.result_schema);
  if (!result_schema)
    return common::make_unexpected(result_schema.error());
  return DistributedVectorFragmentDispatchV2{std::move(*dispatch), std::move(*result_schema)};
}

} // namespace chronos::query
