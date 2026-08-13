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
inline constexpr std::size_t kMinimumDispatchFrameLength =
    distributed_vector_fragment_format::kHeaderLength + sizeof(std::uint32_t) +
    distributed_vector_plan_format::kHeaderLength + distributed_vector_plan_format::kTrailerLength +
    distributed_vector_fragment_format::kTrailerLength;
inline constexpr std::size_t kMinimumResultSchemaFrameLength =
    distributed_vector_result_schema_format::kHeaderLength +
    distributed_vector_result_schema_format::kDescriptorFixedLength + 1U +
    distributed_vector_result_schema_format::kTrailerLength;
inline constexpr std::size_t kMinimumFrameLength =
    distributed_vector_fragment_v2_format::kHeaderLength + kMinimumDispatchFrameLength +
    kMinimumResultSchemaFrameLength + distributed_vector_fragment_v2_format::kTrailerLength;
[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}
[[nodiscard]] common::Status corrupt(const char* message) {
  return {common::StatusCode::kCorruption, message};
}
[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status
validate_decode_limits(const DistributedVectorFragmentV2DecodeLimits& limits) {
  if (limits.maximum_frame_length < kMinimumFrameLength ||
      limits.maximum_frame_length > distributed_vector_fragment_v2_format::kMaximumFrameLength ||
      limits.dispatch.maximum_frame_length < kMinimumDispatchFrameLength ||
      limits.dispatch.maximum_frame_length >
          distributed_vector_fragment_format::kMaximumFrameLength ||
      limits.dispatch.maximum_projection_columns == 0U ||
      limits.dispatch.maximum_projection_columns >
          distributed_vector_fragment_format::kMaximumProjectionColumns ||
      limits.dispatch.plan.maximum_input_columns == 0U ||
      limits.dispatch.plan.maximum_input_columns >
          distributed_vector_plan_format::kMaximumInputColumns ||
      limits.dispatch.plan.maximum_output_columns == 0U ||
      limits.dispatch.plan.maximum_output_columns >
          distributed_vector_plan_format::kMaximumOutputColumns ||
      limits.dispatch.plan.maximum_row_outputs == 0U ||
      limits.dispatch.plan.maximum_row_outputs >
          distributed_vector_plan_format::kMaximumRowOutputs ||
      limits.dispatch.plan.maximum_group_keys == 0U ||
      limits.dispatch.plan.maximum_group_keys > distributed_vector_plan_format::kMaximumGroupKeys ||
      limits.dispatch.plan.maximum_aggregates == 0U ||
      limits.dispatch.plan.maximum_aggregates >
          distributed_vector_plan_format::kMaximumAggregates ||
      limits.dispatch.plan.maximum_order_keys > distributed_vector_plan_format::kMaximumOrderKeys ||
      limits.result_schema.maximum_frame_length < kMinimumResultSchemaFrameLength ||
      limits.result_schema.maximum_frame_length >
          distributed_vector_result_schema_format::kMaximumFrameLength ||
      limits.result_schema.maximum_columns == 0U ||
      limits.result_schema.maximum_columns >
          distributed_vector_result_schema_format::kMaximumColumns ||
      limits.result_schema.maximum_name_length == 0U ||
      limits.result_schema.maximum_name_length >
          distributed_vector_result_schema_format::kMaximumNameLength) {
    return invalid("distributed vector fragment v2 limits are invalid");
  }
  return common::Status::ok();
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
  const common::Status limits_status = validate_decode_limits(limits);
  if (!limits_status.is_ok())
    return common::make_unexpected(limits_status);
  if (bytes.size() < kMinimumFrameLength ||
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
      *total != bytes.size() || *dispatch_length < kMinimumDispatchFrameLength ||
      *dispatch_length > distributed_vector_fragment_format::kMaximumFrameLength ||
      *schema_length < kMinimumResultSchemaFrameLength ||
      *schema_length > distributed_vector_result_schema_format::kMaximumFrameLength ||
      *dispatch_length + *schema_length + distributed_vector_fragment_v2_format::kHeaderLength +
              distributed_vector_fragment_v2_format::kTrailerLength !=
          *total ||
      !std::ranges::all_of(*reserved, [](std::byte value) { return value == std::byte{}; }))
    return common::make_unexpected(corrupt("distributed vector fragment v2 header is invalid"));
  if (*dispatch_length > limits.dispatch.maximum_frame_length ||
      *schema_length > limits.result_schema.maximum_frame_length) {
    return common::make_unexpected(
        exhausted("distributed vector fragment v2 nested value exceeds caller limit"));
  }
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

DistributedVectorFragmentV2Reader::DistributedVectorFragmentV2Reader(
    const DistributedVectorFragmentV2DecodeLimits limits) noexcept
    : limits_(limits) {}

common::Result<DistributedVectorFragmentV2ReadStep>
DistributedVectorFragmentV2Reader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  const common::Status limits_status = validate_decode_limits(limits_);
  if (!limits_status.is_ok())
    return common::make_unexpected(limits_status);

  std::size_t consumed{};
  if (frame_.empty()) {
    const std::size_t copied = std::min(
        bytes.size(), distributed_vector_fragment_v2_format::kHeaderLength - header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != distributed_vector_fragment_v2_format::kHeaderLength)
      return DistributedVectorFragmentV2ReadStep{.consumed_bytes = consumed,
                                                 .dispatch = std::nullopt};

    common::ByteReader crc_reader{common::ByteView{header_}.subspan(kHeaderCrcOffset, 4U)};
    const auto header_crc = crc_reader.read_u32_le();
    common::ByteReader reader{header_};
    const auto magic = reader.read_exact(kMagic.size());
    const auto major = reader.read_u16_le();
    const auto minor = reader.read_u16_le();
    const auto header_length = reader.read_u32_le();
    const auto frame_length = reader.read_u64_le();
    const auto dispatch_length = reader.read_u64_le();
    const auto schema_length = reader.read_u64_le();
    static_cast<void>(reader.skip(8U));
    static_cast<void>(reader.skip(4U));
    const auto reserved = reader.read_exact(12U);
    if (!magic.has_value() || !std::ranges::equal(*magic, kMagic) || !header_crc.has_value() ||
        *header_crc != common::crc32c(common::ByteView{header_}.first(kHeaderCrcOffset)) ||
        !major.has_value() || !minor.has_value()) {
      failure_ = corrupt("distributed vector fragment v2 streaming header is invalid");
      return common::make_unexpected(*failure_);
    }
    if (*major != distributed_vector_fragment_v2_format::kMajor ||
        *minor != distributed_vector_fragment_v2_format::kMinor) {
      failure_ = common::Status{common::StatusCode::kNotSupported,
                                "distributed vector fragment v2 version is unsupported"};
      return common::make_unexpected(*failure_);
    }
    if (!header_length.has_value() ||
        *header_length != distributed_vector_fragment_v2_format::kHeaderLength ||
        !frame_length.has_value() || *frame_length < kMinimumFrameLength ||
        *frame_length > distributed_vector_fragment_v2_format::kMaximumFrameLength ||
        !dispatch_length.has_value() || *dispatch_length < kMinimumDispatchFrameLength ||
        *dispatch_length > distributed_vector_fragment_format::kMaximumFrameLength ||
        !schema_length.has_value() || *schema_length < kMinimumResultSchemaFrameLength ||
        *schema_length > distributed_vector_result_schema_format::kMaximumFrameLength ||
        *dispatch_length + *schema_length + distributed_vector_fragment_v2_format::kHeaderLength +
                distributed_vector_fragment_v2_format::kTrailerLength !=
            *frame_length ||
        !reserved.has_value() || !std::ranges::all_of(*reserved, [](const std::byte value) {
          return value == std::byte{};
        })) {
      failure_ = corrupt("distributed vector fragment v2 streaming header is invalid");
      return common::make_unexpected(*failure_);
    }
    if (*frame_length > limits_.maximum_frame_length ||
        *dispatch_length > limits_.dispatch.maximum_frame_length ||
        *schema_length > limits_.result_schema.maximum_frame_length) {
      failure_ = exhausted("distributed vector fragment v2 exceeds reader limits");
      return common::make_unexpected(*failure_);
    }
    try {
      frame_.resize(static_cast<std::size_t>(*frame_length));
    } catch (const std::bad_alloc&) {
      failure_ = exhausted("distributed vector fragment v2 reader allocation failed");
      return common::make_unexpected(*failure_);
    } catch (const std::length_error&) {
      failure_ = exhausted("distributed vector fragment v2 reader exceeds container limits");
      return common::make_unexpected(*failure_);
    }
    std::ranges::copy(header_, frame_.begin());
    frame_bytes_ = header_.size();
  }

  const common::ByteView remaining = bytes.subspan(consumed);
  const std::size_t copied = std::min(remaining.size(), frame_.size() - frame_bytes_);
  std::ranges::copy(remaining.first(copied),
                    frame_.begin() + static_cast<std::ptrdiff_t>(frame_bytes_));
  frame_bytes_ += copied;
  consumed += copied;
  if (frame_bytes_ != frame_.size())
    return DistributedVectorFragmentV2ReadStep{.consumed_bytes = consumed,
                                               .dispatch = std::nullopt};
  auto decoded = decode_distributed_vector_fragment_dispatch_v2_exact(frame_, limits_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  DistributedVectorFragmentDispatchV2 result = std::move(*decoded);
  frame_.clear();
  frame_bytes_ = 0U;
  header_bytes_ = 0U;
  return DistributedVectorFragmentV2ReadStep{.consumed_bytes = consumed,
                                             .dispatch = std::move(result)};
}

std::size_t DistributedVectorFragmentV2Reader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool DistributedVectorFragmentV2Reader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorFragmentV2WriteCursor::DistributedVectorFragmentV2WriteCursor(
    EncodedDistributedVectorFragmentDispatchV2 encoded) noexcept
    : encoded_(std::move(encoded)) {}

DistributedVectorFragmentV2WriteCursor::DistributedVectorFragmentV2WriteCursor(
    DistributedVectorFragmentV2WriteCursor&& other) noexcept
    : encoded_(std::move(other.encoded_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_.bytes().size();
}

DistributedVectorFragmentV2WriteCursor& DistributedVectorFragmentV2WriteCursor::operator=(
    DistributedVectorFragmentV2WriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_ = std::move(other.encoded_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_.bytes().size();
  }
  return *this;
}

common::Result<DistributedVectorFragmentV2WriteCursor>
DistributedVectorFragmentV2WriteCursor::create(
    const DistributedVectorFragmentDispatchV2& dispatch) {
  auto encoded = encode_distributed_vector_fragment_dispatch_v2(dispatch);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedVectorFragmentV2WriteCursor{std::move(*encoded)};
}

common::ByteView DistributedVectorFragmentV2WriteCursor::pending_write() const noexcept {
  return encoded_.bytes().subspan(written_bytes_);
}

common::Status
DistributedVectorFragmentV2WriteCursor::consume_written(const std::size_t bytes) noexcept {
  if (bytes > encoded_.bytes().size() - written_bytes_)
    return invalid("written byte count exceeds distributed vector fragment v2 frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t DistributedVectorFragmentV2WriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedVectorFragmentV2WriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_.bytes().size();
}

} // namespace chronos::query
