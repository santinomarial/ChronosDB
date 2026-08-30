#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_transport.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

using namespace distributed_vector_grouped_aggregate_shuffle_job_control_format;

inline constexpr std::size_t kHeaderCrcOffset = 124U;

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

[[nodiscard]] bool zero(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{}; });
}

[[nodiscard]] bool nonzero_address(const common::ByteView bytes) noexcept {
  return std::ranges::any_of(bytes, [](const std::byte value) { return value != std::byte{}; });
}

} // namespace

common::Result<std::size_t>
distributed_vector_grouped_aggregate_shuffle_job_control_request_frame_length_v1(
    const common::ByteView header,
    const DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits) {
  const common::Status valid_limits =
      validate_distributed_vector_grouped_aggregate_shuffle_job_control_decode_limits(limits);
  if (!valid_limits.is_ok())
    return common::make_unexpected(valid_limits);
  if (header.size() != kHeaderLength)
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job stream header length is invalid"));
  if (!std::ranges::equal(header.first(kRequestMagic.size()), kRequestMagic))
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job stream header magic is invalid"));
  common::ByteReader crc_reader{header.subspan(kHeaderCrcOffset, sizeof(std::uint32_t))};
  const auto header_crc = crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(header.first(kHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job stream header checksum differs"));
  }

  common::ByteReader reader{header};
  static_cast<void>(reader.skip(kRequestMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto action = reader.read_u8();
  const auto action_reserved = reader.read_exact(7U);
  const auto coordinator = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto query = reader.read_exact(common::Uuid::kSize);
  const auto address = reader.read_exact(4U);
  const auto port = reader.read_u16_le();
  const auto route_reserved = reader.read_exact(2U);
  const auto timeout = reader.read_u64_le();
  const auto authority_length = reader.read_u64_le();
  const auto schema_length = reader.read_u64_le();
  const auto authority_crc = reader.read_u32_le();
  const auto schema_crc = reader.read_u32_le();
  const auto reserved = reader.read_exact(20U);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !action.has_value() || !action_reserved.has_value() ||
      !coordinator.has_value() || !target.has_value() || !query.has_value() ||
      !address.has_value() || !port.has_value() || !route_reserved.has_value() ||
      !timeout.has_value() || !authority_length.has_value() || !schema_length.has_value() ||
      !authority_crc.has_value() || !schema_crc.has_value() || !reserved.has_value()) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job stream header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("grouped shuffle reducer-job stream version is unsupported"));
  if (*frame_length > std::numeric_limits<std::size_t>::max() ||
      *authority_length > std::numeric_limits<std::size_t>::max() ||
      *schema_length > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job stream lengths overflow"));
  }
  const auto frame_size = static_cast<std::size_t>(*frame_length);
  const auto authority_size = static_cast<std::size_t>(*authority_length);
  const auto schema_size = static_cast<std::size_t>(*schema_length);
  const auto payload_size = common::checked_add(authority_size, schema_size);
  const auto expected = payload_size.has_value()
                            ? common::checked_add(kHeaderLength + kTrailerLength, *payload_size)
                            : std::nullopt;
  common::Uuid::Bytes query_bytes{};
  std::ranges::copy(*query, query_bytes.begin());
  if (*header_length != kHeaderLength || !expected.has_value() || *expected != frame_size ||
      frame_size > kMaximumFrameLength || !zero(*action_reserved) || !zero(*route_reserved) ||
      !zero(*reserved) || *coordinator == 0U || *target == 0U || *coordinator == *target ||
      common::Uuid{query_bytes}.is_nil()) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job stream header is invalid"));
  }
  if (frame_size > limits.maximum_frame_length)
    return common::make_unexpected(
        exhausted("grouped shuffle reducer-job stream frame exceeds limit"));

  if (*action ==
      static_cast<std::uint8_t>(DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal)) {
    if (frame_size != kHeaderLength + kTrailerLength || authority_size != 0U || schema_size != 0U ||
        *authority_crc != 0U || *schema_crc != 0U || !zero(*address) || *port != 0U ||
        *timeout != 0U) {
      return common::make_unexpected(
          corruption("grouped shuffle reducer-job stream seal header is noncanonical"));
    }
    return frame_size;
  }
  if (*action != static_cast<std::uint8_t>(
                     DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare)) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job stream action is unknown"));
  }
  constexpr std::size_t kMinimumSchemaFrameLength =
      query::distributed_vector_result_schema_format::kHeaderLength +
      query::distributed_vector_result_schema_format::kDescriptorFixedLength + 1U +
      query::distributed_vector_result_schema_format::kTrailerLength;
  if (authority_size <
          distributed_vector_grouped_aggregate_shuffle_authority_format::kMinimumFrameLength ||
      authority_size >
          distributed_vector_grouped_aggregate_shuffle_authority_format::kMaximumFrameLength ||
      schema_size < kMinimumSchemaFrameLength ||
      schema_size > query::distributed_vector_result_schema_format::kMaximumFrameLength ||
      !nonzero_address(*address) || *port == 0U || *timeout == 0U ||
      *timeout > static_cast<std::uint64_t>(kMaximumExecutionTimeout.count())) {
    return common::make_unexpected(
        corruption("grouped shuffle reducer-job stream prepare header is invalid"));
  }
  if (authority_size > limits.authority.maximum_frame_length ||
      schema_size > limits.result_schema.maximum_frame_length ||
      *timeout > static_cast<std::uint64_t>(limits.maximum_execution_timeout.count())) {
    return common::make_unexpected(
        exhausted("grouped shuffle reducer-job stream prepare exceeds limits"));
  }
  return frame_size;
}

common::Result<std::size_t>
distributed_vector_grouped_aggregate_shuffle_job_control_request_frame_length_v2(
    const common::ByteView header,
    const DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits) {
  namespace v2 = distributed_vector_grouped_aggregate_shuffle_job_control_v2_format;
  const common::Status valid_limits =
      validate_distributed_vector_grouped_aggregate_shuffle_job_control_decode_limits(limits);
  if (!valid_limits.is_ok())
    return common::make_unexpected(valid_limits);
  if (header.size() != v2::kHeaderLength ||
      !std::ranges::equal(header.first(v2::kRequestMagic.size()), v2::kRequestMagic)) {
    return common::make_unexpected(
        corruption("grouped shuffle route stream header framing is invalid"));
  }
  common::ByteReader crc_reader{header.subspan(kHeaderCrcOffset, sizeof(std::uint32_t))};
  const auto header_crc = crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(header.first(kHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("grouped shuffle route stream header checksum differs"));
  }
  common::ByteReader reader{header};
  static_cast<void>(reader.skip(v2::kRequestMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto action = reader.read_u8();
  const auto action_reserved = reader.read_exact(7U);
  const auto coordinator = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto query = reader.read_exact(common::Uuid::kSize);
  const auto fixed_reserved = reader.read_exact(16U);
  const auto routes_length = reader.read_u64_le();
  const auto route_count = reader.read_u64_le();
  static_cast<void>(reader.skip(4U));
  const auto reserved = reader.read_exact(24U);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !action.has_value() || !action_reserved.has_value() ||
      !coordinator.has_value() || !target.has_value() || !query.has_value() ||
      !fixed_reserved.has_value() || !routes_length.has_value() || !route_count.has_value() ||
      !reserved.has_value()) {
    return common::make_unexpected(corruption("grouped shuffle route stream header is truncated"));
  }
  if (*major != v2::kMajor || *minor != v2::kMinor)
    return common::make_unexpected(
        unsupported("grouped shuffle route stream version is unsupported"));
  if (*frame_length > std::numeric_limits<std::size_t>::max() ||
      *routes_length > std::numeric_limits<std::size_t>::max() ||
      *route_count > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(corruption("grouped shuffle route stream lengths overflow"));
  }
  const auto frame_size = static_cast<std::size_t>(*frame_length);
  const auto route_size = static_cast<std::size_t>(*routes_length);
  const auto count = static_cast<std::size_t>(*route_count);
  const auto expected_routes = common::checked_multiply(count, v2::kRouteDescriptorLength);
  const auto expected_frame =
      expected_routes.has_value()
          ? common::checked_add(v2::kHeaderLength + v2::kTrailerLength, *expected_routes)
          : std::nullopt;
  common::Uuid::Bytes query_bytes{};
  std::ranges::copy(*query, query_bytes.begin());
  if (*header_length != v2::kHeaderLength ||
      *action != static_cast<std::uint8_t>(
                     DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes) ||
      !zero(*action_reserved) || !zero(*fixed_reserved) || !zero(*reserved) || *coordinator == 0U ||
      *target == 0U || *coordinator == *target || common::Uuid{query_bytes}.is_nil() ||
      !expected_routes.has_value() || route_size != *expected_routes ||
      !expected_frame.has_value() || frame_size != *expected_frame ||
      frame_size > v2::kMaximumFrameLength) {
    return common::make_unexpected(corruption("grouped shuffle route stream header is invalid"));
  }
  if (count > limits.maximum_routes || frame_size > limits.maximum_frame_length)
    return common::make_unexpected(exhausted("grouped shuffle route stream exceeds limits"));
  return frame_size;
}

common::Result<std::size_t>
distributed_vector_grouped_aggregate_shuffle_job_control_request_frame_length_v3(
    const common::ByteView header,
    const DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits) {
  namespace v3 = distributed_vector_grouped_aggregate_shuffle_job_control_v3_format;
  const common::Status valid_limits =
      validate_distributed_vector_grouped_aggregate_shuffle_job_control_decode_limits(limits);
  if (!valid_limits.is_ok())
    return common::make_unexpected(valid_limits);
  if (header.size() != v3::kHeaderLength ||
      !std::ranges::equal(header.first(v3::kRequestMagic.size()), v3::kRequestMagic)) {
    return common::make_unexpected(
        corruption("grouped shuffle cancellation stream header framing is invalid"));
  }
  common::ByteReader crc_reader{header.subspan(kHeaderCrcOffset, sizeof(std::uint32_t))};
  const auto header_crc = crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(header.first(kHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("grouped shuffle cancellation stream header checksum differs"));
  }
  common::ByteReader reader{header};
  static_cast<void>(reader.skip(v3::kRequestMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto action = reader.read_u8();
  const auto action_reserved = reader.read_exact(7U);
  const auto coordinator = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto query = reader.read_exact(common::Uuid::kSize);
  const auto reserved = reader.read_exact(60U);
  static_cast<void>(reader.skip(4U));
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !action.has_value() || !action_reserved.has_value() ||
      !coordinator.has_value() || !target.has_value() || !query.has_value() ||
      !reserved.has_value()) {
    return common::make_unexpected(
        corruption("grouped shuffle cancellation stream header is truncated"));
  }
  if (*major != v3::kMajor || *minor != v3::kMinor)
    return common::make_unexpected(
        unsupported("grouped shuffle cancellation stream version is unsupported"));
  common::Uuid::Bytes query_bytes{};
  std::ranges::copy(*query, query_bytes.begin());
  if (*header_length != v3::kHeaderLength || *frame_length != v3::kFrameLength ||
      *action != static_cast<std::uint8_t>(
                     DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel) ||
      !zero(*action_reserved) || !zero(*reserved) || *coordinator == 0U || *target == 0U ||
      *coordinator == *target || common::Uuid{query_bytes}.is_nil()) {
    return common::make_unexpected(
        corruption("grouped shuffle cancellation stream header is invalid"));
  }
  if (v3::kFrameLength > limits.maximum_frame_length)
    return common::make_unexpected(exhausted("grouped shuffle cancellation stream exceeds limits"));
  return v3::kFrameLength;
}

DistributedVectorGroupedAggregateShuffleJobControlRequestReader::
    DistributedVectorGroupedAggregateShuffleJobControlRequestReader(
        DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits) noexcept
    : limits_(limits) {}

DistributedVectorGroupedAggregateShuffleJobControlRequestReader::
    DistributedVectorGroupedAggregateShuffleJobControlRequestReader(
        DistributedVectorGroupedAggregateShuffleJobControlRequestReader&& other) noexcept
    : limits_(other.limits_), header_(other.header_), header_bytes_(other.header_bytes_),
      frame_(std::move(other.frame_)), frame_bytes_(other.frame_bytes_),
      expected_frame_bytes_(other.expected_frame_bytes_), failure_(std::move(other.failure_)),
      version_(other.version_) {
  other.reset_frame();
  other.failure_.reset();
}

DistributedVectorGroupedAggregateShuffleJobControlRequestReader&
DistributedVectorGroupedAggregateShuffleJobControlRequestReader::operator=(
    DistributedVectorGroupedAggregateShuffleJobControlRequestReader&& other) noexcept {
  if (this != &other) {
    limits_ = other.limits_;
    header_ = other.header_;
    header_bytes_ = other.header_bytes_;
    frame_ = std::move(other.frame_);
    frame_bytes_ = other.frame_bytes_;
    expected_frame_bytes_ = other.expected_frame_bytes_;
    failure_ = std::move(other.failure_);
    version_ = other.version_;
    other.reset_frame();
    other.failure_.reset();
  }
  return *this;
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlRequestReader>
DistributedVectorGroupedAggregateShuffleJobControlRequestReader::create(
    const DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits) {
  const common::Status valid =
      validate_distributed_vector_grouped_aggregate_shuffle_job_control_decode_limits(limits);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  return DistributedVectorGroupedAggregateShuffleJobControlRequestReader{limits};
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlRequestReadStep>
DistributedVectorGroupedAggregateShuffleJobControlRequestReader::fail(common::Status status) {
  failure_.emplace(std::move(status));
  return common::make_unexpected(*failure_);
}

void DistributedVectorGroupedAggregateShuffleJobControlRequestReader::reset_frame() noexcept {
  header_bytes_ = 0U;
  frame_.clear();
  frame_bytes_ = 0U;
  expected_frame_bytes_.reset();
  version_ = 1U;
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlRequestReadStep>
DistributedVectorGroupedAggregateShuffleJobControlRequestReader::consume(
    const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  std::size_t consumed{};
  if (!expected_frame_bytes_.has_value()) {
    const std::size_t copied = std::min(bytes.size(), header_.size() - header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != header_.size())
      return DistributedVectorGroupedAggregateShuffleJobControlRequestReadStep{.consumed_bytes =
                                                                                   consumed};
    const auto magic = common::ByteView{header_}.first(
        distributed_vector_grouped_aggregate_shuffle_job_control_format::kRequestMagic.size());
    version_ =
        std::ranges::equal(
            magic,
            distributed_vector_grouped_aggregate_shuffle_job_control_v2_format::kRequestMagic)
            ? 2U
        : std::ranges::equal(
              magic,
              distributed_vector_grouped_aggregate_shuffle_job_control_v3_format::kRequestMagic)
            ? 3U
            : 1U;
    auto expected =
        version_ == 2U
            ? distributed_vector_grouped_aggregate_shuffle_job_control_request_frame_length_v2(
                  header_, limits_)
        : version_ == 3U
            ? distributed_vector_grouped_aggregate_shuffle_job_control_request_frame_length_v3(
                  header_, limits_)
            : distributed_vector_grouped_aggregate_shuffle_job_control_request_frame_length_v1(
                  header_, limits_);
    if (!expected.has_value())
      return fail(expected.error());
    try {
      frame_.resize(*expected);
    } catch (const std::bad_alloc&) {
      return fail(exhausted("grouped shuffle reducer-job request reader allocation failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("grouped shuffle reducer-job request frame exceeds containers"));
    }
    std::ranges::copy(header_, frame_.begin());
    frame_bytes_ = header_.size();
    expected_frame_bytes_ = *expected;
  }
  const common::ByteView remainder = bytes.subspan(consumed);
  const std::size_t copied = std::min(remainder.size(), *expected_frame_bytes_ - frame_bytes_);
  std::ranges::copy(remainder.first(copied),
                    frame_.begin() + static_cast<std::ptrdiff_t>(frame_bytes_));
  frame_bytes_ += copied;
  consumed += copied;
  if (frame_bytes_ != *expected_frame_bytes_)
    return DistributedVectorGroupedAggregateShuffleJobControlRequestReadStep{.consumed_bytes =
                                                                                 consumed};
  auto decoded =
      version_ == 2U
          ? decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v2_exact(
                frame_, limits_)
      : version_ == 3U
          ? decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v3_exact(
                frame_, limits_)
          : decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(
                frame_, limits_);
  if (!decoded.has_value())
    return fail(decoded.error());
  auto request = std::move(*decoded);
  reset_frame();
  return DistributedVectorGroupedAggregateShuffleJobControlRequestReadStep{
      .consumed_bytes = consumed, .request = std::move(request)};
}

std::size_t
DistributedVectorGroupedAggregateShuffleJobControlRequestReader::buffered_bytes() const noexcept {
  return expected_frame_bytes_.has_value() ? frame_bytes_ : header_bytes_;
}

std::optional<std::size_t>
DistributedVectorGroupedAggregateShuffleJobControlRequestReader::expected_frame_bytes()
    const noexcept {
  return expected_frame_bytes_;
}

bool DistributedVectorGroupedAggregateShuffleJobControlRequestReader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorGroupedAggregateShuffleJobControlResponseReader::
    DistributedVectorGroupedAggregateShuffleJobControlResponseReader(
        DistributedVectorGroupedAggregateShuffleJobControlResponseReader&& other) noexcept
    : frame_(other.frame_), buffered_bytes_(other.buffered_bytes_),
      failure_(std::move(other.failure_)) {
  other.buffered_bytes_ = 0U;
  other.failure_.reset();
}

DistributedVectorGroupedAggregateShuffleJobControlResponseReader&
DistributedVectorGroupedAggregateShuffleJobControlResponseReader::operator=(
    DistributedVectorGroupedAggregateShuffleJobControlResponseReader&& other) noexcept {
  if (this != &other) {
    frame_ = other.frame_;
    buffered_bytes_ = other.buffered_bytes_;
    failure_ = std::move(other.failure_);
    other.buffered_bytes_ = 0U;
    other.failure_.reset();
  }
  return *this;
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponseReadStep>
DistributedVectorGroupedAggregateShuffleJobControlResponseReader::consume(
    const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  const std::size_t copied = std::min(bytes.size(), frame_.size() - buffered_bytes_);
  std::ranges::copy(bytes.first(copied),
                    frame_.begin() + static_cast<std::ptrdiff_t>(buffered_bytes_));
  buffered_bytes_ += copied;
  if (buffered_bytes_ != frame_.size())
    return DistributedVectorGroupedAggregateShuffleJobControlResponseReadStep{.consumed_bytes =
                                                                                  copied};
  const auto magic = common::ByteView{frame_}.first(
      distributed_vector_grouped_aggregate_shuffle_job_control_format::kResponseMagic.size());
  auto decoded =
      std::ranges::equal(
          magic, distributed_vector_grouped_aggregate_shuffle_job_control_v2_format::kResponseMagic)
          ? decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v2_exact(
                frame_)
      : std::ranges::equal(
            magic,
            distributed_vector_grouped_aggregate_shuffle_job_control_v3_format::kResponseMagic)
          ? decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v3_exact(
                frame_)
          : decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1_exact(
                frame_);
  if (!decoded.has_value()) {
    failure_.emplace(decoded.error());
    return common::make_unexpected(*failure_);
  }
  buffered_bytes_ = 0U;
  return DistributedVectorGroupedAggregateShuffleJobControlResponseReadStep{
      .consumed_bytes = copied, .response = *decoded};
}

std::size_t
DistributedVectorGroupedAggregateShuffleJobControlResponseReader::buffered_bytes() const noexcept {
  return buffered_bytes_;
}

bool DistributedVectorGroupedAggregateShuffleJobControlResponseReader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor::
    DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor(
        EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest frame) noexcept
    : frame_(std::move(frame)) {}

DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor::
    DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor(
        DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor&& other) noexcept
    : frame_(std::move(other.frame_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.frame_.bytes().size();
}

DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor&
DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor::operator=(
    DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor&& other) noexcept {
  if (this != &other) {
    frame_ = std::move(other.frame_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.frame_.bytes().size();
  }
  return *this;
}

DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor
DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor::create(
    EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest frame) noexcept {
  return DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor{std::move(frame)};
}

common::ByteView
DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor::pending_write()
    const noexcept {
  return frame_.bytes().subspan(written_bytes_);
}

common::Status
DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor::consume_written(
    const std::size_t bytes) noexcept {
  if (bytes > frame_.bytes().size() - written_bytes_)
    return invalid("grouped shuffle reducer-job request write exceeds pending bytes");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor::written_bytes()
    const noexcept {
  return written_bytes_;
}

bool DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor::complete()
    const noexcept {
  return written_bytes_ == frame_.bytes().size();
}

DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor::
    DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor(
        EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse frame) noexcept
    : frame_(std::move(frame)) {}

DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor::
    DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor(
        DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor&& other) noexcept
    : frame_(std::move(other.frame_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.frame_.bytes().size();
}

DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor&
DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor::operator=(
    DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor&& other) noexcept {
  if (this != &other) {
    frame_ = std::move(other.frame_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.frame_.bytes().size();
  }
  return *this;
}

DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor
DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor::create(
    EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse frame) noexcept {
  return DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor{std::move(frame)};
}

common::ByteView
DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor::pending_write()
    const noexcept {
  return frame_.bytes().subspan(written_bytes_);
}

common::Status
DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor::consume_written(
    const std::size_t bytes) noexcept {
  if (bytes > frame_.bytes().size() - written_bytes_)
    return invalid("grouped shuffle reducer-job response write exceeds pending bytes");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor::written_bytes()
    const noexcept {
  return written_bytes_;
}

bool DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor::complete()
    const noexcept {
  return written_bytes_ == frame_.bytes().size();
}

} // namespace chronos::cluster
