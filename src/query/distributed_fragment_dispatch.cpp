#include "chronos/query/distributed_fragment_dispatch.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'F'}, std::byte{'D'}, std::byte{'S'},
                                                  std::byte{'P'}, std::byte{'1'}};
inline constexpr std::size_t kHeaderCrcOffset =
    distributed_fragment_dispatch_format::kHeaderLength - sizeof(std::uint32_t);

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

} // namespace

EncodedDistributedAggregateFragmentDispatch::EncodedDistributedAggregateFragmentDispatch(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedAggregateFragmentDispatch::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedAggregateFragmentDispatch>
encode_distributed_aggregate_fragment_dispatch(
    const DistributedAggregateFragmentDispatch& dispatch) {
  if (dispatch.raft_group_id.is_nil())
    return common::make_unexpected(invalid("distributed fragment dispatch group is invalid"));
  common::Result<EncodedDistributedAggregateFragment> inner =
      encode_distributed_aggregate_fragment(dispatch.fragment);
  if (!inner.has_value())
    return common::make_unexpected(inner.error());

  try {
    const std::size_t length = distributed_fragment_dispatch_format::kHeaderLength +
                               inner->bytes().size() +
                               distributed_fragment_dispatch_format::kTrailerLength;
    std::vector<std::byte> bytes(length);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_fragment_dispatch_format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_fragment_dispatch_format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(distributed_fragment_dispatch_format::kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(length);
    if (status.is_ok())
      status = writer.write_exact(dispatch.raft_group_id.bytes());
    if (status.is_ok())
      status = writer.write_u64_le(inner->bytes().size());
    if (status.is_ok())
      status = writer.zero_fill(28U);
    if (!status.is_ok() || writer.offset() != kHeaderCrcOffset) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "distributed fragment dispatch header is inconsistent"});
    }
    status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.write_exact(inner->bytes());
    if (status.is_ok())
      status =
          writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!status.is_ok() || !writer.full()) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "distributed fragment dispatch frame is inconsistent"});
    }
    return EncodedDistributedAggregateFragmentDispatch{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted, "distributed fragment dispatch allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed fragment dispatch exceeds limits"});
  }
}

common::Result<DistributedAggregateFragmentDispatch>
decode_distributed_aggregate_fragment_dispatch_exact(const common::ByteView bytes,
                                                     const DistributedFragmentDecodeLimits limits) {
  if (bytes.size() < distributed_fragment_dispatch_format::kHeaderLength +
                         distributed_fragment_format::kHeaderLength + sizeof(std::uint32_t) +
                         distributed_fragment_format::kTrailerLength +
                         distributed_fragment_dispatch_format::kTrailerLength ||
      bytes.size() > distributed_fragment_dispatch_format::kMaximumFrameLength) {
    return common::make_unexpected(corruption("distributed fragment dispatch length is invalid"));
  }
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("distributed fragment dispatch magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("distributed fragment dispatch header checksum is invalid"));
  }

  common::ByteReader reader{bytes};
  const common::Status skip_magic = reader.skip(kMagic.size());
  if (!skip_magic.is_ok())
    return common::make_unexpected(corruption("distributed fragment dispatch header is truncated"));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto group_bytes = reader.read_exact(common::Uuid::kSize);
  const auto inner_length = reader.read_u64_le();
  const auto reserved = reader.read_exact(28U);
  const auto header_crc = reader.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !group_bytes.has_value() || !inner_length.has_value() ||
      !reserved.has_value() || !header_crc.has_value()) {
    return common::make_unexpected(corruption("distributed fragment dispatch header is truncated"));
  }
  if (*major != distributed_fragment_dispatch_format::kMajor ||
      *minor != distributed_fragment_dispatch_format::kMinor) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotSupported, "distributed fragment dispatch version is unsupported"});
  }
  if (*header_length != distributed_fragment_dispatch_format::kHeaderLength ||
      std::ranges::any_of(*reserved, [](const std::byte value) { return value != std::byte{0}; })) {
    return common::make_unexpected(corruption("distributed fragment dispatch header is invalid"));
  }
  if (*inner_length < distributed_fragment_format::kHeaderLength + sizeof(std::uint32_t) +
                          distributed_fragment_format::kTrailerLength ||
      *inner_length > distributed_fragment_format::kMaximumFrameLength ||
      *frame_length != distributed_fragment_dispatch_format::kHeaderLength + *inner_length +
                           distributed_fragment_dispatch_format::kTrailerLength ||
      *frame_length != bytes.size()) {
    return common::make_unexpected(
        corruption("distributed fragment dispatch inner length is invalid"));
  }
  common::ByteReader trailer_reader{bytes.last(4U)};
  const auto stored_frame_crc = trailer_reader.read_u32_le();
  if (!stored_frame_crc.has_value() ||
      *stored_frame_crc != common::crc32c(bytes.first(bytes.size() - 4U))) {
    return common::make_unexpected(
        corruption("distributed fragment dispatch frame checksum is invalid"));
  }

  common::Uuid::Bytes group_id_bytes{};
  std::ranges::copy(*group_bytes, group_id_bytes.begin());
  common::Uuid group_id{group_id_bytes};
  if (group_id.is_nil())
    return common::make_unexpected(corruption("distributed fragment dispatch group is invalid"));
  common::Result<DistributedAggregateFragment> fragment =
      decode_distributed_aggregate_fragment_exact(
          bytes.subspan(distributed_fragment_dispatch_format::kHeaderLength, *inner_length),
          limits);
  if (!fragment.has_value())
    return common::make_unexpected(fragment.error());
  return DistributedAggregateFragmentDispatch{.raft_group_id = group_id,
                                              .fragment = std::move(*fragment)};
}

} // namespace chronos::query
