#include "chronos/query/distributed_vector_exchange.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <ranges>
#include <utility>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'X'}, std::byte{'V'}, std::byte{'E'},
                                                  std::byte{'C'}, std::byte{'1'}};
inline constexpr std::uint32_t kTerminalFlag = 1U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status validate_message(const DistributedVectorExchangeMessage& message) {
  if (message.query_id.is_nil() || message.tablet_id.uuid().is_nil() || message.sequence == 0U)
    return invalid("distributed vector exchange identity or sequence is invalid");
  if (message.encoded_batch.empty()) {
    return message.terminal ? common::Status::ok()
                            : invalid("distributed vector exchange nonterminal frame is empty");
  }
  const auto batch = columnar::decode_columnar_batch_v1_exact(message.encoded_batch);
  return batch.has_value() ? common::Status::ok() : batch.error().status();
}

} // namespace

EncodedDistributedVectorExchangeMessage::EncodedDistributedVectorExchangeMessage(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedVectorExchangeMessage::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedVectorExchangeMessage>
encode_distributed_vector_exchange_message(const DistributedVectorExchangeMessage& message) {
  const common::Status validation = validate_message(message);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  if (message.encoded_batch.size() > std::numeric_limits<std::uint32_t>::max())
    return common::make_unexpected(invalid("distributed vector exchange batch is too large"));
  const std::size_t frame_length = distributed_vector_exchange_format::kHeaderLength +
                                   message.encoded_batch.size() +
                                   distributed_vector_exchange_format::kTrailerLength;
  if (frame_length > distributed_vector_exchange_format::kMaximumFrameLength)
    return common::make_unexpected(invalid("distributed vector exchange frame is too large"));
  try {
    std::vector<std::byte> bytes(frame_length);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_exchange_format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_exchange_format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(distributed_vector_exchange_format::kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(frame_length);
    if (status.is_ok())
      status = writer.write_exact(message.query_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(message.tablet_id.bytes());
    if (status.is_ok())
      status = writer.write_u64_le(message.sequence);
    if (status.is_ok())
      status = writer.write_u32_le(message.terminal ? kTerminalFlag : 0U);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(message.encoded_batch.size()));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(72U)));
    if (status.is_ok())
      status = writer.zero_fill(4U);
    if (status.is_ok())
      status = writer.write_exact(message.encoded_batch);
    if (status.is_ok())
      status =
          writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                    "distributed vector exchange layout failed"});
    return EncodedDistributedVectorExchangeMessage{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed vector exchange allocation failed"});
  }
}

common::Result<DistributedVectorExchangeMessage> decode_distributed_vector_exchange_message_exact(
    const common::ByteView bytes, const DistributedVectorExchangeDecodeLimits limits) {
  if (limits.maximum_frame_length < distributed_vector_exchange_format::kHeaderLength +
                                        distributed_vector_exchange_format::kTrailerLength ||
      limits.maximum_frame_length > distributed_vector_exchange_format::kMaximumFrameLength ||
      limits.batch.max_batch_length == 0U) {
    return common::make_unexpected(invalid("distributed vector exchange limits are invalid"));
  }
  if (bytes.size() < distributed_vector_exchange_format::kHeaderLength +
                         distributed_vector_exchange_format::kTrailerLength)
    return common::make_unexpected(corruption("distributed vector exchange frame is truncated"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("distributed vector exchange magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(72U, 4U)};
  const auto header_crc = header_crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(bytes.first(72U)))
    return common::make_unexpected(
        corruption("distributed vector exchange header checksum is invalid"));

  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto query_id = reader.read_exact(16U);
  const auto tablet_id = reader.read_exact(16U);
  const auto sequence = reader.read_u64_le();
  const auto flags = reader.read_u32_le();
  const auto batch_length = reader.read_u32_le();
  static_cast<void>(reader.skip(8U));
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !query_id.has_value() || !tablet_id.has_value() ||
      !sequence.has_value() || !flags.has_value() || !batch_length.has_value())
    return common::make_unexpected(corruption("distributed vector exchange header is truncated"));
  if (*major != distributed_vector_exchange_format::kMajor ||
      *minor != distributed_vector_exchange_format::kMinor)
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotSupported, "distributed vector exchange version is unsupported"});
  if (*header_length != distributed_vector_exchange_format::kHeaderLength ||
      (*flags & ~kTerminalFlag) != 0U || *frame_length != bytes.size() ||
      *frame_length > limits.maximum_frame_length ||
      *batch_length != bytes.size() - distributed_vector_exchange_format::kHeaderLength -
                           distributed_vector_exchange_format::kTrailerLength ||
      !std::ranges::all_of(bytes.subspan(76U, 4U),
                           [](std::byte value) { return value == std::byte{}; }))
    return common::make_unexpected(
        corruption("distributed vector exchange header is noncanonical"));
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("distributed vector exchange checksum is invalid"));
  common::Uuid::Bytes query_bytes{};
  common::Uuid::Bytes tablet_bytes{};
  std::ranges::copy(*query_id, query_bytes.begin());
  std::ranges::copy(*tablet_id, tablet_bytes.begin());
  const common::Uuid query{query_bytes};
  auto tablet = schema::TabletId::from_bytes(tablet_bytes);
  if (query.is_nil() || !tablet.has_value() || *sequence == 0U)
    return common::make_unexpected(corruption("distributed vector exchange identity is invalid"));
  const bool terminal = (*flags & kTerminalFlag) != 0U;
  const common::ByteView batch_bytes =
      bytes.subspan(distributed_vector_exchange_format::kHeaderLength, *batch_length);
  if (batch_bytes.empty() && !terminal)
    return common::make_unexpected(
        corruption("distributed vector exchange nonterminal frame is empty"));
  if (!batch_bytes.empty()) {
    const auto batch = columnar::decode_columnar_batch_v1_exact(batch_bytes, limits.batch);
    if (!batch.has_value())
      return common::make_unexpected(batch.error().status());
  }
  try {
    return DistributedVectorExchangeMessage{
        .query_id = query,
        .tablet_id = *tablet,
        .sequence = *sequence,
        .terminal = terminal,
        .encoded_batch = {batch_bytes.begin(), batch_bytes.end()}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector exchange decode allocation failed"});
  }
}

} // namespace chronos::query
