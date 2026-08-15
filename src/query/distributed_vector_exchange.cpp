#include "chronos/query/distributed_vector_exchange.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <ranges>
#include <set>
#include <stdexcept>
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

[[nodiscard]] common::Status retained_failure(const std::optional<common::Status>& failure) {
  return failure.value_or(common::Status{common::StatusCode::kInternal,
                                         "distributed vector coordinator failure is missing"});
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

[[nodiscard]] bool same_message(const DistributedVectorExchangeMessage& lhs,
                                const DistributedVectorExchangeMessage& rhs) {
  return lhs.query_id == rhs.query_id && lhs.tablet_id == rhs.tablet_id &&
         lhs.sequence == rhs.sequence && lhs.terminal == rhs.terminal &&
         std::ranges::equal(lhs.encoded_batch, rhs.encoded_batch);
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

DistributedVectorExchangeReader::DistributedVectorExchangeReader(
    const DistributedVectorExchangeDecodeLimits limits)
    : limits_(limits) {}

common::Result<DistributedVectorExchangeReadStep>
DistributedVectorExchangeReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  if (limits_.maximum_frame_length < distributed_vector_exchange_format::kHeaderLength +
                                         distributed_vector_exchange_format::kTrailerLength ||
      limits_.maximum_frame_length > distributed_vector_exchange_format::kMaximumFrameLength ||
      limits_.batch.max_batch_length == 0U) {
    return common::make_unexpected(invalid("distributed vector exchange limits are invalid"));
  }
  std::size_t consumed{};
  if (frame_.empty()) {
    const std::size_t copied =
        std::min(bytes.size(), distributed_vector_exchange_format::kHeaderLength - header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != distributed_vector_exchange_format::kHeaderLength)
      return DistributedVectorExchangeReadStep{.consumed_bytes = consumed};

    common::ByteReader crc_reader{common::ByteView{header_}.subspan(72U, 4U)};
    const auto header_crc = crc_reader.read_u32_le();
    common::ByteReader reader{header_};
    const auto magic = reader.read_exact(8U);
    const auto major = reader.read_u16_le();
    const auto minor = reader.read_u16_le();
    const auto header_length = reader.read_u32_le();
    const auto frame_length = reader.read_u64_le();
    static_cast<void>(reader.skip(40U));
    const auto flags = reader.read_u32_le();
    const auto batch_length = reader.read_u32_le();
    static_cast<void>(reader.skip(4U));
    const auto reserved = reader.read_u32_le();
    if (!magic.has_value() || !std::ranges::equal(*magic, kMagic) || !header_crc.has_value() ||
        *header_crc != common::crc32c(common::ByteView{header_}.first(72U)) || !major.has_value() ||
        !minor.has_value() || !header_length.has_value() ||
        *header_length != distributed_vector_exchange_format::kHeaderLength ||
        !frame_length.has_value() ||
        *frame_length < distributed_vector_exchange_format::kHeaderLength +
                            distributed_vector_exchange_format::kTrailerLength ||
        *frame_length > limits_.maximum_frame_length || !flags.has_value() ||
        (*flags & ~kTerminalFlag) != 0U || !batch_length.has_value() ||
        *batch_length != *frame_length - distributed_vector_exchange_format::kHeaderLength -
                             distributed_vector_exchange_format::kTrailerLength ||
        !reserved.has_value() || *reserved != 0U) {
      failure_ = corruption("distributed vector exchange streaming header is invalid");
      return common::make_unexpected(*failure_);
    }
    if (*major != distributed_vector_exchange_format::kMajor ||
        *minor != distributed_vector_exchange_format::kMinor) {
      failure_ = common::Status{common::StatusCode::kNotSupported,
                                "distributed vector exchange version is unsupported"};
      return common::make_unexpected(*failure_);
    }
    if (*batch_length > limits_.batch.max_batch_length) {
      failure_ = common::Status{common::StatusCode::kResourceExhausted,
                                "distributed vector exchange batch exceeds configured limit"};
      return common::make_unexpected(*failure_);
    }
    try {
      frame_.resize(static_cast<std::size_t>(*frame_length));
    } catch (const std::bad_alloc&) {
      failure_ = common::Status{common::StatusCode::kResourceExhausted,
                                "distributed vector exchange reader allocation failed"};
      return common::make_unexpected(*failure_);
    } catch (const std::length_error&) {
      failure_ = common::Status{common::StatusCode::kResourceExhausted,
                                "distributed vector exchange reader exceeds container limits"};
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
    return DistributedVectorExchangeReadStep{.consumed_bytes = consumed};
  auto decoded = decode_distributed_vector_exchange_message_exact(frame_, limits_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  DistributedVectorExchangeMessage result = std::move(*decoded);
  frame_.clear();
  frame_bytes_ = 0U;
  header_bytes_ = 0U;
  return DistributedVectorExchangeReadStep{.consumed_bytes = consumed,
                                           .message = std::move(result)};
}

std::size_t DistributedVectorExchangeReader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool DistributedVectorExchangeReader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorExchangeWriteCursor::DistributedVectorExchangeWriteCursor(
    EncodedDistributedVectorExchangeMessage encoded) noexcept
    : encoded_(std::move(encoded)) {}

DistributedVectorExchangeWriteCursor::DistributedVectorExchangeWriteCursor(
    DistributedVectorExchangeWriteCursor&& other) noexcept
    : encoded_(std::move(other.encoded_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_.bytes().size();
}

DistributedVectorExchangeWriteCursor& DistributedVectorExchangeWriteCursor::operator=(
    DistributedVectorExchangeWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_ = std::move(other.encoded_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_.bytes().size();
  }
  return *this;
}

common::Result<DistributedVectorExchangeWriteCursor>
DistributedVectorExchangeWriteCursor::create(const DistributedVectorExchangeMessage& message) {
  auto encoded = encode_distributed_vector_exchange_message(message);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedVectorExchangeWriteCursor{std::move(*encoded)};
}

common::ByteView DistributedVectorExchangeWriteCursor::pending_write() const noexcept {
  return encoded_.bytes().subspan(written_bytes_);
}

common::Status
DistributedVectorExchangeWriteCursor::consume_written(const std::size_t bytes) noexcept {
  if (bytes > encoded_.bytes().size() - written_bytes_)
    return invalid("written byte count exceeds distributed vector exchange frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t DistributedVectorExchangeWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedVectorExchangeWriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_.bytes().size();
}

class DistributedVectorCoordinator::Impl {
public:
  struct FragmentProgress {
    std::vector<DistributedVectorExchangeMessage> messages;
    bool terminal{};
  };

  Impl(common::Uuid id, std::vector<schema::TabletId> tablets,
       const DistributedVectorCoordinatorLimits configured)
      : query_id(id), tablet_order(std::move(tablets)), limits(configured) {
    for (const schema::TabletId& tablet_id : tablet_order)
      fragments.emplace(tablet_id, FragmentProgress{});
  }

  common::Uuid query_id;
  std::vector<schema::TabletId> tablet_order;
  DistributedVectorCoordinatorLimits limits;
  std::map<schema::TabletId, FragmentProgress> fragments;
  std::size_t retained_messages{};
  std::size_t retained_batch_bytes{};
  std::optional<common::Status> failure;
  bool finished{};
};

DistributedVectorCoordinator::DistributedVectorCoordinator(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

DistributedVectorCoordinator::~DistributedVectorCoordinator() = default;

DistributedVectorCoordinator::DistributedVectorCoordinator(
    DistributedVectorCoordinator&&) noexcept = default;

DistributedVectorCoordinator&
DistributedVectorCoordinator::operator=(DistributedVectorCoordinator&&) noexcept = default;

common::Result<DistributedVectorCoordinator>
DistributedVectorCoordinator::create(common::Uuid query_id, std::vector<schema::TabletId> tablets,
                                     const DistributedVectorCoordinatorLimits limits) {
  if (query_id.is_nil() || tablets.empty())
    return common::make_unexpected(invalid("distributed vector coordinator identity is invalid"));
  if (limits.messages.maximum_messages_per_fragment == 0U ||
      limits.messages.maximum_messages_per_fragment > limits.messages.maximum_total_messages ||
      limits.messages.maximum_total_messages > kMaximumDistributedCoordinatorMessages ||
      limits.messages.maximum_total_messages < tablets.size() ||
      limits.maximum_total_batch_bytes == 0U ||
      limits.maximum_total_batch_bytes > kMaximumDistributedVectorCoordinatorBytes) {
    return common::make_unexpected(invalid("distributed vector coordinator limits are invalid"));
  }
  try {
    std::set<schema::TabletId> unique_tablets;
    for (const schema::TabletId& tablet_id : tablets) {
      if (tablet_id.uuid().is_nil() || !unique_tablets.insert(tablet_id).second) {
        return common::make_unexpected(
            invalid("distributed vector coordinator tablet set is invalid"));
      }
    }
    return DistributedVectorCoordinator{
        std::make_unique<Impl>(query_id, std::move(tablets), limits)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector coordinator allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector coordinator exceeds container limits"});
  }
}

common::Status
DistributedVectorCoordinator::accept(const DistributedVectorExchangeMessage& message) {
  if (impl_->finished)
    return invalid("distributed vector coordinator is already finished");
  if (impl_->failure.has_value())
    return retained_failure(impl_->failure);
  common::Status validation = validate_message(message);
  if (!validation.is_ok())
    return validation;
  auto fragment = impl_->fragments.find(message.tablet_id);
  if (message.query_id != impl_->query_id || fragment == impl_->fragments.end())
    return invalid("distributed vector result does not belong to the coordinator");

  Impl::FragmentProgress& progress = fragment->second;
  if (message.sequence <= progress.messages.size()) {
    return same_message(message, progress.messages[message.sequence - 1U])
               ? common::Status::ok()
               : common::Status{common::StatusCode::kAlreadyExists,
                                "distributed vector sequence conflicts with retained state"};
  }
  if (progress.terminal)
    return invalid("distributed vector fragment emitted after its terminal message");
  if (message.sequence != progress.messages.size() + 1U) {
    return common::Status{common::StatusCode::kUnavailable,
                          "distributed vector fragment sequence has a gap"};
  }
  if (progress.messages.size() == impl_->limits.messages.maximum_messages_per_fragment ||
      impl_->retained_messages == impl_->limits.messages.maximum_total_messages ||
      message.encoded_batch.size() >
          impl_->limits.maximum_total_batch_bytes - impl_->retained_batch_bytes) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "distributed vector coordinator retention is exhausted"};
  }
  try {
    progress.messages.push_back(message);
  } catch (const std::bad_alloc&) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "distributed vector coordinator retention allocation failed"};
  } catch (const std::length_error&) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "distributed vector coordinator retention exceeds container limits"};
  }
  progress.terminal = message.terminal;
  ++impl_->retained_messages;
  impl_->retained_batch_bytes += message.encoded_batch.size();
  return common::Status::ok();
}

common::Status DistributedVectorCoordinator::worker_failed(const schema::TabletId& tablet_id,
                                                           common::Status failure) {
  if (impl_->finished)
    return invalid("distributed vector coordinator is already finished");
  const auto fragment = impl_->fragments.find(tablet_id);
  if (fragment == impl_->fragments.end() || failure.is_ok())
    return invalid("distributed vector worker failure is invalid or unplanned");
  if (fragment->second.terminal)
    return common::Status::ok();
  if (impl_->failure.has_value())
    return retained_failure(impl_->failure);
  impl_->failure = std::move(failure);
  return common::Status::ok();
}

common::Result<std::vector<DistributedVectorExchangeMessage>>
DistributedVectorCoordinator::finish() && {
  if (impl_->finished)
    return common::make_unexpected(invalid("distributed vector coordinator is already finished"));
  if (impl_->failure.has_value())
    return common::make_unexpected(retained_failure(impl_->failure));
  for (const schema::TabletId& tablet_id : impl_->tablet_order) {
    if (!impl_->fragments.at(tablet_id).terminal) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kUnavailable, "distributed vector query has incomplete fragments"});
    }
  }
  std::vector<DistributedVectorExchangeMessage> result;
  try {
    result.reserve(impl_->retained_messages);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed vector result allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector result exceeds container limits"});
  }
  for (const schema::TabletId& tablet_id : impl_->tablet_order) {
    auto& messages = impl_->fragments.at(tablet_id).messages;
    std::ranges::move(messages, std::back_inserter(result));
  }
  impl_->finished = true;
  return result;
}

} // namespace chronos::query
