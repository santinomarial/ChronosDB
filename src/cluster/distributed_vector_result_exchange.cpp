#include "chronos/cluster/distributed_vector_result_exchange.hpp"

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

namespace chronos::cluster {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'X'}, std::byte{'V'}, std::byte{'E'},
                                                  std::byte{'C'}, std::byte{'2'}};
inline constexpr std::uint32_t kTerminalFlag = 1U;
inline constexpr std::size_t kHeaderCrcOffset = 72U;
inline constexpr std::size_t kReservedOffset = 76U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status
validate_limits(const DistributedVectorResultExchangeDecodeLimits& limits) {
  if (limits.maximum_frame_length <
          distributed_vector_result_exchange_v2_format::kHeaderLength +
              distributed_vector_result_exchange_v2_format::kTrailerLength ||
      limits.maximum_frame_length >
          distributed_vector_result_exchange_v2_format::kMaximumFrameLength ||
      limits.result_batch.protocol.maximum_payload_size == 0U ||
      limits.result_batch.protocol.maximum_payload_size > network::kDefaultMaximumPayloadSize ||
      limits.result_batch.maximum_rows == 0U || limits.result_batch.maximum_columns == 0U ||
      limits.result_batch.maximum_columns >
          query::distributed_vector_result_schema_format::kMaximumColumns ||
      limits.result_batch.maximum_column_name_bytes == 0U ||
      limits.result_batch.maximum_column_name_bytes > 65'536U)
    return invalid("distributed vector result exchange limits are invalid");
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_expected_schema(const query::DistributedVectorResultSchema& expected_schema,
                         const DistributedVectorResultExchangeDecodeLimits* limits = nullptr) {
  common::Status status = query::validate_distributed_vector_result_schema_value(expected_schema);
  if (!status.is_ok())
    return status;
  if (limits != nullptr) {
    if (expected_schema.columns.size() > limits->result_batch.maximum_columns)
      return exhausted("distributed vector result schema exceeds the caller column limit");
    for (const query::DistributedVectorResultColumn& column : expected_schema.columns) {
      if (column.name.size() > limits->result_batch.maximum_column_name_bytes)
        return exhausted("distributed vector result schema exceeds the caller name limit");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] bool descriptors_match(const network::QueryResultBatchView& batch,
                                     const query::DistributedVectorResultSchema& expected_schema) {
  const std::span<const network::QueryResultColumn> columns = batch.columns();
  if (columns.size() != expected_schema.columns.size())
    return false;
  for (std::size_t index = 0U; index < columns.size(); ++index) {
    const query::DistributedVectorResultColumn& expected = expected_schema.columns[index];
    if (columns[index].name != expected.name || columns[index].type != expected.type ||
        columns[index].nullable != expected.nullable)
      return false;
  }
  return true;
}

[[nodiscard]] common::Status
validate_message(const DistributedVectorResultExchangeMessage& message,
                 const query::DistributedVectorResultSchema& expected_schema) {
  common::Status schema_status = validate_expected_schema(expected_schema);
  if (!schema_status.is_ok())
    return schema_status;
  if (message.query_id.is_nil() || message.tablet_id.uuid().is_nil() || message.sequence == 0U)
    return invalid("distributed vector result exchange identity or sequence is invalid");
  if (message.encoded_result_batch.empty()) {
    return message.terminal
               ? common::Status::ok()
               : invalid("distributed vector result exchange nonterminal frame is empty");
  }
  const auto batch = network::decode_query_result_batch(message.encoded_result_batch);
  if (!batch.has_value()) {
    return batch.error().code() == common::StatusCode::kResourceExhausted
               ? batch.error()
               : invalid("distributed vector result exchange batch is not canonical");
  }
  return descriptors_match(*batch, expected_schema)
             ? common::Status::ok()
             : invalid("distributed vector result exchange schema differs from the fragment");
}

[[nodiscard]] bool same_magic(const common::ByteView bytes) {
  return bytes.size() >= kMagic.size() && std::ranges::equal(bytes.first(kMagic.size()), kMagic);
}

} // namespace

EncodedDistributedVectorResultExchangeMessage::EncodedDistributedVectorResultExchangeMessage(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedVectorResultExchangeMessage::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedVectorResultExchangeMessage>
encode_distributed_vector_result_exchange_message_v2(
    const DistributedVectorResultExchangeMessage& message,
    const query::DistributedVectorResultSchema& expected_schema) {
  const common::Status validation = validate_message(message, expected_schema);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  if (message.encoded_result_batch.size() > network::kDefaultMaximumPayloadSize ||
      message.encoded_result_batch.size() > std::numeric_limits<std::uint32_t>::max())
    return common::make_unexpected(
        invalid("distributed vector result exchange batch is too large"));
  const std::size_t frame_length = distributed_vector_result_exchange_v2_format::kHeaderLength +
                                   message.encoded_result_batch.size() +
                                   distributed_vector_result_exchange_v2_format::kTrailerLength;
  if (frame_length > distributed_vector_result_exchange_v2_format::kMaximumFrameLength)
    return common::make_unexpected(
        invalid("distributed vector result exchange frame is too large"));
  try {
    std::vector<std::byte> bytes(frame_length);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_result_exchange_v2_format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_result_exchange_v2_format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(distributed_vector_result_exchange_v2_format::kHeaderLength);
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
      status = writer.write_u32_le(static_cast<std::uint32_t>(message.encoded_result_batch.size()));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.zero_fill(4U);
    if (status.is_ok())
      status = writer.write_exact(message.encoded_result_batch);
    if (status.is_ok())
      status =
          writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "distributed vector result exchange layout failed"});
    return EncodedDistributedVectorResultExchangeMessage{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("distributed vector result exchange allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed vector result exchange exceeds container limits"));
  }
}

common::Result<DistributedVectorResultExchangeMessage>
decode_distributed_vector_result_exchange_message_v2_exact(
    const common::ByteView bytes, const query::DistributedVectorResultSchema& expected_schema,
    const DistributedVectorResultExchangeDecodeLimits limits) {
  const common::Status limit_status = validate_limits(limits);
  if (!limit_status.is_ok())
    return common::make_unexpected(limit_status);
  const common::Status schema_status = validate_expected_schema(expected_schema, &limits);
  if (!schema_status.is_ok())
    return common::make_unexpected(schema_status);
  constexpr std::size_t kMinimum = distributed_vector_result_exchange_v2_format::kHeaderLength +
                                   distributed_vector_result_exchange_v2_format::kTrailerLength;
  if (bytes.size() < kMinimum ||
      bytes.size() > distributed_vector_result_exchange_v2_format::kMaximumFrameLength)
    return common::make_unexpected(
        corruption("distributed vector result exchange length is invalid"));
  if (bytes.size() > limits.maximum_frame_length)
    return common::make_unexpected(
        exhausted("distributed vector result exchange exceeds the caller frame limit"));
  if (!same_magic(bytes))
    return common::make_unexpected(
        corruption("distributed vector result exchange magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto header_crc = header_crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(bytes.first(kHeaderCrcOffset)))
    return common::make_unexpected(
        corruption("distributed vector result exchange header checksum is invalid"));

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
    return common::make_unexpected(
        corruption("distributed vector result exchange header is truncated"));
  if (*major != distributed_vector_result_exchange_v2_format::kMajor ||
      *minor != distributed_vector_result_exchange_v2_format::kMinor)
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported,
                       "distributed vector result exchange version is unsupported"});
  if (*header_length != distributed_vector_result_exchange_v2_format::kHeaderLength ||
      (*flags & ~kTerminalFlag) != 0U || *frame_length != bytes.size() ||
      *batch_length != bytes.size() - distributed_vector_result_exchange_v2_format::kHeaderLength -
                           distributed_vector_result_exchange_v2_format::kTrailerLength ||
      !std::ranges::all_of(bytes.subspan(kReservedOffset, 4U),
                           [](const std::byte value) { return value == std::byte{}; }))
    return common::make_unexpected(
        corruption("distributed vector result exchange header is noncanonical"));
  if (*batch_length > limits.result_batch.protocol.maximum_payload_size)
    return common::make_unexpected(
        exhausted("distributed vector result exchange batch exceeds the caller limit"));
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(
        corruption("distributed vector result exchange checksum is invalid"));

  common::Uuid::Bytes query_bytes{};
  common::Uuid::Bytes tablet_bytes{};
  std::ranges::copy(*query_id, query_bytes.begin());
  std::ranges::copy(*tablet_id, tablet_bytes.begin());
  const common::Uuid query{query_bytes};
  const auto tablet = schema::TabletId::from_bytes(tablet_bytes);
  if (query.is_nil() || !tablet.has_value() || *sequence == 0U)
    return common::make_unexpected(
        corruption("distributed vector result exchange identity is invalid"));
  const bool terminal = (*flags & kTerminalFlag) != 0U;
  const common::ByteView batch_bytes =
      bytes.subspan(distributed_vector_result_exchange_v2_format::kHeaderLength, *batch_length);
  if (batch_bytes.empty() && !terminal)
    return common::make_unexpected(
        corruption("distributed vector result exchange nonterminal frame is empty"));
  if (!batch_bytes.empty()) {
    const auto batch = network::decode_query_result_batch(batch_bytes, limits.result_batch);
    if (!batch.has_value())
      return common::make_unexpected(batch.error());
    if (!descriptors_match(*batch, expected_schema))
      return common::make_unexpected(
          corruption("distributed vector result exchange schema differs from the fragment"));
  }
  try {
    return DistributedVectorResultExchangeMessage{
        .query_id = query,
        .tablet_id = *tablet,
        .sequence = *sequence,
        .terminal = terminal,
        .encoded_result_batch = {batch_bytes.begin(), batch_bytes.end()}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("distributed vector result exchange decode allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed vector result exchange decode exceeds container limits"));
  }
}

DistributedVectorResultExchangeReader::DistributedVectorResultExchangeReader(
    query::DistributedVectorResultSchema&& expected_schema,
    const DistributedVectorResultExchangeDecodeLimits limits) noexcept
    : expected_schema_(std::move(expected_schema)), limits_(limits) {}

common::Result<DistributedVectorResultExchangeReadStep>
DistributedVectorResultExchangeReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  const common::Status limit_status = validate_limits(limits_);
  if (!limit_status.is_ok())
    return common::make_unexpected(limit_status);
  const common::Status schema_status = validate_expected_schema(expected_schema_, &limits_);
  if (!schema_status.is_ok())
    return common::make_unexpected(schema_status);
  std::size_t consumed{};
  if (frame_.empty()) {
    const std::size_t copied = std::min(
        bytes.size(), distributed_vector_result_exchange_v2_format::kHeaderLength - header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != distributed_vector_result_exchange_v2_format::kHeaderLength)
      return DistributedVectorResultExchangeReadStep{.consumed_bytes = consumed,
                                                     .message = std::nullopt};

    common::ByteReader crc_reader{common::ByteView{header_}.subspan(kHeaderCrcOffset, 4U)};
    const auto header_crc = crc_reader.read_u32_le();
    common::ByteReader reader{header_};
    const auto magic = reader.read_exact(kMagic.size());
    const auto major = reader.read_u16_le();
    const auto minor = reader.read_u16_le();
    const auto header_length = reader.read_u32_le();
    const auto frame_length = reader.read_u64_le();
    static_cast<void>(reader.skip(40U));
    const auto flags = reader.read_u32_le();
    const auto batch_length = reader.read_u32_le();
    static_cast<void>(reader.skip(4U));
    const auto reserved = reader.read_u32_le();
    constexpr std::size_t kMinimum = distributed_vector_result_exchange_v2_format::kHeaderLength +
                                     distributed_vector_result_exchange_v2_format::kTrailerLength;
    if (!magic.has_value() || !std::ranges::equal(*magic, kMagic) || !header_crc.has_value() ||
        *header_crc != common::crc32c(common::ByteView{header_}.first(kHeaderCrcOffset)) ||
        !major.has_value() || !minor.has_value() || !header_length.has_value() ||
        *header_length != distributed_vector_result_exchange_v2_format::kHeaderLength ||
        !frame_length.has_value() || *frame_length < kMinimum ||
        *frame_length > distributed_vector_result_exchange_v2_format::kMaximumFrameLength ||
        !flags.has_value() || (*flags & ~kTerminalFlag) != 0U || !batch_length.has_value() ||
        *batch_length != *frame_length -
                             distributed_vector_result_exchange_v2_format::kHeaderLength -
                             distributed_vector_result_exchange_v2_format::kTrailerLength ||
        !reserved.has_value() || *reserved != 0U) {
      failure_ = corruption("distributed vector result exchange streaming header is invalid");
      return common::make_unexpected(*failure_);
    }
    if (*major != distributed_vector_result_exchange_v2_format::kMajor ||
        *minor != distributed_vector_result_exchange_v2_format::kMinor) {
      failure_ = common::Status{common::StatusCode::kNotSupported,
                                "distributed vector result exchange version is unsupported"};
      return common::make_unexpected(*failure_);
    }
    if (*frame_length > limits_.maximum_frame_length ||
        *batch_length > limits_.result_batch.protocol.maximum_payload_size) {
      failure_ = exhausted("distributed vector result exchange exceeds configured limits");
      return common::make_unexpected(*failure_);
    }
    try {
      frame_.resize(static_cast<std::size_t>(*frame_length));
    } catch (const std::bad_alloc&) {
      failure_ = exhausted("distributed vector result exchange reader allocation failed");
      return common::make_unexpected(*failure_);
    } catch (const std::length_error&) {
      failure_ = exhausted("distributed vector result exchange reader exceeds container limits");
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
    return DistributedVectorResultExchangeReadStep{.consumed_bytes = consumed,
                                                   .message = std::nullopt};
  auto decoded =
      decode_distributed_vector_result_exchange_message_v2_exact(frame_, expected_schema_, limits_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  DistributedVectorResultExchangeMessage result = std::move(*decoded);
  frame_.clear();
  frame_bytes_ = 0U;
  header_bytes_ = 0U;
  return DistributedVectorResultExchangeReadStep{.consumed_bytes = consumed,
                                                 .message = std::move(result)};
}

std::size_t DistributedVectorResultExchangeReader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool DistributedVectorResultExchangeReader::failed() const noexcept {
  return failure_.has_value();
}

DistributedVectorResultExchangeWriteCursor::DistributedVectorResultExchangeWriteCursor(
    EncodedDistributedVectorResultExchangeMessage encoded) noexcept
    : encoded_(std::move(encoded)) {}

DistributedVectorResultExchangeWriteCursor::DistributedVectorResultExchangeWriteCursor(
    DistributedVectorResultExchangeWriteCursor&& other) noexcept
    : encoded_(std::move(other.encoded_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_.bytes().size();
}

DistributedVectorResultExchangeWriteCursor& DistributedVectorResultExchangeWriteCursor::operator=(
    DistributedVectorResultExchangeWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_ = std::move(other.encoded_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_.bytes().size();
  }
  return *this;
}

common::Result<DistributedVectorResultExchangeWriteCursor>
DistributedVectorResultExchangeWriteCursor::create(
    const DistributedVectorResultExchangeMessage& message,
    const query::DistributedVectorResultSchema& expected_schema) {
  auto encoded = encode_distributed_vector_result_exchange_message_v2(message, expected_schema);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedVectorResultExchangeWriteCursor{std::move(*encoded)};
}

common::ByteView DistributedVectorResultExchangeWriteCursor::pending_write() const noexcept {
  return encoded_.bytes().subspan(written_bytes_);
}

common::Status
DistributedVectorResultExchangeWriteCursor::consume_written(const std::size_t bytes) noexcept {
  if (bytes > encoded_.bytes().size() - written_bytes_)
    return invalid("written byte count exceeds distributed vector result exchange frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t DistributedVectorResultExchangeWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedVectorResultExchangeWriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_.bytes().size();
}

namespace {

[[nodiscard]] bool same_message(const DistributedVectorResultExchangeMessage& left,
                                const DistributedVectorResultExchangeMessage& right) noexcept {
  return left.query_id == right.query_id && left.tablet_id == right.tablet_id &&
         left.sequence == right.sequence && left.terminal == right.terminal &&
         left.encoded_result_batch == right.encoded_result_batch;
}

} // namespace

class DistributedVectorResultCoordinatorV2::Impl {
public:
  struct FragmentProgress {
    std::vector<DistributedVectorResultExchangeMessage> messages;
    bool terminal{};
  };

  Impl(common::Uuid id, std::vector<schema::TabletId> tablets,
       query::DistributedVectorResultSchema schema,
       const DistributedVectorResultCoordinatorLimitsV2 configured)
      : query_id(id), tablet_order(std::move(tablets)), result_schema(std::move(schema)),
        limits(configured) {
    for (const schema::TabletId& tablet_id : tablet_order)
      fragments.emplace(tablet_id, FragmentProgress{});
  }

  common::Uuid query_id;
  std::vector<schema::TabletId> tablet_order;
  query::DistributedVectorResultSchema result_schema;
  DistributedVectorResultCoordinatorLimitsV2 limits;
  std::map<schema::TabletId, FragmentProgress> fragments;
  std::size_t retained_messages{};
  std::size_t retained_encoded_bytes{};
  std::optional<common::Status> failure;
  bool finished{};
};

DistributedVectorResultCoordinatorV2::DistributedVectorResultCoordinatorV2(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorResultCoordinatorV2::~DistributedVectorResultCoordinatorV2() = default;
DistributedVectorResultCoordinatorV2::DistributedVectorResultCoordinatorV2(
    DistributedVectorResultCoordinatorV2&&) noexcept = default;
DistributedVectorResultCoordinatorV2& DistributedVectorResultCoordinatorV2::operator=(
    DistributedVectorResultCoordinatorV2&&) noexcept = default;

common::Result<DistributedVectorResultCoordinatorV2> DistributedVectorResultCoordinatorV2::create(
    common::Uuid query_id, std::vector<schema::TabletId> tablets,
    query::DistributedVectorResultSchema result_schema,
    const DistributedVectorResultCoordinatorLimitsV2 limits) {
  constexpr std::size_t kMinimumMessageBytes =
      distributed_vector_result_exchange_v2_format::kHeaderLength +
      distributed_vector_result_exchange_v2_format::kTrailerLength;
  const common::Status schema_status =
      query::validate_distributed_vector_result_schema_value(result_schema);
  if (!schema_status.is_ok())
    return common::make_unexpected(schema_status);
  if (query_id.is_nil() || tablets.empty())
    return common::make_unexpected(invalid("vector result v2 coordinator identity is invalid"));
  if (limits.messages.maximum_messages_per_fragment == 0U ||
      limits.messages.maximum_messages_per_fragment > limits.messages.maximum_total_messages ||
      limits.messages.maximum_total_messages > query::kMaximumDistributedCoordinatorMessages ||
      limits.messages.maximum_total_messages < tablets.size() ||
      limits.maximum_total_encoded_bytes > kMaximumDistributedVectorResultCoordinatorBytesV2 ||
      tablets.size() > limits.maximum_total_encoded_bytes / kMinimumMessageBytes) {
    return common::make_unexpected(invalid("vector result v2 coordinator limits are invalid"));
  }
  try {
    std::set<schema::TabletId> unique_tablets;
    for (const schema::TabletId& tablet_id : tablets) {
      if (tablet_id.uuid().is_nil() || !unique_tablets.insert(tablet_id).second) {
        return common::make_unexpected(
            invalid("vector result v2 coordinator tablet set is invalid"));
      }
    }
    return DistributedVectorResultCoordinatorV2{
        std::make_unique<Impl>(query_id, std::move(tablets), std::move(result_schema), limits)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector result v2 coordinator allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("vector result v2 coordinator exceeds limits"));
  }
}

common::Status DistributedVectorResultCoordinatorV2::accept(
    const DistributedVectorResultExchangeMessage& message) {
  Impl& impl = *implementation_;
  if (impl.finished)
    return invalid("vector result v2 coordinator is already finished");
  if (impl.failure.has_value())
    return *impl.failure;
  auto fragment = impl.fragments.find(message.tablet_id);
  if (message.query_id != impl.query_id || fragment == impl.fragments.end())
    return invalid("vector result v2 message does not belong to the coordinator");
  auto encoded = encode_distributed_vector_result_exchange_message_v2(message, impl.result_schema);
  if (!encoded.has_value())
    return encoded.error();
  if (message.encoded_result_batch.empty() && message.sequence != 1U)
    return invalid("vector result v2 empty terminal is not sequence one");

  Impl::FragmentProgress& progress = fragment->second;
  if (message.sequence <= progress.messages.size()) {
    return same_message(message, progress.messages[message.sequence - 1U])
               ? common::Status::ok()
               : common::Status{common::StatusCode::kAlreadyExists,
                                "vector result v2 sequence conflicts with retained state"};
  }
  if (progress.terminal)
    return invalid("vector result v2 fragment emitted after its terminal message");
  if (message.sequence != progress.messages.size() + 1U) {
    return {common::StatusCode::kUnavailable, "vector result v2 fragment sequence has a gap"};
  }
  if (progress.messages.size() == impl.limits.messages.maximum_messages_per_fragment ||
      impl.retained_messages == impl.limits.messages.maximum_total_messages ||
      encoded->bytes().size() >
          impl.limits.maximum_total_encoded_bytes - impl.retained_encoded_bytes) {
    return exhausted("vector result v2 coordinator retention is exhausted");
  }
  try {
    progress.messages.push_back(message);
  } catch (const std::bad_alloc&) {
    return exhausted("vector result v2 coordinator retention allocation failed");
  } catch (const std::length_error&) {
    return exhausted("vector result v2 coordinator retention exceeds limits");
  }
  progress.terminal = message.terminal;
  ++impl.retained_messages;
  impl.retained_encoded_bytes += encoded->bytes().size();
  return common::Status::ok();
}

common::Status
DistributedVectorResultCoordinatorV2::worker_failed(const schema::TabletId& tablet_id,
                                                    common::Status failure) {
  Impl& impl = *implementation_;
  if (impl.finished)
    return invalid("vector result v2 coordinator is already finished");
  const auto fragment = impl.fragments.find(tablet_id);
  if (fragment == impl.fragments.end() || failure.is_ok())
    return invalid("vector result v2 worker failure is invalid or unplanned");
  if (fragment->second.terminal)
    return common::Status::ok();
  if (impl.failure.has_value())
    return *impl.failure;
  impl.failure = std::move(failure);
  return common::Status::ok();
}

common::Result<DistributedVectorQueryResultV2> DistributedVectorResultCoordinatorV2::finish() && {
  Impl& impl = *implementation_;
  if (impl.finished)
    return common::make_unexpected(invalid("vector result v2 coordinator is already finished"));
  if (impl.failure.has_value())
    return common::make_unexpected(*impl.failure);
  for (const schema::TabletId& tablet_id : impl.tablet_order) {
    if (!impl.fragments.at(tablet_id).terminal) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kUnavailable, "vector result v2 query has incomplete fragments"});
    }
  }
  std::vector<DistributedVectorResultExchangeMessage> messages;
  try {
    messages.reserve(impl.retained_messages);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector result v2 allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("vector result v2 exceeds limits"));
  }
  for (const schema::TabletId& tablet_id : impl.tablet_order) {
    auto& retained = impl.fragments.at(tablet_id).messages;
    std::ranges::move(retained, std::back_inserter(messages));
  }
  DistributedVectorQueryResultV2 result{.result_schema = std::move(impl.result_schema),
                                        .messages = std::move(messages)};
  impl.finished = true;
  return result;
}

} // namespace chronos::cluster
