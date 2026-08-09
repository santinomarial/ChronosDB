#include "chronos/live/multi_tablet_subscription_checkpoint.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/status.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'S'},
                                           std::byte{'U'}, std::byte{'B'}, std::byte{'C'},
                                           std::byte{'P'}, std::byte{'1'}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::array<std::byte, 8U> kBoundMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'S'},
                                                std::byte{'U'}, std::byte{'B'}, std::byte{'C'},
                                                std::byte{'G'}, std::byte{'1'}};

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status unsupported(std::string message) {
  return {common::StatusCode::kNotSupported, std::move(message)};
}

[[nodiscard]] bool valid_limits(const MultiTabletSubscriptionCheckpointCodecLimits& limits) {
  return limits.maximum_checkpoint_bytes >= kMultiTabletSubscriptionCheckpointHeaderSize +
                                                kMultiTabletSubscriptionCheckpointTrailerSize &&
         limits.maximum_checkpoint_bytes <= kMaximumMultiTabletSubscriptionCheckpointSize &&
         limits.maximum_sources != 0U && limits.maximum_sources <= kMaximumResumeTokenSources &&
         limits.maximum_retained_changes != 0U &&
         limits.maximum_retained_changes <= std::numeric_limits<std::uint32_t>::max() &&
         limits.maximum_result_key_bytes != 0U &&
         limits.maximum_result_key_bytes <= std::numeric_limits<std::uint32_t>::max() &&
         limits.maximum_payload_bytes != 0U &&
         limits.maximum_payload_bytes <= std::numeric_limits<std::uint32_t>::max();
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  return value;
}

[[nodiscard]] common::Result<std::size_t>
validate_and_size(const MultiTabletSubscriptionCheckpoint& checkpoint,
                  const MultiTabletSubscriptionCheckpointCodecLimits& limits) {
  if (checkpoint.database_id.is_nil() || checkpoint.table_id.uuid().is_nil() ||
      checkpoint.schema_id.uuid().is_nil() || checkpoint.schema_version.value() == 0U ||
      checkpoint.sources.empty() || checkpoint.sources.size() > limits.maximum_sources ||
      checkpoint.retained_changes.size() > limits.maximum_retained_changes)
    return common::make_unexpected(
        invalid("subscription checkpoint identity or counts are invalid"));
  try {
    std::map<schema::TabletId, std::size_t> indexes;
    std::vector<std::uint64_t> expected;
    expected.reserve(checkpoint.sources.size());
    for (std::size_t index = 0U; index < checkpoint.sources.size(); ++index) {
      const auto& source = checkpoint.sources[index];
      if (source.latest_position.tablet_id.uuid().is_nil() ||
          !source.latest_position.wal_id.is_valid() ||
          source.expired_through_sequence > source.latest_position.record_sequence ||
          (index != 0U && checkpoint.sources[index - 1U].latest_position.tablet_id >=
                              source.latest_position.tablet_id) ||
          !indexes.emplace(source.latest_position.tablet_id, index).second)
        return common::make_unexpected(invalid("subscription checkpoint source vector is invalid"));
      expected.push_back(source.expired_through_sequence);
    }

    auto total = common::checked_add(kMultiTabletSubscriptionCheckpointHeaderSize,
                                     kMultiTabletSubscriptionCheckpointTrailerSize);
    const auto source_bytes = common::checked_multiply(
        checkpoint.sources.size(), kMultiTabletSubscriptionCheckpointSourceSize);
    if (!total.has_value() || !source_bytes.has_value())
      return common::make_unexpected(exhausted("subscription checkpoint size overflows"));
    total = common::checked_add(*total, *source_bytes);
    for (const CommittedChange& change : checkpoint.retained_changes) {
      const auto found = indexes.find(change.position.tablet_id);
      if (found == indexes.end())
        return common::make_unexpected(invalid("subscription checkpoint change source is unknown"));
      const std::size_t index = found->second;
      if (change.position.wal_id != checkpoint.sources[index].latest_position.wal_id ||
          expected[index] == std::numeric_limits<std::uint64_t>::max() ||
          change.position.record_sequence != expected[index] + 1U ||
          change.position.record_sequence >
              checkpoint.sources[index].latest_position.record_sequence ||
          change.schema_id != checkpoint.schema_id ||
          change.schema_version != checkpoint.schema_version ||
          (change.operation != LogicalChangeOperation::kUpsert &&
           change.operation != LogicalChangeOperation::kDelete) ||
          change.result_key.empty() || change.result_key.size() > limits.maximum_result_key_bytes ||
          change.payload.size() > limits.maximum_payload_bytes ||
          (change.operation == LogicalChangeOperation::kDelete && !change.payload.empty()))
        return common::make_unexpected(
            invalid("subscription checkpoint retained change is noncanonical"));
      auto change_size = common::checked_add(kMultiTabletSubscriptionCheckpointChangeEnvelopeSize,
                                             change.result_key.size());
      change_size = change_size.has_value()
                        ? common::checked_add(*change_size, change.payload.size())
                        : std::nullopt;
      total = total.has_value() && change_size.has_value()
                  ? common::checked_add(*total, *change_size)
                  : std::nullopt;
      if (!total.has_value() || *total > limits.maximum_checkpoint_bytes)
        return common::make_unexpected(exhausted("subscription checkpoint exceeds size limit"));
      expected[index] = change.position.record_sequence;
    }
    for (std::size_t index = 0U; index < expected.size(); ++index) {
      if (expected[index] != checkpoint.sources[index].latest_position.record_sequence)
        return common::make_unexpected(
            invalid("subscription checkpoint omits a retained source suffix"));
    }
    return *total;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("subscription checkpoint validation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("subscription checkpoint exceeds container limits"));
  }
}

[[nodiscard]] common::Status write_change(common::ByteWriter& writer,
                                          const CommittedChange& change) {
  common::Status status = writer.write_exact(change.position.tablet_id.bytes());
  if (status.is_ok())
    status = writer.write_exact(change.position.wal_id.bytes);
  if (status.is_ok())
    status = writer.write_u64_le(change.position.record_sequence);
  if (status.is_ok())
    status = writer.write_exact(change.schema_id.bytes());
  if (status.is_ok())
    status = writer.write_u64_le(change.schema_version.value());
  if (status.is_ok())
    status = writer.write_u8(static_cast<std::uint8_t>(change.operation));
  if (status.is_ok())
    status = writer.zero_fill(7U);
  if (status.is_ok())
    status = writer.write_u32_le(static_cast<std::uint32_t>(change.result_key.size()));
  if (status.is_ok())
    status = writer.write_u32_le(static_cast<std::uint32_t>(change.payload.size()));
  if (status.is_ok())
    status = writer.write_exact(change.result_key);
  if (status.is_ok())
    status = writer.write_exact(change.payload);
  return status;
}

} // namespace

common::Result<std::vector<std::byte>> encode_multi_tablet_subscription_checkpoint_v1(
    const MultiTabletSubscriptionCheckpoint& checkpoint,
    const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("subscription checkpoint codec limits are invalid"));
  auto total = validate_and_size(checkpoint, limits);
  if (!total.has_value())
    return common::make_unexpected(total.error());
  try {
    std::vector<std::byte> bytes(*total, std::byte{0});
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kMultiTabletSubscriptionCheckpointHeaderSize);
    if (status.is_ok())
      status = writer.write_u64_le(bytes.size());
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(checkpoint.sources.size()));
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(checkpoint.retained_changes.size()));
    if (status.is_ok())
      status = writer.write_exact(checkpoint.database_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(checkpoint.table_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(checkpoint.plan_fingerprint);
    if (status.is_ok())
      status = writer.write_exact(checkpoint.schema_id.bytes());
    if (status.is_ok())
      status = writer.write_u64_le(checkpoint.schema_version.value());
    if (status.is_ok())
      status = writer.zero_fill(8U);
    for (const auto& source : checkpoint.sources) {
      if (status.is_ok())
        status = writer.write_exact(source.latest_position.tablet_id.bytes());
      if (status.is_ok())
        status = writer.write_exact(source.latest_position.wal_id.bytes);
      if (status.is_ok())
        status = writer.write_u64_le(source.latest_position.record_sequence);
      if (status.is_ok())
        status = writer.write_u64_le(source.expired_through_sequence);
    }
    for (const CommittedChange& change : checkpoint.retained_changes) {
      if (status.is_ok())
        status = write_change(writer, change);
    }
    if (!status.is_ok() || writer.remaining() != kMultiTabletSubscriptionCheckpointTrailerSize)
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "subscription checkpoint layout mismatch"});
    status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(
        bytes.size() - kMultiTabletSubscriptionCheckpointTrailerSize)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                    "subscription checkpoint trailer mismatch"});
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription checkpoint encoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("subscription checkpoint exceeds container limits"));
  }
}

common::Result<MultiTabletSubscriptionCheckpoint> decode_multi_tablet_subscription_checkpoint_v1(
    const common::ByteView bytes, const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("subscription checkpoint codec limits are invalid"));
  if (bytes.size() < kMultiTabletSubscriptionCheckpointHeaderSize +
                         kMultiTabletSubscriptionCheckpointTrailerSize ||
      bytes.size() > limits.maximum_checkpoint_bytes)
    return common::make_unexpected(corruption("subscription checkpoint size is invalid"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("subscription checkpoint magic is invalid"));
  common::ByteReader prefix{bytes};
  static_cast<void>(prefix.skip(kMagic.size()));
  auto major = prefix.read_u16_le();
  auto minor = prefix.read_u16_le();
  auto header_size = prefix.read_u32_le();
  auto total_size = prefix.read_u64_le();
  auto source_count = prefix.read_u32_le();
  auto change_count = prefix.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !header_size.has_value() ||
      !total_size.has_value() || !source_count.has_value() || !change_count.has_value())
    return common::make_unexpected(corruption("subscription checkpoint header is truncated"));
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("subscription checkpoint version is unsupported"));
  if (*header_size != kMultiTabletSubscriptionCheckpointHeaderSize || *total_size != bytes.size() ||
      *source_count == 0U || *source_count > limits.maximum_sources ||
      *change_count > limits.maximum_retained_changes)
    return common::make_unexpected(corruption("subscription checkpoint header fields are invalid"));
  const std::uint32_t stored_crc =
      load_u32(bytes, bytes.size() - kMultiTabletSubscriptionCheckpointTrailerSize);
  if (stored_crc !=
      common::crc32c(bytes.first(bytes.size() - kMultiTabletSubscriptionCheckpointTrailerSize)))
    return common::make_unexpected(corruption("subscription checkpoint checksum is invalid"));

  try {
    common::ByteReader reader{bytes};
    static_cast<void>(reader.skip(32U));
    auto database_bytes = reader.read_exact(16U);
    auto table_bytes = reader.read_exact(16U);
    auto plan = reader.read_exact(PlanFingerprint{}.size());
    auto schema_bytes = reader.read_exact(16U);
    auto schema_version = reader.read_u64_le();
    auto reserved = reader.read_exact(8U);
    if (!database_bytes.has_value() || !table_bytes.has_value() || !plan.has_value() ||
        !schema_bytes.has_value() || !schema_version.has_value() || !reserved.has_value() ||
        !std::ranges::all_of(*reserved,
                             [](const std::byte value) { return value == std::byte{0}; }))
      return common::make_unexpected(corruption("subscription checkpoint identity is invalid"));
    common::Uuid::Bytes database_array{};
    common::Uuid::Bytes table_array{};
    common::Uuid::Bytes schema_array{};
    std::ranges::copy(*database_bytes, database_array.begin());
    std::ranges::copy(*table_bytes, table_array.begin());
    std::ranges::copy(*schema_bytes, schema_array.begin());
    auto table_id = schema::TableId::from_bytes(table_array);
    auto schema_id = schema::SchemaId::from_bytes(schema_array);
    auto version = schema::SchemaVersion::from_value(*schema_version);
    common::Uuid database_id{database_array};
    if (database_id.is_nil() || !table_id.has_value() || !schema_id.has_value() ||
        !version.has_value())
      return common::make_unexpected(corruption("subscription checkpoint identity is invalid"));
    PlanFingerprint plan_array{};
    std::ranges::copy(*plan, plan_array.begin());
    MultiTabletSubscriptionCheckpoint checkpoint{database_id, *table_id, plan_array, *schema_id,
                                                 *version,    {},        {}};
    checkpoint.sources.reserve(*source_count);
    for (std::uint32_t index = 0U; index < *source_count; ++index) {
      auto tablet_bytes = reader.read_exact(16U);
      auto wal_bytes = reader.read_exact(16U);
      auto latest = reader.read_u64_le();
      auto expired = reader.read_u64_le();
      if (!tablet_bytes.has_value() || !wal_bytes.has_value() || !latest.has_value() ||
          !expired.has_value())
        return common::make_unexpected(corruption("subscription checkpoint source is truncated"));
      common::Uuid::Bytes tablet_array{};
      wal::WalId wal_id{};
      std::ranges::copy(*tablet_bytes, tablet_array.begin());
      std::ranges::copy(*wal_bytes, wal_id.bytes.begin());
      auto tablet_id = schema::TabletId::from_bytes(tablet_array);
      if (!tablet_id.has_value())
        return common::make_unexpected(corruption("subscription checkpoint tablet is invalid"));
      checkpoint.sources.push_back({{*tablet_id, wal_id, *latest}, *expired});
    }
    checkpoint.retained_changes.reserve(*change_count);
    for (std::uint32_t index = 0U; index < *change_count; ++index) {
      auto tablet_bytes = reader.read_exact(16U);
      auto wal_bytes = reader.read_exact(16U);
      auto sequence = reader.read_u64_le();
      auto change_schema_bytes = reader.read_exact(16U);
      auto change_version = reader.read_u64_le();
      auto operation = reader.read_u8();
      auto change_reserved = reader.read_exact(7U);
      auto key_size = reader.read_u32_le();
      auto payload_size = reader.read_u32_le();
      if (!tablet_bytes.has_value() || !wal_bytes.has_value() || !sequence.has_value() ||
          !change_schema_bytes.has_value() || !change_version.has_value() ||
          !operation.has_value() || !change_reserved.has_value() || !key_size.has_value() ||
          !payload_size.has_value() ||
          !std::ranges::all_of(*change_reserved,
                               [](const std::byte value) { return value == std::byte{0}; }) ||
          *key_size > limits.maximum_result_key_bytes ||
          *payload_size > limits.maximum_payload_bytes)
        return common::make_unexpected(corruption("subscription checkpoint change is invalid"));
      auto key = reader.read_exact(*key_size);
      auto payload = reader.read_exact(*payload_size);
      if (!key.has_value() || !payload.has_value())
        return common::make_unexpected(corruption("subscription checkpoint change is truncated"));
      common::Uuid::Bytes tablet_array{};
      common::Uuid::Bytes change_schema_array{};
      wal::WalId wal_id{};
      std::ranges::copy(*tablet_bytes, tablet_array.begin());
      std::ranges::copy(*wal_bytes, wal_id.bytes.begin());
      std::ranges::copy(*change_schema_bytes, change_schema_array.begin());
      auto tablet_id = schema::TabletId::from_bytes(tablet_array);
      auto change_schema_id = schema::SchemaId::from_bytes(change_schema_array);
      auto parsed_version = schema::SchemaVersion::from_value(*change_version);
      if (!tablet_id.has_value() || !change_schema_id.has_value() || !parsed_version.has_value())
        return common::make_unexpected(
            corruption("subscription checkpoint change identity is invalid"));
      checkpoint.retained_changes.push_back(
          {{*tablet_id, wal_id, *sequence},
           *change_schema_id,
           *parsed_version,
           static_cast<LogicalChangeOperation>(*operation),
           std::vector<std::byte>{key->begin(), key->end()},
           std::vector<std::byte>{payload->begin(), payload->end()}});
    }
    if (reader.remaining() != kMultiTabletSubscriptionCheckpointTrailerSize)
      return common::make_unexpected(corruption("subscription checkpoint has trailing bytes"));
    auto valid = validate_and_size(checkpoint, limits);
    if (!valid.has_value() || *valid != bytes.size())
      return common::make_unexpected(
          valid.has_value()
              ? corruption("subscription checkpoint layout is invalid")
              : common::Status{common::StatusCode::kCorruption, valid.error().message()});
    return checkpoint;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription checkpoint decoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("subscription checkpoint exceeds container limits"));
  }
}

common::Result<std::vector<std::byte>> encode_bound_multi_tablet_subscription_checkpoint_v1(
    const BoundMultiTabletSubscriptionCheckpoint& checkpoint,
    const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  if (!valid_limits(limits) || checkpoint.checkpoint_generation == 0U)
    return common::make_unexpected(
        invalid("bound subscription checkpoint generation or limits are invalid"));
  auto nested = encode_multi_tablet_subscription_checkpoint_v1(checkpoint.state, limits);
  if (!nested.has_value())
    return common::make_unexpected(nested.error());
  auto total =
      common::checked_add(kBoundMultiTabletSubscriptionCheckpointHeaderSize, nested->size());
  total = total.has_value()
              ? common::checked_add(*total, kBoundMultiTabletSubscriptionCheckpointTrailerSize)
              : std::nullopt;
  if (!total.has_value() || *total > limits.maximum_checkpoint_bytes)
    return common::make_unexpected(exhausted("bound subscription checkpoint exceeds size limit"));
  try {
    std::vector<std::byte> bytes(*total, std::byte{0});
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kBoundMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kBoundMultiTabletSubscriptionCheckpointHeaderSize);
    if (status.is_ok())
      status = writer.write_u64_le(bytes.size());
    if (status.is_ok())
      status = writer.write_u64_le(checkpoint.checkpoint_generation);
    if (status.is_ok())
      status = writer.write_u64_le(nested->size());
    if (status.is_ok())
      status = writer.zero_fill(24U);
    if (status.is_ok())
      status = writer.write_exact(*nested);
    if (!status.is_ok() || writer.remaining() != kBoundMultiTabletSubscriptionCheckpointTrailerSize)
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "bound subscription checkpoint layout mismatch"});
    status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(
        bytes.size() - kBoundMultiTabletSubscriptionCheckpointTrailerSize)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "bound subscription checkpoint trailer mismatch"});
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("bound subscription checkpoint encoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("bound subscription checkpoint exceeds container limits"));
  }
}

common::Result<BoundMultiTabletSubscriptionCheckpoint>
decode_bound_multi_tablet_subscription_checkpoint_v1(
    const common::ByteView bytes, const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("subscription checkpoint codec limits are invalid"));
  if (bytes.size() < kBoundMultiTabletSubscriptionCheckpointHeaderSize +
                         kMultiTabletSubscriptionCheckpointHeaderSize +
                         kMultiTabletSubscriptionCheckpointTrailerSize +
                         kBoundMultiTabletSubscriptionCheckpointTrailerSize ||
      bytes.size() > limits.maximum_checkpoint_bytes)
    return common::make_unexpected(corruption("bound subscription checkpoint size is invalid"));
  if (!std::ranges::equal(bytes.first(kBoundMagic.size()), kBoundMagic))
    return common::make_unexpected(corruption("bound subscription checkpoint magic is invalid"));
  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kBoundMagic.size()));
  auto major = reader.read_u16_le();
  auto minor = reader.read_u16_le();
  auto header_size = reader.read_u32_le();
  auto total_size = reader.read_u64_le();
  auto generation = reader.read_u64_le();
  auto nested_size = reader.read_u64_le();
  auto reserved = reader.read_exact(24U);
  if (!major.has_value() || !minor.has_value() || !header_size.has_value() ||
      !total_size.has_value() || !generation.has_value() || !nested_size.has_value() ||
      !reserved.has_value())
    return common::make_unexpected(corruption("bound subscription checkpoint header is truncated"));
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("bound subscription checkpoint version is unsupported"));
  const std::size_t expected_nested = bytes.size() -
                                      kBoundMultiTabletSubscriptionCheckpointHeaderSize -
                                      kBoundMultiTabletSubscriptionCheckpointTrailerSize;
  if (*header_size != kBoundMultiTabletSubscriptionCheckpointHeaderSize ||
      *total_size != bytes.size() || *generation == 0U || *nested_size != expected_nested ||
      !std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{0}; }))
    return common::make_unexpected(corruption("bound subscription checkpoint header is invalid"));
  const std::uint32_t stored_crc =
      load_u32(bytes, bytes.size() - kBoundMultiTabletSubscriptionCheckpointTrailerSize);
  if (stored_crc != common::crc32c(bytes.first(bytes.size() -
                                               kBoundMultiTabletSubscriptionCheckpointTrailerSize)))
    return common::make_unexpected(corruption("bound subscription checkpoint checksum is invalid"));
  auto nested = reader.read_exact(expected_nested);
  if (!nested.has_value() ||
      reader.remaining() != kBoundMultiTabletSubscriptionCheckpointTrailerSize)
    return common::make_unexpected(corruption("bound subscription checkpoint payload is invalid"));
  auto decoded = decode_multi_tablet_subscription_checkpoint_v1(*nested, limits);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error());
  return BoundMultiTabletSubscriptionCheckpoint{*generation, std::move(*decoded)};
}

} // namespace chronos::live
