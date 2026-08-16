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

constexpr std::array<std::byte, 8U> kV1Magic{std::byte{'C'}, std::byte{'H'}, std::byte{'S'},
                                             std::byte{'U'}, std::byte{'B'}, std::byte{'C'},
                                             std::byte{'P'}, std::byte{'1'}};
constexpr std::array<std::byte, 8U> kV2Magic{std::byte{'C'}, std::byte{'H'}, std::byte{'S'},
                                             std::byte{'U'}, std::byte{'B'}, std::byte{'C'},
                                             std::byte{'P'}, std::byte{'2'}};
constexpr std::array<std::byte, 8U> kBoundV1Magic{std::byte{'C'}, std::byte{'H'}, std::byte{'S'},
                                                  std::byte{'U'}, std::byte{'B'}, std::byte{'C'},
                                                  std::byte{'G'}, std::byte{'1'}};
constexpr std::array<std::byte, 8U> kBoundV2Magic{std::byte{'C'}, std::byte{'H'}, std::byte{'S'},
                                                  std::byte{'U'}, std::byte{'B'}, std::byte{'C'},
                                                  std::byte{'G'}, std::byte{'2'}};
constexpr std::uint16_t kCheckpointMinorInitial = 0U;
constexpr std::uint16_t kCheckpointMinorSchemaState = 1U;
constexpr std::uint16_t kBoundMinor = 0U;
constexpr std::uint8_t kPlanSchemaInvalidated = 1U;

struct CheckpointLayout {
  std::array<std::byte, 8U> magic;
  std::uint16_t major{};
  std::size_t source_size{};
  std::size_t change_envelope_size{};
  bool source_tagged{};
};

struct BoundLayout {
  std::array<std::byte, 8U> magic;
  std::uint16_t major{};
};

constexpr CheckpointLayout kV1Layout{kV1Magic, 1U, kMultiTabletSubscriptionCheckpointV1SourceSize,
                                     kMultiTabletSubscriptionCheckpointV1ChangeEnvelopeSize, false};
constexpr CheckpointLayout kV2Layout{kV2Magic, 2U, kMultiTabletSubscriptionCheckpointV2SourceSize,
                                     kMultiTabletSubscriptionCheckpointV2ChangeEnvelopeSize, true};
constexpr BoundLayout kBoundV1Layout{kBoundV1Magic, 1U};
constexpr BoundLayout kBoundV2Layout{kBoundV2Magic, 2U};

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

[[nodiscard]] common::Result<std::size_t> add_size(const std::size_t left,
                                                   const std::size_t right) {
  const auto result = common::checked_add(left, right);
  if (!result.has_value())
    return common::make_unexpected(exhausted("subscription checkpoint size overflows"));
  return result.value();
}

[[nodiscard]] common::Result<std::size_t> multiply_size(const std::size_t left,
                                                        const std::size_t right) {
  const auto result = common::checked_multiply(left, right);
  if (!result.has_value())
    return common::make_unexpected(exhausted("subscription checkpoint size overflows"));
  return result.value();
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

[[nodiscard]] common::Result<const CheckpointLayout*>
checkpoint_layout(const common::ByteView bytes, const std::optional<std::uint16_t> required_major) {
  if (bytes.size() < 12U)
    return common::make_unexpected(corruption("subscription checkpoint header is truncated"));
  common::ByteReader reader{bytes.subspan(8U)};
  auto major = reader.read_u16_le();
  auto minor = reader.read_u16_le();
  if (!major.has_value() || !minor.has_value())
    return common::make_unexpected(corruption("subscription checkpoint version is truncated"));
  const CheckpointLayout* layout = nullptr;
  if (*major == kV1Layout.major)
    layout = &kV1Layout;
  else if (*major == kV2Layout.major)
    layout = &kV2Layout;
  else
    return common::make_unexpected(unsupported("subscription checkpoint version is unsupported"));
  if (*minor > kCheckpointMinorSchemaState ||
      (required_major.has_value() && *required_major != layout->major))
    return common::make_unexpected(unsupported("subscription checkpoint version is unsupported"));
  if (!std::ranges::equal(bytes.first(layout->magic.size()), layout->magic))
    return common::make_unexpected(
        corruption("subscription checkpoint magic and version disagree"));
  return layout;
}

[[nodiscard]] common::Result<const BoundLayout*>
bound_layout(const common::ByteView bytes, const std::optional<std::uint16_t> required_major) {
  if (bytes.size() < 12U)
    return common::make_unexpected(corruption("bound subscription checkpoint header is truncated"));
  common::ByteReader reader{bytes.subspan(8U)};
  auto major = reader.read_u16_le();
  auto minor = reader.read_u16_le();
  if (!major.has_value() || !minor.has_value())
    return common::make_unexpected(
        corruption("bound subscription checkpoint version is truncated"));
  const BoundLayout* layout = nullptr;
  if (*major == kBoundV1Layout.major)
    layout = &kBoundV1Layout;
  else if (*major == kBoundV2Layout.major)
    layout = &kBoundV2Layout;
  else
    return common::make_unexpected(
        unsupported("bound subscription checkpoint version is unsupported"));
  if (*minor != kBoundMinor || (required_major.has_value() && *required_major != layout->major))
    return common::make_unexpected(
        unsupported("bound subscription checkpoint version is unsupported"));
  if (!std::ranges::equal(bytes.first(layout->magic.size()), layout->magic))
    return common::make_unexpected(
        corruption("bound subscription checkpoint magic and version disagree"));
  return layout;
}

[[nodiscard]] common::Result<std::size_t>
validate_and_size(const MultiTabletSubscriptionCheckpoint& checkpoint,
                  const MultiTabletSubscriptionCheckpointCodecLimits& limits,
                  const CheckpointLayout& layout) {
  if (checkpoint.database_id.is_nil() || checkpoint.table_id.uuid().is_nil() ||
      checkpoint.schema_id.uuid().is_nil() || checkpoint.schema_version.value() == 0U ||
      checkpoint.sources.empty() || checkpoint.sources.size() > limits.maximum_sources ||
      checkpoint.retained_changes.size() > limits.maximum_retained_changes)
    return common::make_unexpected(
        invalid("subscription checkpoint identity or counts are invalid"));
  if (!checkpoint.plan_schema_compatible && !checkpoint.retained_changes.empty())
    return common::make_unexpected(
        invalid("schema-incompatible subscription checkpoint retains replay changes"));
  try {
    std::map<schema::TabletId, std::size_t> indexes;
    std::vector<std::uint64_t> expected;
    expected.reserve(checkpoint.sources.size());
    for (std::size_t index = 0U; index < checkpoint.sources.size(); ++index) {
      const auto& source = checkpoint.sources[index];
      if (!source.latest_position.is_valid() ||
          (!layout.source_tagged &&
           source.latest_position.source_kind != SubscriptionSourceKind::kWal) ||
          source.expired_through_sequence > source.latest_position.record_sequence ||
          (index != 0U && checkpoint.sources[index - 1U].latest_position.tablet_id >=
                              source.latest_position.tablet_id) ||
          !indexes.emplace(source.latest_position.tablet_id, index).second)
        return common::make_unexpected(invalid("subscription checkpoint source vector is invalid"));
      expected.push_back(source.expired_through_sequence);
    }

    auto total = add_size(kMultiTabletSubscriptionCheckpointHeaderSize,
                          kMultiTabletSubscriptionCheckpointTrailerSize);
    auto source_bytes = multiply_size(checkpoint.sources.size(), layout.source_size);
    if (!total.has_value())
      return common::make_unexpected(total.error());
    if (!source_bytes.has_value())
      return common::make_unexpected(source_bytes.error());
    total = add_size(*total, *source_bytes);
    if (!total.has_value())
      return common::make_unexpected(total.error());
    std::size_t total_bytes = *total;
    if (total_bytes > limits.maximum_checkpoint_bytes)
      return common::make_unexpected(exhausted("subscription checkpoint exceeds size limit"));
    for (const CommittedChange& change : checkpoint.retained_changes) {
      const auto found = indexes.find(change.position.tablet_id);
      if (found == indexes.end())
        return common::make_unexpected(invalid("subscription checkpoint change source is unknown"));
      const std::size_t index = found->second;
      if (!change.position.is_valid() ||
          (!layout.source_tagged && change.position.source_kind != SubscriptionSourceKind::kWal) ||
          !change.position.same_source(checkpoint.sources[index].latest_position) ||
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
      auto change_size = add_size(layout.change_envelope_size, change.result_key.size());
      if (!change_size.has_value())
        return common::make_unexpected(change_size.error());
      change_size = add_size(*change_size, change.payload.size());
      if (!change_size.has_value())
        return common::make_unexpected(change_size.error());
      auto next_total = add_size(total_bytes, *change_size);
      if (!next_total.has_value())
        return common::make_unexpected(next_total.error());
      total_bytes = *next_total;
      if (total_bytes > limits.maximum_checkpoint_bytes)
        return common::make_unexpected(exhausted("subscription checkpoint exceeds size limit"));
      expected[index] = change.position.record_sequence;
    }
    for (std::size_t index = 0U; index < expected.size(); ++index) {
      if (expected[index] != checkpoint.sources[index].latest_position.record_sequence)
        return common::make_unexpected(
            invalid("subscription checkpoint omits a retained source suffix"));
    }
    return total_bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("subscription checkpoint validation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("subscription checkpoint exceeds container limits"));
  }
}

[[nodiscard]] common::Status write_source_position(common::ByteWriter& writer,
                                                   const SourcePosition& position,
                                                   const bool source_tagged) {
  common::Status status = writer.write_exact(position.tablet_id.bytes());
  if (status.is_ok() && source_tagged)
    status = writer.write_u8(static_cast<std::uint8_t>(position.source_kind));
  if (status.is_ok() && source_tagged)
    status = writer.zero_fill(7U);
  if (status.is_ok()) {
    status = position.source_kind == SubscriptionSourceKind::kWal
                 ? writer.write_exact(position.wal_id.bytes)
                 : writer.write_exact(position.raft_group_id.bytes());
  }
  if (status.is_ok())
    status = writer.write_u64_le(position.record_sequence);
  return status;
}

[[nodiscard]] common::Status write_change(common::ByteWriter& writer, const CommittedChange& change,
                                          const bool source_tagged) {
  common::Status status = write_source_position(writer, change.position, source_tagged);
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

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_checkpoint(const MultiTabletSubscriptionCheckpoint& checkpoint,
                  const MultiTabletSubscriptionCheckpointCodecLimits limits,
                  const CheckpointLayout& layout) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("subscription checkpoint codec limits are invalid"));
  auto total = validate_and_size(checkpoint, limits, layout);
  if (!total.has_value())
    return common::make_unexpected(total.error());
  try {
    std::vector<std::byte> bytes(*total, std::byte{0});
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(layout.magic);
    if (status.is_ok())
      status = writer.write_u16_le(layout.major);
    if (status.is_ok())
      status = writer.write_u16_le(checkpoint.plan_schema_compatible ? kCheckpointMinorInitial
                                                                     : kCheckpointMinorSchemaState);
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
      status = writer.write_u8(checkpoint.plan_schema_compatible ? 0U : kPlanSchemaInvalidated);
    if (status.is_ok())
      status = writer.zero_fill(7U);
    for (const auto& source : checkpoint.sources) {
      if (status.is_ok())
        status = write_source_position(writer, source.latest_position, layout.source_tagged);
      if (status.is_ok())
        status = writer.write_u64_le(source.expired_through_sequence);
    }
    for (const CommittedChange& change : checkpoint.retained_changes) {
      if (status.is_ok())
        status = write_change(writer, change, layout.source_tagged);
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

[[nodiscard]] common::Result<SourcePosition> read_source_position(common::ByteReader& reader,
                                                                  const bool source_tagged) {
  auto tablet_bytes = reader.read_exact(common::Uuid::kSize);
  std::optional<std::uint8_t> source_kind;
  common::Result<common::ByteView> reserved = common::ByteView{};
  if (source_tagged) {
    auto decoded_kind = reader.read_u8();
    if (decoded_kind.has_value())
      source_kind = *decoded_kind;
    reserved = reader.read_exact(7U);
  }
  auto source_bytes = reader.read_exact(common::Uuid::kSize);
  auto sequence = reader.read_u64_le();
  if (!tablet_bytes.has_value() || (source_tagged && !source_kind.has_value()) ||
      !reserved.has_value() || !source_bytes.has_value() || !sequence.has_value())
    return common::make_unexpected(corruption("subscription checkpoint source is truncated"));
  if (source_tagged &&
      std::ranges::any_of(*reserved, [](const std::byte value) { return value != std::byte{0}; }))
    return common::make_unexpected(
        unsupported("subscription checkpoint source flags are unsupported"));

  common::Uuid::Bytes tablet_array{};
  common::Uuid::Bytes source_array{};
  std::ranges::copy(*tablet_bytes, tablet_array.begin());
  std::ranges::copy(*source_bytes, source_array.begin());
  auto tablet_id = schema::TabletId::from_bytes(tablet_array);
  if (!tablet_id.has_value())
    return common::make_unexpected(corruption("subscription checkpoint tablet is invalid"));
  const std::uint8_t kind =
      source_kind.value_or(static_cast<std::uint8_t>(SubscriptionSourceKind::kWal));
  SourcePosition position =
      SourcePosition::wal(*tablet_id, wal::WalId{.bytes = source_array}, *sequence);
  if (kind == static_cast<std::uint8_t>(SubscriptionSourceKind::kRaft))
    position = SourcePosition::raft(*tablet_id, common::Uuid{source_array}, *sequence);
  else if (kind != static_cast<std::uint8_t>(SubscriptionSourceKind::kWal))
    return common::make_unexpected(
        unsupported("subscription checkpoint source kind is unsupported"));
  if (!position.is_valid())
    return common::make_unexpected(corruption("subscription checkpoint source is invalid"));
  return position;
}

[[nodiscard]] common::Result<MultiTabletSubscriptionCheckpoint>
decode_checkpoint(const common::ByteView bytes,
                  const MultiTabletSubscriptionCheckpointCodecLimits limits,
                  const std::optional<std::uint16_t> required_major) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("subscription checkpoint codec limits are invalid"));
  if (bytes.size() < kMultiTabletSubscriptionCheckpointHeaderSize +
                         kMultiTabletSubscriptionCheckpointTrailerSize ||
      bytes.size() > limits.maximum_checkpoint_bytes)
    return common::make_unexpected(corruption("subscription checkpoint size is invalid"));
  auto layout = checkpoint_layout(bytes, required_major);
  if (!layout.has_value())
    return common::make_unexpected(layout.error());

  common::ByteReader prefix{bytes};
  static_cast<void>(prefix.skip((*layout)->magic.size() + 4U));
  auto header_size = prefix.read_u32_le();
  auto total_size = prefix.read_u64_le();
  auto source_count = prefix.read_u32_le();
  auto change_count = prefix.read_u32_le();
  if (!header_size.has_value() || !total_size.has_value() || !source_count.has_value() ||
      !change_count.has_value())
    return common::make_unexpected(corruption("subscription checkpoint header is truncated"));
  if (*header_size != kMultiTabletSubscriptionCheckpointHeaderSize || *total_size != bytes.size() ||
      *source_count == 0U || *source_count > limits.maximum_sources ||
      *change_count > limits.maximum_retained_changes)
    return common::make_unexpected(corruption("subscription checkpoint header fields are invalid"));
  auto minimum_size = multiply_size(*source_count, (*layout)->source_size);
  auto minimum_changes = multiply_size(*change_count, (*layout)->change_envelope_size);
  if (minimum_size.has_value())
    minimum_size = add_size(kMultiTabletSubscriptionCheckpointHeaderSize, *minimum_size);
  if (minimum_size.has_value() && minimum_changes.has_value())
    minimum_size = add_size(*minimum_size, *minimum_changes);
  if (minimum_size.has_value())
    minimum_size = add_size(*minimum_size, kMultiTabletSubscriptionCheckpointTrailerSize);
  if (!minimum_size.has_value() || *minimum_size > bytes.size())
    return common::make_unexpected(corruption("subscription checkpoint counts exceed its size"));
  const std::uint32_t stored_crc =
      load_u32(bytes, bytes.size() - kMultiTabletSubscriptionCheckpointTrailerSize);
  if (stored_crc !=
      common::crc32c(bytes.first(bytes.size() - kMultiTabletSubscriptionCheckpointTrailerSize)))
    return common::make_unexpected(corruption("subscription checkpoint checksum is invalid"));

  try {
    common::ByteReader reader{bytes};
    static_cast<void>(reader.skip(32U));
    auto database_bytes = reader.read_exact(common::Uuid::kSize);
    auto table_bytes = reader.read_exact(common::Uuid::kSize);
    auto plan = reader.read_exact(PlanFingerprint{}.size());
    auto schema_bytes = reader.read_exact(common::Uuid::kSize);
    auto schema_version = reader.read_u64_le();
    auto reserved = reader.read_exact(8U);
    if (!database_bytes.has_value() || !table_bytes.has_value() || !plan.has_value() ||
        !schema_bytes.has_value() || !schema_version.has_value() || !reserved.has_value())
      return common::make_unexpected(corruption("subscription checkpoint identity is invalid"));
    const std::uint16_t minor = static_cast<std::uint16_t>(
        std::to_integer<std::uint8_t>(bytes[10U]) |
        (static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[11U])) << 8U));
    const std::uint8_t schema_flags = std::to_integer<std::uint8_t>((*reserved)[0]);
    if ((minor == kCheckpointMinorInitial && schema_flags != 0U) ||
        (minor == kCheckpointMinorSchemaState && schema_flags != kPlanSchemaInvalidated) ||
        !std::ranges::all_of(reserved->subspan(1U),
                             [](const std::byte value) { return value == std::byte{0}; }))
      return common::make_unexpected(
          corruption("subscription checkpoint schema-state flags are invalid"));
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
    checkpoint.plan_schema_compatible = schema_flags == 0U;
    checkpoint.sources.reserve(*source_count);
    for (std::uint32_t index = 0U; index < *source_count; ++index) {
      auto position = read_source_position(reader, (*layout)->source_tagged);
      auto expired = reader.read_u64_le();
      if (!position.has_value())
        return common::make_unexpected(position.error());
      if (!expired.has_value())
        return common::make_unexpected(corruption("subscription checkpoint source is truncated"));
      checkpoint.sources.push_back({*position, *expired});
    }
    checkpoint.retained_changes.reserve(*change_count);
    for (std::uint32_t index = 0U; index < *change_count; ++index) {
      auto position = read_source_position(reader, (*layout)->source_tagged);
      auto change_schema_bytes = reader.read_exact(common::Uuid::kSize);
      auto change_version = reader.read_u64_le();
      auto operation = reader.read_u8();
      auto change_reserved = reader.read_exact(7U);
      auto key_size = reader.read_u32_le();
      auto payload_size = reader.read_u32_le();
      if (!position.has_value())
        return common::make_unexpected(position.error());
      if (!change_schema_bytes.has_value() || !change_version.has_value() ||
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
      common::Uuid::Bytes change_schema_array{};
      std::ranges::copy(*change_schema_bytes, change_schema_array.begin());
      auto change_schema_id = schema::SchemaId::from_bytes(change_schema_array);
      auto parsed_version = schema::SchemaVersion::from_value(*change_version);
      if (!change_schema_id.has_value() || !parsed_version.has_value())
        return common::make_unexpected(
            corruption("subscription checkpoint change identity is invalid"));
      checkpoint.retained_changes.push_back(
          {*position, *change_schema_id, *parsed_version,
           static_cast<LogicalChangeOperation>(*operation),
           std::vector<std::byte>{key->begin(), key->end()},
           std::vector<std::byte>{payload->begin(), payload->end()}});
    }
    if (reader.remaining() != kMultiTabletSubscriptionCheckpointTrailerSize)
      return common::make_unexpected(corruption("subscription checkpoint has trailing bytes"));
    auto valid = validate_and_size(checkpoint, limits, **layout);
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

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_bound_checkpoint(const BoundMultiTabletSubscriptionCheckpoint& checkpoint,
                        const MultiTabletSubscriptionCheckpointCodecLimits limits,
                        const BoundLayout& layout) {
  if (!valid_limits(limits) || checkpoint.checkpoint_generation == 0U)
    return common::make_unexpected(
        invalid("bound subscription checkpoint generation or limits are invalid"));
  const CheckpointLayout& nested_layout = layout.major == 1U ? kV1Layout : kV2Layout;
  auto nested = encode_checkpoint(checkpoint.state, limits, nested_layout);
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
    common::Status status = writer.write_exact(layout.magic);
    if (status.is_ok())
      status = writer.write_u16_le(layout.major);
    if (status.is_ok())
      status = writer.write_u16_le(kBoundMinor);
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

[[nodiscard]] common::Result<BoundMultiTabletSubscriptionCheckpoint>
decode_bound_checkpoint(const common::ByteView bytes,
                        const MultiTabletSubscriptionCheckpointCodecLimits limits,
                        const std::optional<std::uint16_t> required_major) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("subscription checkpoint codec limits are invalid"));
  if (bytes.size() < kBoundMultiTabletSubscriptionCheckpointHeaderSize +
                         kMultiTabletSubscriptionCheckpointHeaderSize +
                         kMultiTabletSubscriptionCheckpointTrailerSize +
                         kBoundMultiTabletSubscriptionCheckpointTrailerSize ||
      bytes.size() > limits.maximum_checkpoint_bytes)
    return common::make_unexpected(corruption("bound subscription checkpoint size is invalid"));
  auto layout = bound_layout(bytes, required_major);
  if (!layout.has_value())
    return common::make_unexpected(layout.error());
  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip((*layout)->magic.size() + 4U));
  auto header_size = reader.read_u32_le();
  auto total_size = reader.read_u64_le();
  auto generation = reader.read_u64_le();
  auto nested_size = reader.read_u64_le();
  auto reserved = reader.read_exact(24U);
  if (!header_size.has_value() || !total_size.has_value() || !generation.has_value() ||
      !nested_size.has_value() || !reserved.has_value())
    return common::make_unexpected(corruption("bound subscription checkpoint header is truncated"));
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
  auto decoded = decode_checkpoint(*nested, limits, (*layout)->major);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error());
  return BoundMultiTabletSubscriptionCheckpoint{*generation, std::move(*decoded)};
}

} // namespace

common::Result<std::vector<std::byte>> encode_multi_tablet_subscription_checkpoint_v1(
    const MultiTabletSubscriptionCheckpoint& checkpoint,
    const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  return encode_checkpoint(checkpoint, limits, kV1Layout);
}

common::Result<MultiTabletSubscriptionCheckpoint> decode_multi_tablet_subscription_checkpoint_v1(
    const common::ByteView bytes, const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  return decode_checkpoint(bytes, limits, kV1Layout.major);
}

common::Result<std::vector<std::byte>> encode_multi_tablet_subscription_checkpoint_v2(
    const MultiTabletSubscriptionCheckpoint& checkpoint,
    const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  return encode_checkpoint(checkpoint, limits, kV2Layout);
}

common::Result<MultiTabletSubscriptionCheckpoint> decode_multi_tablet_subscription_checkpoint_v2(
    const common::ByteView bytes, const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  return decode_checkpoint(bytes, limits, kV2Layout.major);
}

common::Result<MultiTabletSubscriptionCheckpoint> decode_multi_tablet_subscription_checkpoint(
    const common::ByteView bytes, const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  return decode_checkpoint(bytes, limits, std::nullopt);
}

common::Result<std::vector<std::byte>> encode_bound_multi_tablet_subscription_checkpoint_v1(
    const BoundMultiTabletSubscriptionCheckpoint& checkpoint,
    const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  return encode_bound_checkpoint(checkpoint, limits, kBoundV1Layout);
}

common::Result<BoundMultiTabletSubscriptionCheckpoint>
decode_bound_multi_tablet_subscription_checkpoint_v1(
    const common::ByteView bytes, const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  return decode_bound_checkpoint(bytes, limits, kBoundV1Layout.major);
}

common::Result<std::vector<std::byte>> encode_bound_multi_tablet_subscription_checkpoint_v2(
    const BoundMultiTabletSubscriptionCheckpoint& checkpoint,
    const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  return encode_bound_checkpoint(checkpoint, limits, kBoundV2Layout);
}

common::Result<BoundMultiTabletSubscriptionCheckpoint>
decode_bound_multi_tablet_subscription_checkpoint_v2(
    const common::ByteView bytes, const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  return decode_bound_checkpoint(bytes, limits, kBoundV2Layout.major);
}

common::Result<BoundMultiTabletSubscriptionCheckpoint>
decode_bound_multi_tablet_subscription_checkpoint(
    const common::ByteView bytes, const MultiTabletSubscriptionCheckpointCodecLimits limits) {
  return decode_bound_checkpoint(bytes, limits, std::nullopt);
}

} // namespace chronos::live
