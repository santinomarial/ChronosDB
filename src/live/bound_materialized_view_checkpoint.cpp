#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/live/materialized_view_checkpoint.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

constexpr std::array<std::byte, 8U> kBoundMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'M'},
                                                std::byte{'V'}, std::byte{'C'}, std::byte{'B'},
                                                std::byte{'1'}, std::byte{0U}};
constexpr std::uint16_t kBoundMajor = 1U;
constexpr std::uint16_t kBoundMinorLegacy = 0U;
constexpr std::uint16_t kBoundMinorGeneration = 1U;
constexpr std::size_t kTotalSizeOffset = 24U;
constexpr std::size_t kDatabaseIdOffset = 32U;
constexpr std::size_t kViewIdOffset = 48U;
constexpr std::size_t kTableIdOffset = 64U;
constexpr std::size_t kSchemaIdOffset = 80U;
constexpr std::size_t kSchemaVersionOffset = 96U;
constexpr std::size_t kPlanFingerprintOffset = 104U;
constexpr std::size_t kPayloadSizeOffset = 136U;
constexpr std::size_t kHeaderCrcOffset = 144U;
constexpr std::size_t kGenerationOffset = 148U;
constexpr std::size_t kGenerationReservedOffset = 156U;

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status unsupported(std::string message) {
  return common::Status{common::StatusCode::kNotSupported, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

void store_u32(std::span<std::byte> bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
  }
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes, const std::size_t offset) {
  std::uint16_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    const auto byte =
        static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + index]));
    value = static_cast<std::uint16_t>(value | static_cast<std::uint16_t>(byte << (index * 8U)));
  }
  return value;
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes, const std::size_t offset) {
  std::uint64_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

template <typename Identifier>
[[nodiscard]] common::Result<Identifier> read_identifier(const common::ByteView bytes,
                                                         const std::size_t offset) {
  common::Uuid::Bytes value{};
  std::ranges::copy(bytes.subspan(offset, value.size()), value.begin());
  return Identifier::from_bytes(value);
}

[[nodiscard]] bool valid_identity(const MaterializedViewCheckpointIdentity& identity) noexcept {
  return !identity.database_id.is_nil() && !identity.view_id.is_nil() &&
         !identity.table_id.uuid().is_nil() && !identity.schema_id.uuid().is_nil() &&
         identity.schema_version.value() != 0U;
}

[[nodiscard]] bool valid_limits(const MaterializedViewCheckpointCodecLimits limits) noexcept {
  return limits.maximum_checkpoint_bytes >= kBoundMaterializedViewCheckpointHeaderSize +
                                                kMaterializedViewCheckpointHeaderSize +
                                                kMaterializedViewCheckpointTrailerSize +
                                                kBoundMaterializedViewCheckpointTrailerSize &&
         limits.maximum_checkpoint_bytes <= kMaximumMaterializedViewCheckpointSize &&
         limits.maximum_rows > 0U && limits.maximum_windows > 0U &&
         limits.maximum_window_contributions > 0U;
}

} // namespace

common::Result<std::vector<std::byte>>
encode_bound_materialized_view_checkpoint_v1(const BoundMaterializedViewCheckpoint& checkpoint,
                                             const MaterializedViewCheckpointCodecLimits limits) {
  if (!valid_limits(limits)) {
    return common::make_unexpected(invalid("bound checkpoint codec limits are invalid"));
  }
  if (!valid_identity(checkpoint.identity)) {
    return common::make_unexpected(invalid("bound materialized-view identity is invalid"));
  }
  auto payload = encode_windowed_materialized_view_checkpoint_v1(checkpoint.state, limits);
  if (!payload.has_value()) {
    return common::make_unexpected(payload.error());
  }
  auto total = common::checked_add(kBoundMaterializedViewCheckpointHeaderSize, payload->size());
  total = total.has_value()
              ? common::checked_add(*total, kBoundMaterializedViewCheckpointTrailerSize)
              : std::nullopt;
  if (!total.has_value() || *total > limits.maximum_checkpoint_bytes ||
      payload->size() > std::numeric_limits<std::uint64_t>::max()) {
    return common::make_unexpected(
        exhausted("bound materialized-view checkpoint exceeds size limit"));
  }
  try {
    std::vector<std::byte> bytes(*total, std::byte{0U});
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kBoundMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kBoundMajor);
    if (status.is_ok())
      status = writer.write_u16_le(checkpoint.checkpoint_generation == 0U ? kBoundMinorLegacy
                                                                          : kBoundMinorGeneration);
    if (status.is_ok())
      status = writer.write_u32_le(kBoundMaterializedViewCheckpointHeaderSize);
    if (status.is_ok())
      status = writer.write_u32_le(0U);
    if (status.is_ok())
      status = writer.write_u32_le(0U);
    if (status.is_ok())
      status = writer.write_u64_le(bytes.size());
    if (status.is_ok())
      status = writer.write_exact(checkpoint.identity.database_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(checkpoint.identity.view_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(checkpoint.identity.table_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(checkpoint.identity.schema_id.bytes());
    if (status.is_ok())
      status = writer.write_u64_le(checkpoint.identity.schema_version.value());
    if (status.is_ok())
      status = writer.write_exact(checkpoint.identity.plan_fingerprint);
    if (status.is_ok())
      status = writer.write_u64_le(payload->size());
    if (status.is_ok())
      status = writer.write_u32_le(0U);
    if (status.is_ok())
      status = writer.write_u64_le(checkpoint.checkpoint_generation);
    if (status.is_ok())
      status = writer.zero_fill(4U);
    if (status.is_ok())
      status = writer.write_exact(*payload);
    if (status.is_ok())
      status = writer.write_u32_le(0U);
    if (!status.is_ok() || !writer.full()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal,
                         "bound materialized-view checkpoint layout is inconsistent"});
    }
    store_u32(
        bytes, kHeaderCrcOffset,
        common::crc32c(common::ByteView{bytes}.first(kBoundMaterializedViewCheckpointHeaderSize)));
    store_u32(bytes, bytes.size() - kBoundMaterializedViewCheckpointTrailerSize,
              common::crc32c(common::ByteView{bytes}.first(
                  bytes.size() - kBoundMaterializedViewCheckpointTrailerSize)));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("bound materialized-view allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("bound materialized-view checkpoint exceeded container limits"));
  }
}

common::Result<BoundMaterializedViewCheckpoint>
decode_bound_materialized_view_checkpoint_v1(const common::ByteView bytes,
                                             const MaterializedViewCheckpointCodecLimits limits) {
  if (!valid_limits(limits)) {
    return common::make_unexpected(invalid("bound checkpoint codec limits are invalid"));
  }
  if (bytes.size() <
      kBoundMaterializedViewCheckpointHeaderSize + kBoundMaterializedViewCheckpointTrailerSize) {
    return common::make_unexpected(corruption("bound materialized-view checkpoint is truncated"));
  }
  if (bytes.size() > limits.maximum_checkpoint_bytes) {
    return common::make_unexpected(
        exhausted("bound materialized-view checkpoint exceeds decode limit"));
  }
  std::array<std::byte, kBoundMaterializedViewCheckpointHeaderSize> header{};
  std::ranges::copy(bytes.first(header.size()), header.begin());
  const std::uint32_t header_crc = load_u32(bytes, kHeaderCrcOffset);
  store_u32(header, kHeaderCrcOffset, 0U);
  if (common::crc32c(header) != header_crc) {
    return common::make_unexpected(corruption("bound materialized-view header checksum mismatch"));
  }
  if (!std::ranges::equal(bytes.first(kBoundMagic.size()), kBoundMagic) ||
      load_u16(bytes, 8U) != kBoundMajor) {
    return common::make_unexpected(
        unsupported("bound materialized-view magic or major is unknown"));
  }
  const std::uint16_t minor = load_u16(bytes, 10U);
  if ((minor != kBoundMinorLegacy && minor != kBoundMinorGeneration) ||
      load_u32(bytes, 12U) != kBoundMaterializedViewCheckpointHeaderSize) {
    return common::make_unexpected(
        unsupported("bound materialized-view minor or header is unknown"));
  }
  const std::uint64_t total_size = load_u64(bytes, kTotalSizeOffset);
  const std::uint64_t payload_size = load_u64(bytes, kPayloadSizeOffset);
  const std::uint64_t checkpoint_generation = load_u64(bytes, kGenerationOffset);
  const auto framed = payload_size <= std::numeric_limits<std::size_t>::max()
                          ? common::checked_add(kBoundMaterializedViewCheckpointHeaderSize,
                                                static_cast<std::size_t>(payload_size))
                          : std::nullopt;
  const auto exact_size =
      framed.has_value() ? common::checked_add(*framed, kBoundMaterializedViewCheckpointTrailerSize)
                         : std::nullopt;
  if (total_size != bytes.size() || !exact_size.has_value() || *exact_size != bytes.size() ||
      load_u32(bytes, 16U) != 0U || load_u32(bytes, 20U) != 0U ||
      ((minor == kBoundMinorLegacy && checkpoint_generation != 0U) ||
       (minor == kBoundMinorGeneration && checkpoint_generation == 0U)) ||
      std::ranges::any_of(bytes.subspan(kGenerationReservedOffset, 4U),
                          [](const std::byte value) { return value != std::byte{0U}; }) ||
      common::crc32c(bytes.first(bytes.size() - kBoundMaterializedViewCheckpointTrailerSize)) !=
          load_u32(bytes, bytes.size() - kBoundMaterializedViewCheckpointTrailerSize)) {
    return common::make_unexpected(corruption("bound materialized-view framing is invalid"));
  }

  common::Uuid::Bytes database_bytes{};
  common::Uuid::Bytes view_bytes{};
  std::ranges::copy(bytes.subspan(kDatabaseIdOffset, database_bytes.size()),
                    database_bytes.begin());
  std::ranges::copy(bytes.subspan(kViewIdOffset, view_bytes.size()), view_bytes.begin());
  auto table_id = read_identifier<schema::TableId>(bytes, kTableIdOffset);
  auto schema_id = read_identifier<schema::SchemaId>(bytes, kSchemaIdOffset);
  auto schema_version = schema::SchemaVersion::from_value(load_u64(bytes, kSchemaVersionOffset));
  if (!table_id.has_value() || !schema_id.has_value() || !schema_version.has_value()) {
    return common::make_unexpected(
        corruption("bound materialized-view schema identity is invalid"));
  }
  MaterializedViewCheckpointIdentity identity{.database_id = common::Uuid{database_bytes},
                                              .view_id = common::Uuid{view_bytes},
                                              .table_id = *table_id,
                                              .schema_id = *schema_id,
                                              .schema_version = *schema_version};
  std::ranges::copy(bytes.subspan(kPlanFingerprintOffset, identity.plan_fingerprint.size()),
                    identity.plan_fingerprint.begin());
  if (!valid_identity(identity)) {
    return common::make_unexpected(corruption("bound materialized-view identity is invalid"));
  }
  const common::ByteView payload = bytes.subspan(kBoundMaterializedViewCheckpointHeaderSize,
                                                 static_cast<std::size_t>(payload_size));
  auto state = decode_windowed_materialized_view_checkpoint_v1(payload, limits);
  if (!state.has_value()) {
    return common::make_unexpected(state.error());
  }
  return BoundMaterializedViewCheckpoint{identity, checkpoint_generation, std::move(*state)};
}

} // namespace chronos::live
