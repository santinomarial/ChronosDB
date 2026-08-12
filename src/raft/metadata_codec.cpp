#include "chronos/raft/metadata_codec.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                           std::byte{'N'}, std::byte{'M'}, std::byte{'D'},
                                           std::byte{'C'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kHeaderCrcOffset = 36U;

enum class CommandKind : std::uint8_t {
  kNode = 1U,
  kSchema = 2U,
  kTablet = 3U,
  kRetention = 4U,
  kTablePolicy = 5U,
};

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

[[nodiscard]] bool valid_limits(const MetadataCommandCodecLimits& limits) {
  return limits.maximum_command_bytes >= kMetadataCommandHeaderSize + kMetadataCommandTrailerSize &&
         limits.maximum_command_bytes <= kMaximumMetadataCommandSize &&
         limits.maximum_endpoint_bytes > 0U &&
         limits.maximum_endpoint_bytes <= kMaximumMetadataCommandSize - kMetadataCommandHeaderSize -
                                              kMetadataCommandTrailerSize - 12U &&
         limits.maximum_replicas > 0U &&
         limits.maximum_replicas <= (kMaximumMetadataCommandSize - kMetadataCommandHeaderSize -
                                     kMetadataCommandTrailerSize - 56U) /
                                        sizeof(NodeId);
}

void store_u32(std::span<std::byte> bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
  }
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes, const std::size_t offset) {
  return static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset])) |
         static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + 1U]) << 8U);
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

[[nodiscard]] common::Status write_uuid(common::ByteWriter& writer, const common::Uuid& value) {
  return writer.write_exact(value.bytes());
}

template <typename Identifier>
[[nodiscard]] common::Result<Identifier> read_identifier(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value()) {
    return common::make_unexpected(bytes.error());
  }
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  return Identifier::from_bytes(owned);
}

[[nodiscard]] common::Result<std::pair<CommandKind, std::vector<std::byte>>>
encode_payload(MetadataCommand command, const MetadataCommandCodecLimits& limits) {
  CommandKind kind = CommandKind::kNode;
  std::size_t size = 0U;
  common::Status validation = std::visit(
      [&](auto& value) -> common::Status {
        using T = std::remove_cvref_t<decltype(value)>;
        if constexpr (std::is_same_v<T, ClusterNodeMetadata>) {
          kind = CommandKind::kNode;
          if (value.node_id == 0U || value.endpoint.empty() ||
              value.endpoint.size() > limits.maximum_endpoint_bytes ||
              value.endpoint.size() > std::numeric_limits<std::uint32_t>::max()) {
            return invalid("metadata node command is invalid");
          }
          size = 12U + value.endpoint.size();
        } else if constexpr (std::is_same_v<T, SchemaMetadata>) {
          kind = CommandKind::kSchema;
          if (value.table_id.uuid().is_nil() || value.schema_id.uuid().is_nil()) {
            return invalid("metadata schema command is invalid");
          }
          size = 40U;
        } else if constexpr (std::is_same_v<T, TabletPlacementMetadata>) {
          kind = CommandKind::kTablet;
          std::ranges::sort(value.replicas);
          if (value.table_id.uuid().is_nil() || value.tablet_id.uuid().is_nil() ||
              value.placement_epoch == 0U || value.replicas.empty() ||
              value.replicas.size() > limits.maximum_replicas || value.replicas.front() == 0U ||
              std::adjacent_find(value.replicas.begin(), value.replicas.end()) !=
                  value.replicas.end() ||
              (value.leader_hint.has_value() &&
               !std::binary_search(value.replicas.begin(), value.replicas.end(),
                                   *value.leader_hint))) {
            return invalid("metadata tablet command is invalid");
          }
          size = 56U + value.replicas.size() * sizeof(NodeId);
        } else if constexpr (std::is_same_v<T, RetentionMetadata>) {
          kind = CommandKind::kRetention;
          if (value.table_id.uuid().is_nil() || value.system_history_ns < 0 ||
              value.retry_retention_positions == 0U) {
            return invalid("metadata retention command is invalid");
          }
          size = 32U;
        } else {
          kind = CommandKind::kTablePolicy;
          if (value.table_id.uuid().is_nil() || value.partition_interval_ns <= 0 ||
              value.retention_ns <= 0 || value.system_history_ns <= 0 ||
              value.allowed_lateness_ns < 0 || value.retry_retention_positions == 0U) {
            return invalid("metadata complete table policy command is invalid");
          }
          size = 64U;
        }
        return common::Status::ok();
      },
      command);
  if (!validation.is_ok()) {
    return common::make_unexpected(validation);
  }
  if (size >
      limits.maximum_command_bytes - kMetadataCommandHeaderSize - kMetadataCommandTrailerSize) {
    return common::make_unexpected(exhausted("metadata command exceeds codec limit"));
  }
  std::vector<std::byte> payload(size, std::byte{0U});
  common::ByteWriter writer{payload};
  common::Status status = std::visit(
      [&](const auto& value) -> common::Status {
        using T = std::remove_cvref_t<decltype(value)>;
        if constexpr (std::is_same_v<T, ClusterNodeMetadata>) {
          if (auto result = writer.write_u64_le(value.node_id); !result.is_ok())
            return result;
          if (auto result = writer.write_u32_le(static_cast<std::uint32_t>(value.endpoint.size()));
              !result.is_ok())
            return result;
          return writer.write_exact(std::as_bytes(std::span{value.endpoint}));
        } else if constexpr (std::is_same_v<T, SchemaMetadata>) {
          if (auto result = write_uuid(writer, value.table_id.uuid()); !result.is_ok())
            return result;
          if (auto result = write_uuid(writer, value.schema_id.uuid()); !result.is_ok())
            return result;
          return writer.write_u64_le(value.schema_version.value());
        } else if constexpr (std::is_same_v<T, TabletPlacementMetadata>) {
          if (auto result = write_uuid(writer, value.table_id.uuid()); !result.is_ok())
            return result;
          if (auto result = write_uuid(writer, value.tablet_id.uuid()); !result.is_ok())
            return result;
          if (auto result = writer.write_u64_le(value.placement_epoch); !result.is_ok())
            return result;
          if (auto result = writer.write_u32_le(static_cast<std::uint32_t>(value.replicas.size()));
              !result.is_ok())
            return result;
          if (auto result = writer.write_u8(value.leader_hint.has_value() ? 1U : 0U);
              !result.is_ok())
            return result;
          if (auto result = writer.zero_fill(3U); !result.is_ok())
            return result;
          if (auto result = writer.write_u64_le(value.leader_hint.value_or(0U)); !result.is_ok())
            return result;
          for (const NodeId replica : value.replicas) {
            if (auto result = writer.write_u64_le(replica); !result.is_ok())
              return result;
          }
          return common::Status::ok();
        } else if constexpr (std::is_same_v<T, RetentionMetadata>) {
          if (auto result = write_uuid(writer, value.table_id.uuid()); !result.is_ok())
            return result;
          if (auto result = writer.write_i64_le(value.system_history_ns); !result.is_ok())
            return result;
          return writer.write_u64_le(value.retry_retention_positions);
        } else {
          for (const common::Status& result :
               {write_uuid(writer, value.table_id.uuid()),
                writer.write_i64_le(value.partition_interval_ns),
                writer.write_i64_le(value.retention_ns),
                writer.write_i64_le(value.system_history_ns),
                writer.write_i64_le(value.allowed_lateness_ns),
                writer.write_u64_le(value.retry_retention_positions), writer.zero_fill(8U)}) {
            if (!result.is_ok())
              return result;
          }
          return common::Status::ok();
        }
      },
      command);
  if (!status.is_ok() || !writer.full()) {
    return common::make_unexpected(status.is_ok() ? corruption("metadata payload size mismatch")
                                                  : status);
  }
  return std::pair{kind, std::move(payload)};
}

[[nodiscard]] common::Result<MetadataCommand>
decode_payload(const CommandKind kind, const common::ByteView payload,
               const MetadataCommandCodecLimits& limits) {
  common::ByteReader reader{payload};
  if (kind == CommandKind::kNode) {
    auto node_id = reader.read_u64_le();
    auto length = reader.read_u32_le();
    if (!node_id.has_value() || !length.has_value() || *node_id == 0U || *length == 0U ||
        *length > limits.maximum_endpoint_bytes || *length != reader.remaining()) {
      return common::make_unexpected(corruption("metadata node payload is invalid"));
    }
    auto endpoint = reader.read_exact(*length);
    if (!endpoint.has_value())
      return common::make_unexpected(endpoint.error());
    std::string owned(endpoint->size(), '\0');
    std::memcpy(owned.data(), endpoint->data(), endpoint->size());
    return MetadataCommand{ClusterNodeMetadata{*node_id, std::move(owned)}};
  }
  if (kind == CommandKind::kSchema) {
    auto table_id = read_identifier<schema::TableId>(reader);
    auto schema_id = read_identifier<schema::SchemaId>(reader);
    auto version = reader.read_u64_le();
    if (!table_id.has_value() || !schema_id.has_value() || !version.has_value() ||
        !reader.empty()) {
      return common::make_unexpected(corruption("metadata schema payload is invalid"));
    }
    auto schema_version = schema::SchemaVersion::from_value(*version);
    if (!schema_version.has_value())
      return common::make_unexpected(corruption("metadata schema version is invalid"));
    return MetadataCommand{SchemaMetadata{*table_id, *schema_id, *schema_version}};
  }
  if (kind == CommandKind::kTablet) {
    auto table_id = read_identifier<schema::TableId>(reader);
    auto tablet_id = read_identifier<schema::TabletId>(reader);
    auto epoch = reader.read_u64_le();
    auto count = reader.read_u32_le();
    auto has_leader = reader.read_u8();
    auto reserved = reader.read_exact(3U);
    auto leader = reader.read_u64_le();
    if (!table_id.has_value() || !tablet_id.has_value() || !epoch.has_value() ||
        !count.has_value() || !has_leader.has_value() || !reserved.has_value() ||
        !leader.has_value() || *epoch == 0U || *count == 0U || *count > limits.maximum_replicas ||
        *has_leader > 1U ||
        std::ranges::any_of(*reserved, [](std::byte byte) { return byte != std::byte{0U}; }) ||
        reader.remaining() != static_cast<std::size_t>(*count) * sizeof(NodeId)) {
      return common::make_unexpected(corruption("metadata tablet payload is invalid"));
    }
    std::vector<NodeId> replicas;
    replicas.reserve(*count);
    for (std::uint32_t index = 0U; index < *count; ++index) {
      auto replica = reader.read_u64_le();
      if (!replica.has_value())
        return common::make_unexpected(replica.error());
      replicas.push_back(*replica);
    }
    if (replicas.front() == 0U || !std::ranges::is_sorted(replicas) ||
        std::adjacent_find(replicas.begin(), replicas.end()) != replicas.end() ||
        (*has_leader == 0U ? *leader != 0U
                           : !std::binary_search(replicas.begin(), replicas.end(), *leader))) {
      return common::make_unexpected(corruption("metadata tablet membership is noncanonical"));
    }
    return MetadataCommand{TabletPlacementMetadata{
        *table_id, *tablet_id, *epoch, std::move(replicas),
        *has_leader == 0U ? std::optional<NodeId>{} : std::optional<NodeId>{*leader}}};
  }
  if (kind == CommandKind::kRetention) {
    auto table_id = read_identifier<schema::TableId>(reader);
    auto history = reader.read_i64_le();
    auto retries = reader.read_u64_le();
    if (!table_id.has_value() || !history.has_value() || !retries.has_value() || !reader.empty() ||
        *history < 0 || *retries == 0U) {
      return common::make_unexpected(corruption("metadata retention payload is invalid"));
    }
    return MetadataCommand{RetentionMetadata{*table_id, *history, *retries}};
  }
  if (kind == CommandKind::kTablePolicy) {
    auto table_id = read_identifier<schema::TableId>(reader);
    auto partition = reader.read_i64_le();
    auto retention = reader.read_i64_le();
    auto history = reader.read_i64_le();
    auto lateness = reader.read_i64_le();
    auto retries = reader.read_u64_le();
    auto reserved = reader.read_exact(8U);
    if (!table_id.has_value() || !partition.has_value() || !retention.has_value() ||
        !history.has_value() || !lateness.has_value() || !retries.has_value() ||
        !reserved.has_value() || !reader.empty() || *partition <= 0 || *retention <= 0 ||
        *history <= 0 || *lateness < 0 || *retries == 0U ||
        std::ranges::any_of(*reserved,
                            [](const std::byte byte) { return byte != std::byte{0U}; })) {
      return common::make_unexpected(corruption("metadata complete table policy is invalid"));
    }
    return MetadataCommand{
        TablePolicyMetadata{*table_id, *partition, *retention, *history, *lateness, *retries}};
  }
  return common::make_unexpected(unsupported("metadata command kind is unsupported"));
}

} // namespace

common::Result<std::vector<std::byte>>
encode_metadata_command_v1(MetadataCommand command, const MetadataCommandCodecLimits limits) {
  if (!valid_limits(limits)) {
    return common::make_unexpected(invalid("metadata codec limits are invalid"));
  }
  auto encoded_payload = encode_payload(std::move(command), limits);
  if (!encoded_payload.has_value()) {
    return common::make_unexpected(encoded_payload.error());
  }
  const auto& [kind, payload] = *encoded_payload;
  const std::size_t total_size =
      kMetadataCommandHeaderSize + payload.size() + kMetadataCommandTrailerSize;
  if (total_size > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(exhausted("metadata command size overflows format"));
  }
  std::vector<std::byte> bytes(total_size, std::byte{0U});
  common::ByteWriter writer{bytes};
  for (const common::Status& status :
       {writer.write_exact(kMagic), writer.write_u16_le(kMajor), writer.write_u16_le(kMinor),
        writer.write_u32_le(kMetadataCommandHeaderSize),
        writer.write_u32_le(static_cast<std::uint32_t>(total_size)),
        writer.write_u32_le(static_cast<std::uint32_t>(payload.size())),
        writer.write_u8(static_cast<std::uint8_t>(kind)), writer.zero_fill(7U),
        writer.write_u32_le(common::crc32c(payload)), writer.write_u32_le(0U), writer.zero_fill(8U),
        writer.write_exact(payload), writer.write_u32_le(0U)}) {
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  store_u32(bytes, kHeaderCrcOffset,
            common::crc32c(common::ByteView{bytes}.first(kMetadataCommandHeaderSize)));
  store_u32(
      bytes, total_size - kMetadataCommandTrailerSize,
      common::crc32c(common::ByteView{bytes}.first(total_size - kMetadataCommandTrailerSize)));
  return bytes;
}

common::Result<MetadataCommand>
decode_metadata_command_v1(const common::ByteView bytes, const MetadataCommandCodecLimits limits) {
  if (!valid_limits(limits)) {
    return common::make_unexpected(invalid("metadata codec limits are invalid"));
  }
  if (bytes.size() < kMetadataCommandHeaderSize + kMetadataCommandTrailerSize) {
    return common::make_unexpected(corruption("metadata command is shorter than fixed framing"));
  }
  std::array<std::byte, kMetadataCommandHeaderSize> header{};
  std::ranges::copy(bytes.first(kMetadataCommandHeaderSize), header.begin());
  const std::uint32_t stored_header_crc = load_u32(bytes, kHeaderCrcOffset);
  store_u32(header, kHeaderCrcOffset, 0U);
  if (common::crc32c(header) != stored_header_crc) {
    return common::make_unexpected(corruption("metadata command header checksum mismatch"));
  }
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic) || load_u16(bytes, 8U) != kMajor) {
    return common::make_unexpected(unsupported("metadata command magic or major version unknown"));
  }
  if (load_u16(bytes, 10U) != kMinor || load_u32(bytes, 12U) != kMetadataCommandHeaderSize) {
    return common::make_unexpected(unsupported("metadata command minor version or header unknown"));
  }
  const std::uint32_t total_size = load_u32(bytes, 16U);
  const std::uint32_t payload_size = load_u32(bytes, 20U);
  if (total_size != bytes.size() || total_size > limits.maximum_command_bytes ||
      payload_size != total_size - kMetadataCommandHeaderSize - kMetadataCommandTrailerSize ||
      std::ranges::any_of(bytes.subspan(25U, 7U),
                          [](std::byte byte) { return byte != std::byte{0U}; }) ||
      std::ranges::any_of(bytes.subspan(40U, 8U),
                          [](std::byte byte) { return byte != std::byte{0U}; })) {
    return common::make_unexpected(corruption("metadata command header relationships invalid"));
  }
  const common::ByteView payload = bytes.subspan(kMetadataCommandHeaderSize, payload_size);
  if (common::crc32c(payload) != load_u32(bytes, 32U) ||
      common::crc32c(bytes.first(total_size - kMetadataCommandTrailerSize)) !=
          load_u32(bytes, total_size - kMetadataCommandTrailerSize)) {
    return common::make_unexpected(
        corruption("metadata command payload or trailer checksum mismatch"));
  }
  const auto kind = static_cast<CommandKind>(std::to_integer<std::uint8_t>(bytes[24U]));
  return decode_payload(kind, payload, limits);
}

} // namespace chronos::raft
