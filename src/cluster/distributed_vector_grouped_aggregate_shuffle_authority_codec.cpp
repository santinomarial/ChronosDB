#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority_codec.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

using namespace distributed_vector_grouped_aggregate_shuffle_authority_format;

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'G'}, std::byte{'S'},
                                                  std::byte{'A'}, std::byte{'1'}};
inline constexpr std::size_t kHeaderCrcOffset = 68U;

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

[[nodiscard]] common::Result<std::size_t>
encoded_body_size(const DistributedVectorGroupedAggregateShuffleAuthority& authority) {
  auto sources = common::checked_multiply(authority.sources().size(), kSourceLength);
  auto destinations = common::checked_multiply(authority.destinations().size(), kDestinationLength);
  auto keys = common::checked_multiply(authority.key_definitions().size(), kKeyLength);
  auto aggregates =
      common::checked_multiply(authority.aggregate_definitions().size(), kAggregateLength);
  if (!sources.has_value() || !destinations.has_value() || !keys.has_value() ||
      !aggregates.has_value()) {
    return common::make_unexpected(exhausted("grouped shuffle authority size overflowed"));
  }
  auto first = common::checked_add(*sources, *destinations);
  auto second = first.has_value() ? common::checked_add(*first, *keys) : std::nullopt;
  auto result = second.has_value() ? common::checked_add(*second, *aggregates) : std::nullopt;
  if (!result.has_value())
    return common::make_unexpected(exhausted("grouped shuffle authority size overflowed"));
  return *result;
}

[[nodiscard]] common::Result<std::uint8_t>
operation_code(const query::VectorAggregateOperation operation) {
  const auto value = static_cast<std::uint8_t>(operation);
  if (value > static_cast<std::uint8_t>(query::VectorAggregateOperation::kVarianceSample))
    return common::make_unexpected(invalid("grouped shuffle aggregate operation is invalid"));
  return static_cast<std::uint8_t>(value + 1U);
}

[[nodiscard]] common::Result<query::VectorAggregateOperation>
decode_operation(const std::uint8_t code) {
  if (code == 0U ||
      code > static_cast<std::uint8_t>(query::VectorAggregateOperation::kVarianceSample) + 1U) {
    return common::make_unexpected(corruption("grouped shuffle aggregate operation is invalid"));
  }
  return static_cast<query::VectorAggregateOperation>(code - 1U);
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] common::Result<schema::LogicalType> decode_type(const std::uint16_t code,
                                                              const std::uint16_t parameter_0,
                                                              const std::uint16_t parameter_1) {
  const auto kind = schema::logical_type_kind_from_code(code);
  if (!kind.has_value())
    return common::make_unexpected(corruption("grouped shuffle authority type is unassigned"));
  auto type = schema::LogicalType::create(*kind, parameter_0, parameter_1);
  if (!type.has_value())
    return common::make_unexpected(corruption("grouped shuffle authority type is invalid"));
  return *type;
}

[[nodiscard]] bool all_zero(const common::ByteView bytes) {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{}; });
}

} // namespace

EncodedDistributedVectorGroupedAggregateShuffleAuthority::
    EncodedDistributedVectorGroupedAggregateShuffleAuthority(std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedVectorGroupedAggregateShuffleAuthority::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedVectorGroupedAggregateShuffleAuthority>
encode_distributed_vector_grouped_aggregate_shuffle_authority(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority) {
  const auto body_size = encoded_body_size(authority);
  if (!body_size.has_value())
    return common::make_unexpected(body_size.error());
  const auto frame_size = common::checked_add(kHeaderLength + kTrailerLength, *body_size);
  if (!frame_size.has_value() || *frame_size > kMaximumFrameLength)
    return common::make_unexpected(exhausted("grouped shuffle authority frame is too large"));
  try {
    std::vector<std::byte> bytes(*frame_size);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kHeaderLength);
    if (write.is_ok())
      write = writer.write_u64_le(*frame_size);
    if (write.is_ok())
      write = writer.write_exact(authority.query_id().bytes());
    if (write.is_ok())
      write = writer.write_u16_le(authority.hash_version());
    if (write.is_ok())
      write = writer.zero_fill(2U);
    if (write.is_ok())
      write = writer.write_u32_le(static_cast<std::uint32_t>(authority.sources().size()));
    if (write.is_ok())
      write = writer.write_u32_le(static_cast<std::uint32_t>(authority.destinations().size()));
    if (write.is_ok())
      write = writer.write_u32_le(static_cast<std::uint32_t>(authority.key_definitions().size()));
    if (write.is_ok())
      write =
          writer.write_u32_le(static_cast<std::uint32_t>(authority.aggregate_definitions().size()));
    if (write.is_ok())
      write = writer.write_u64_le(*body_size);
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.zero_fill(24U);
    for (const auto& source : authority.sources()) {
      if (write.is_ok())
        write = writer.write_exact(source.tablet_id.bytes());
      if (write.is_ok())
        write = writer.write_u64_le(source.node_id);
    }
    for (const auto& destination : authority.destinations()) {
      if (write.is_ok())
        write = writer.write_u32_le(destination.partition_id);
      if (write.is_ok())
        write = writer.zero_fill(4U);
      if (write.is_ok())
        write = writer.write_u64_le(destination.node_id);
    }
    for (const auto& key : authority.key_definitions()) {
      if (write.is_ok())
        write = writer.write_u64_le(key.column_ordinal);
      if (write.is_ok())
        write = writer.write_u16_le(key.type.code());
      if (write.is_ok())
        write = writer.write_u16_le(key.type.parameter_0());
      if (write.is_ok())
        write = writer.write_u16_le(key.type.parameter_1());
      if (write.is_ok())
        write = writer.write_u8(key.nullable ? 1U : 0U);
      if (write.is_ok())
        write = writer.zero_fill(9U);
    }
    for (const auto& aggregate : authority.aggregate_definitions()) {
      const auto operation = operation_code(aggregate.operation);
      if (!operation.has_value())
        return common::make_unexpected(operation.error());
      if (write.is_ok())
        write = writer.write_u8(*operation);
      if (write.is_ok())
        write = writer.write_u8(aggregate.input.has_value() ? 1U : 0U);
      if (write.is_ok())
        write = writer.write_u8(aggregate.input.has_value() && aggregate.input->nullable ? 1U : 0U);
      if (write.is_ok())
        write = writer.zero_fill(5U);
      if (write.is_ok())
        write =
            writer.write_u64_le(aggregate.input.has_value() ? aggregate.input->column_ordinal : 0U);
      if (write.is_ok())
        write =
            writer.write_u16_le(aggregate.input.has_value() ? aggregate.input->type.code() : 0U);
      if (write.is_ok())
        write = writer.write_u16_le(
            aggregate.input.has_value() ? aggregate.input->type.parameter_0() : 0U);
      if (write.is_ok())
        write = writer.write_u16_le(
            aggregate.input.has_value() ? aggregate.input->type.parameter_1() : 0U);
      if (write.is_ok())
        write = writer.zero_fill(10U);
    }
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("grouped shuffle authority layout failed"));
    return EncodedDistributedVectorGroupedAggregateShuffleAuthority{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle authority encoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle authority encoding exceeds limits"));
  }
}

common::Result<DistributedVectorGroupedAggregateShuffleAuthority>
decode_distributed_vector_grouped_aggregate_shuffle_authority_exact(
    const common::ByteView bytes,
    const DistributedVectorGroupedAggregateShuffleAuthorityDecodeLimits limits) {
  if (limits.maximum_frame_length < kMinimumFrameLength ||
      limits.maximum_frame_length > kMaximumFrameLength || limits.authority.maximum_sources == 0U ||
      limits.authority.maximum_sources > kMaximumDistributedVectorGroupedAggregateShuffleSources ||
      limits.authority.maximum_partitions == 0U ||
      limits.authority.maximum_partitions >
          query::kMaximumDistributedVectorGroupedAggregatePartitions ||
      limits.authority.maximum_retained_configuration_bytes == 0U ||
      limits.authority.maximum_retained_configuration_bytes >
          kMaximumDistributedVectorGroupedAggregateShuffleAuthorityBytes) {
    return common::make_unexpected(invalid("grouped shuffle authority decode limits are invalid"));
  }
  if (bytes.size() < kMinimumFrameLength || bytes.size() > kMaximumFrameLength)
    return common::make_unexpected(corruption("grouped shuffle authority frame length is invalid"));
  if (bytes.size() > limits.maximum_frame_length)
    return common::make_unexpected(exhausted("grouped shuffle authority exceeds the caller limit"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("grouped shuffle authority magic is invalid"));
  common::ByteReader crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto stored_header_crc = crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kHeaderCrcOffset)))
    return common::make_unexpected(corruption("grouped shuffle authority header checksum differs"));
  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto query_id = reader.read_exact(common::Uuid::kSize);
  const auto hash_version = reader.read_u16_le();
  const auto small_reserved = reader.read_exact(2U);
  const auto source_count = reader.read_u32_le();
  const auto destination_count = reader.read_u32_le();
  const auto key_count = reader.read_u32_le();
  const auto aggregate_count = reader.read_u32_le();
  const auto body_length = reader.read_u64_le();
  static_cast<void>(reader.skip(4U));
  const auto reserved = reader.read_exact(24U);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !query_id.has_value() || !hash_version.has_value() ||
      !small_reserved.has_value() || !source_count.has_value() || !destination_count.has_value() ||
      !key_count.has_value() || !aggregate_count.has_value() || !body_length.has_value() ||
      !reserved.has_value())
    return common::make_unexpected(corruption("grouped shuffle authority header is truncated"));
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(unsupported("grouped shuffle authority version is unsupported"));
  auto source_bytes =
      common::checked_multiply(static_cast<std::size_t>(*source_count), kSourceLength);
  auto destination_bytes =
      common::checked_multiply(static_cast<std::size_t>(*destination_count), kDestinationLength);
  auto key_bytes = common::checked_multiply(static_cast<std::size_t>(*key_count), kKeyLength);
  auto aggregate_bytes =
      common::checked_multiply(static_cast<std::size_t>(*aggregate_count), kAggregateLength);
  auto body_a = source_bytes.has_value() && destination_bytes.has_value()
                    ? common::checked_add(*source_bytes, *destination_bytes)
                    : std::nullopt;
  auto body_b = body_a.has_value() && key_bytes.has_value()
                    ? common::checked_add(*body_a, *key_bytes)
                    : std::nullopt;
  auto expected_body = body_b.has_value() && aggregate_bytes.has_value()
                           ? common::checked_add(*body_b, *aggregate_bytes)
                           : std::nullopt;
  if (*header_length != kHeaderLength || *frame_length != bytes.size() ||
      *hash_version != kDistributedVectorGroupedAggregateShuffleHashVersionV1 ||
      !all_zero(*small_reserved) || !all_zero(*reserved) || *source_count == 0U ||
      *destination_count == 0U || *key_count == 0U ||
      *key_count > query::kMaximumGroupedAggregateKeys ||
      *aggregate_count > query::kMaximumGroupedAggregateWidth || !expected_body.has_value() ||
      *body_length != *expected_body ||
      *body_length != bytes.size() - kHeaderLength - kTrailerLength)
    return common::make_unexpected(corruption("grouped shuffle authority header is invalid"));
  if (*source_count > limits.authority.maximum_sources ||
      *destination_count > limits.authority.maximum_partitions)
    return common::make_unexpected(exhausted("grouped shuffle authority exceeds count limits"));
  common::ByteReader trailer{bytes.last(kTrailerLength)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("grouped shuffle authority checksum differs"));
  try {
    common::Uuid::Bytes query_bytes{};
    std::ranges::copy(*query_id, query_bytes.begin());
    std::vector<DistributedVectorGroupedAggregateShuffleSource> sources;
    std::vector<DistributedVectorGroupedAggregateShuffleDestination> destinations;
    std::vector<query::VectorGroupKeyDefinition> keys;
    std::vector<query::VectorAggregateDefinition> aggregates;
    sources.reserve(*source_count);
    destinations.reserve(*destination_count);
    keys.reserve(*key_count);
    aggregates.reserve(*aggregate_count);
    for (std::uint32_t index = 0U; index < *source_count; ++index) {
      const auto tablet = reader.read_exact(common::Uuid::kSize);
      const auto node = reader.read_u64_le();
      if (!tablet.has_value() || !node.has_value())
        return common::make_unexpected(corruption("grouped shuffle authority source is truncated"));
      common::Uuid::Bytes tablet_bytes{};
      std::ranges::copy(*tablet, tablet_bytes.begin());
      auto tablet_id = schema::TabletId::from_bytes(tablet_bytes);
      if (!tablet_id.has_value())
        return common::make_unexpected(corruption("grouped shuffle authority source is invalid"));
      sources.push_back({*tablet_id, *node});
    }
    for (std::uint32_t index = 0U; index < *destination_count; ++index) {
      const auto partition = reader.read_u32_le();
      const auto descriptor_reserved = reader.read_exact(4U);
      const auto node = reader.read_u64_le();
      if (!partition.has_value() || !descriptor_reserved.has_value() || !node.has_value() ||
          !all_zero(*descriptor_reserved))
        return common::make_unexpected(
            corruption("grouped shuffle authority destination is invalid"));
      destinations.push_back({*partition, *node});
    }
    for (std::uint32_t index = 0U; index < *key_count; ++index) {
      const auto ordinal = reader.read_u64_le();
      const auto code = reader.read_u16_le();
      const auto parameter_0 = reader.read_u16_le();
      const auto parameter_1 = reader.read_u16_le();
      const auto nullable = reader.read_u8();
      const auto descriptor_reserved = reader.read_exact(9U);
      if (!ordinal.has_value() || !code.has_value() || !parameter_0.has_value() ||
          !parameter_1.has_value() || !nullable.has_value() || !descriptor_reserved.has_value() ||
          *nullable > 1U || !all_zero(*descriptor_reserved) ||
          *ordinal > std::numeric_limits<std::size_t>::max())
        return common::make_unexpected(corruption("grouped shuffle authority key is invalid"));
      auto type = decode_type(*code, *parameter_0, *parameter_1);
      if (!type.has_value())
        return common::make_unexpected(type.error());
      keys.push_back({static_cast<std::size_t>(*ordinal), *type, *nullable == 1U});
    }
    for (std::uint32_t index = 0U; index < *aggregate_count; ++index) {
      const auto operation = reader.read_u8();
      const auto has_input = reader.read_u8();
      const auto nullable = reader.read_u8();
      const auto prefix_reserved = reader.read_exact(5U);
      const auto ordinal = reader.read_u64_le();
      const auto code = reader.read_u16_le();
      const auto parameter_0 = reader.read_u16_le();
      const auto parameter_1 = reader.read_u16_le();
      const auto suffix_reserved = reader.read_exact(10U);
      if (!operation.has_value() || !has_input.has_value() || !nullable.has_value() ||
          !prefix_reserved.has_value() || !ordinal.has_value() || !code.has_value() ||
          !parameter_0.has_value() || !parameter_1.has_value() || !suffix_reserved.has_value() ||
          *has_input > 1U || *nullable > 1U || !all_zero(*prefix_reserved) ||
          !all_zero(*suffix_reserved) || *ordinal > std::numeric_limits<std::size_t>::max())
        return common::make_unexpected(
            corruption("grouped shuffle authority aggregate is invalid"));
      auto decoded_operation = decode_operation(*operation);
      if (!decoded_operation.has_value())
        return common::make_unexpected(decoded_operation.error());
      std::optional<query::VectorAggregateInput> input;
      if (*has_input == 0U) {
        if (*nullable != 0U || *ordinal != 0U || *code != 0U || *parameter_0 != 0U ||
            *parameter_1 != 0U)
          return common::make_unexpected(
              corruption("grouped shuffle aggregate absence is noncanonical"));
      } else {
        auto type = decode_type(*code, *parameter_0, *parameter_1);
        if (!type.has_value())
          return common::make_unexpected(type.error());
        input =
            query::VectorAggregateInput{static_cast<std::size_t>(*ordinal), *type, *nullable == 1U};
      }
      aggregates.push_back({*decoded_operation, input});
    }
    if (reader.remaining() != kTrailerLength)
      return common::make_unexpected(corruption("grouped shuffle authority body is noncanonical"));
    auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
        common::Uuid{query_bytes}, std::move(sources), std::move(destinations), std::move(keys),
        std::move(aggregates), limits.authority);
    if (!authority.has_value() &&
        authority.error().code() == common::StatusCode::kResourceExhausted)
      return common::make_unexpected(authority.error());
    if (!authority.has_value())
      return common::make_unexpected(corruption("grouped shuffle authority value is invalid"));
    return authority;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle authority decoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle authority decoding exceeds limits"));
  }
}

} // namespace chronos::cluster
