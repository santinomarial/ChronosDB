#include "chronos/query/distributed_fragment.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'F'}, std::byte{'R'}, std::byte{'A'},
                                                  std::byte{'G'}, std::byte{'1'}};
inline constexpr std::array<std::byte, 8U> kGroupedMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'F'},
    std::byte{'G'}, std::byte{'R'}, std::byte{'P'}, std::byte{'1'}};
inline constexpr std::uint32_t kLowerPresent = 1U << 0U;
inline constexpr std::uint32_t kLowerInclusive = 1U << 1U;
inline constexpr std::uint32_t kUpperPresent = 1U << 2U;
inline constexpr std::uint32_t kUpperInclusive = 1U << 3U;
inline constexpr std::uint32_t kMaximumStalenessPresent = 1U << 4U;
inline constexpr std::uint32_t kBarrierPresent = 1U << 5U;
inline constexpr std::uint32_t kKnownFlags = kLowerPresent | kLowerInclusive | kUpperPresent |
                                             kUpperInclusive | kMaximumStalenessPresent |
                                             kBarrierPresent;
inline constexpr std::size_t kHeaderCrcOffset =
    distributed_fragment_format::kHeaderLength - sizeof(std::uint32_t);
inline constexpr std::size_t kGroupedHeaderCrcOffset =
    distributed_grouped_float64_fragment_format::kHeaderLength - sizeof(std::uint32_t);

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status validate_fragment(const DistributedAggregateFragment& fragment) {
  if (fragment.query_id.is_nil() || fragment.database_id.uuid().is_nil() ||
      fragment.table_id.uuid().is_nil() || fragment.tablet_id.uuid().is_nil() ||
      fragment.destination_schema_id.uuid().is_nil() || fragment.snapshot_generation == 0U ||
      fragment.serving_node == 0U || fragment.placement_epoch == 0U) {
    return invalid("distributed fragment identity or snapshot route is invalid");
  }
  if (fragment.destination_column_ordinals.empty() ||
      fragment.destination_column_ordinals.size() >
          distributed_fragment_format::kMaximumProjectionColumns ||
      fragment.aggregate_input_index >= fragment.destination_column_ordinals.size()) {
    return invalid("distributed fragment projection or aggregate input is invalid");
  }
  std::bitset<schema::kMaximumSchemaColumnCount> seen;
  for (const std::uint32_t ordinal : fragment.destination_column_ordinals) {
    if (ordinal >= schema::kMaximumSchemaColumnCount || seen[ordinal])
      return invalid("distributed fragment projection ordinals are invalid or duplicated");
    seen.set(ordinal);
  }
  if (fragment.event_time_predicate.has_value() &&
      !fragment.event_time_predicate->lower.has_value() &&
      !fragment.event_time_predicate->upper.has_value()) {
    return invalid("distributed fragment empty event-time predicate is not canonical");
  }

  switch (fragment.read_policy.consistency) {
  case DistributedReadConsistency::kLeaderLinearizable:
    if (fragment.read_policy.maximum_staleness_positions.has_value() ||
        !fragment.linearizable_barrier.has_value() || fragment.linearizable_barrier->term == 0U ||
        fragment.linearizable_barrier->context == 0U ||
        fragment.linearizable_barrier->read_index == 0U ||
        fragment.applied_position < fragment.linearizable_barrier->read_index) {
      return invalid("distributed fragment linearizable admission is invalid");
    }
    break;
  case DistributedReadConsistency::kFollowerBoundedStale: {
    if (!fragment.read_policy.maximum_staleness_positions.has_value() ||
        fragment.linearizable_barrier.has_value() ||
        fragment.observed_leader_commit_position == 0U) {
      return invalid("distributed fragment bounded-stale admission is invalid");
    }
    const std::uint64_t lag =
        fragment.observed_leader_commit_position > fragment.applied_position
            ? fragment.observed_leader_commit_position - fragment.applied_position
            : 0U;
    if (lag > *fragment.read_policy.maximum_staleness_positions)
      return invalid("distributed fragment bounded-stale admission exceeds its lag");
    break;
  }
  case DistributedReadConsistency::kLocalEventual:
    if (fragment.read_policy.maximum_staleness_positions.has_value() ||
        fragment.linearizable_barrier.has_value()) {
      return invalid("distributed fragment local-eventual admission is invalid");
    }
    break;
  default:
    return invalid("distributed fragment consistency mode is invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] std::size_t encoded_length(const std::size_t columns) noexcept {
  return distributed_fragment_format::kHeaderLength + columns * sizeof(std::uint32_t) +
         distributed_fragment_format::kTrailerLength;
}

} // namespace

EncodedDistributedAggregateFragment::EncodedDistributedAggregateFragment(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedAggregateFragment::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedAggregateFragment>
encode_distributed_aggregate_fragment(const DistributedAggregateFragment& fragment) {
  const common::Status validation = validate_fragment(fragment);
  if (!validation.is_ok())
    return common::make_unexpected(validation);

  try {
    std::vector<std::byte> bytes(encoded_length(fragment.destination_column_ordinals.size()));
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_fragment_format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_fragment_format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(distributed_fragment_format::kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(bytes.size());
    if (status.is_ok())
      status = writer.write_exact(fragment.query_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(fragment.database_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(fragment.table_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(fragment.tablet_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(fragment.destination_schema_id.bytes());
    if (status.is_ok())
      status = writer.write_u64_le(fragment.snapshot_generation);
    if (status.is_ok())
      status = writer.write_u64_le(fragment.serving_node);
    if (status.is_ok())
      status = writer.write_u64_le(fragment.applied_position);
    if (status.is_ok())
      status = writer.write_u64_le(fragment.observed_leader_commit_position);
    if (status.is_ok())
      status = writer.write_u64_le(fragment.placement_epoch);
    if (status.is_ok())
      status = writer.write_u64_le(fragment.read_policy.maximum_staleness_positions.value_or(0U));
    if (status.is_ok())
      status = writer.write_u64_le(
          fragment.linearizable_barrier.has_value() ? fragment.linearizable_barrier->term : 0U);
    if (status.is_ok())
      status = writer.write_u64_le(
          fragment.linearizable_barrier.has_value() ? fragment.linearizable_barrier->context : 0U);
    if (status.is_ok())
      status = writer.write_u64_le(fragment.linearizable_barrier.has_value()
                                       ? fragment.linearizable_barrier->read_index
                                       : 0U);
    if (status.is_ok())
      status = writer.write_u32_le(
          static_cast<std::uint32_t>(fragment.destination_column_ordinals.size()));
    if (status.is_ok())
      status = writer.write_u32_le(fragment.aggregate_input_index);

    std::uint32_t flags = 0U;
    std::int64_t lower = 0;
    std::int64_t upper = 0;
    if (fragment.event_time_predicate.has_value() &&
        fragment.event_time_predicate->lower.has_value()) {
      flags |= kLowerPresent;
      lower = fragment.event_time_predicate->lower->value;
      if (fragment.event_time_predicate->lower->inclusive)
        flags |= kLowerInclusive;
    }
    if (fragment.event_time_predicate.has_value() &&
        fragment.event_time_predicate->upper.has_value()) {
      flags |= kUpperPresent;
      upper = fragment.event_time_predicate->upper->value;
      if (fragment.event_time_predicate->upper->inclusive)
        flags |= kUpperInclusive;
    }
    if (fragment.read_policy.maximum_staleness_positions.has_value())
      flags |= kMaximumStalenessPresent;
    if (fragment.linearizable_barrier.has_value())
      flags |= kBarrierPresent;
    if (status.is_ok())
      status = writer.write_u32_le(flags);
    if (status.is_ok())
      status = writer.write_u8(static_cast<std::uint8_t>(fragment.read_policy.consistency));
    if (status.is_ok())
      status = writer.zero_fill(3U);
    if (status.is_ok())
      status = writer.write_i64_le(lower);
    if (status.is_ok())
      status = writer.write_i64_le(upper);
    if (status.is_ok())
      status = writer.zero_fill(4U);
    if (!status.is_ok() || writer.offset() != kHeaderCrcOffset) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "distributed fragment header layout is inconsistent"});
    }
    status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    for (const std::uint32_t ordinal : fragment.destination_column_ordinals) {
      if (status.is_ok())
        status = writer.write_u32_le(ordinal);
    }
    if (status.is_ok())
      status =
          writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!status.is_ok() || !writer.full()) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "distributed fragment frame layout is inconsistent"});
    }
    return EncodedDistributedAggregateFragment{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed fragment allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed fragment exceeds container limits"});
  }
}

common::Result<DistributedAggregateFragment>
decode_distributed_aggregate_fragment_exact(const common::ByteView bytes,
                                            const DistributedFragmentDecodeLimits limits) {
  if (limits.maximum_projection_columns == 0U ||
      limits.maximum_projection_columns > distributed_fragment_format::kMaximumProjectionColumns) {
    return common::make_unexpected(invalid("distributed fragment decode limits are invalid"));
  }
  if (bytes.size() < distributed_fragment_format::kHeaderLength +
                         distributed_fragment_format::kTrailerLength ||
      bytes.size() > distributed_fragment_format::kMaximumFrameLength) {
    return common::make_unexpected(corruption("distributed fragment frame length is invalid"));
  }
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("distributed fragment magic is invalid"));

  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, sizeof(std::uint32_t))};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kHeaderCrcOffset))) {
    return common::make_unexpected(corruption("distributed fragment header checksum is invalid"));
  }

  common::ByteReader reader{bytes};
  if (!reader.skip(kMagic.size()).is_ok())
    return common::make_unexpected(corruption("distributed fragment header is truncated"));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto query_bytes = reader.read_exact(common::Uuid::kSize);
  const auto database_bytes = reader.read_exact(common::Uuid::kSize);
  const auto table_bytes = reader.read_exact(common::Uuid::kSize);
  const auto tablet_bytes = reader.read_exact(common::Uuid::kSize);
  const auto schema_bytes = reader.read_exact(common::Uuid::kSize);
  const auto snapshot_generation = reader.read_u64_le();
  const auto serving_node = reader.read_u64_le();
  const auto applied_position = reader.read_u64_le();
  const auto observed_commit = reader.read_u64_le();
  const auto placement_epoch = reader.read_u64_le();
  const auto maximum_staleness = reader.read_u64_le();
  const auto barrier_term = reader.read_u64_le();
  const auto barrier_context = reader.read_u64_le();
  const auto barrier_index = reader.read_u64_le();
  const auto projection_count = reader.read_u32_le();
  const auto aggregate_input_index = reader.read_u32_le();
  const auto flags = reader.read_u32_le();
  const auto consistency = reader.read_u8();
  const auto small_reserved = reader.read_exact(3U);
  const auto lower = reader.read_i64_le();
  const auto upper = reader.read_i64_le();
  const auto reserved = reader.read_exact(4U);
  const auto header_crc = reader.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !query_bytes.has_value() || !database_bytes.has_value() ||
      !table_bytes.has_value() || !tablet_bytes.has_value() || !schema_bytes.has_value() ||
      !snapshot_generation.has_value() || !serving_node.has_value() ||
      !applied_position.has_value() || !observed_commit.has_value() ||
      !placement_epoch.has_value() || !maximum_staleness.has_value() || !barrier_term.has_value() ||
      !barrier_context.has_value() || !barrier_index.has_value() || !projection_count.has_value() ||
      !aggregate_input_index.has_value() || !flags.has_value() || !consistency.has_value() ||
      !small_reserved.has_value() || !lower.has_value() || !upper.has_value() ||
      !reserved.has_value() || !header_crc.has_value()) {
    return common::make_unexpected(corruption("distributed fragment header is truncated"));
  }
  if (*major != distributed_fragment_format::kMajor ||
      *minor != distributed_fragment_format::kMinor) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                  "distributed fragment version is unsupported"});
  }
  if (*header_length != distributed_fragment_format::kHeaderLength ||
      (*flags & ~kKnownFlags) != 0U ||
      std::ranges::any_of(*small_reserved,
                          [](const std::byte value) { return value != std::byte{0}; }) ||
      std::ranges::any_of(*reserved, [](const std::byte value) { return value != std::byte{0}; })) {
    return common::make_unexpected(corruption("distributed fragment header fields are invalid"));
  }
  if (*projection_count == 0U ||
      *projection_count > distributed_fragment_format::kMaximumProjectionColumns) {
    return common::make_unexpected(corruption("distributed fragment projection count is invalid"));
  }
  if (*projection_count > limits.maximum_projection_columns) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted, "distributed fragment projection limit exceeded"});
  }
  const std::size_t expected_length = encoded_length(*projection_count);
  if (*frame_length != expected_length || bytes.size() != expected_length)
    return common::make_unexpected(corruption("distributed fragment exact length is invalid"));
  common::ByteReader trailer_reader{bytes.last(4U)};
  const auto stored_frame_crc = trailer_reader.read_u32_le();
  if (!stored_frame_crc.has_value() ||
      *stored_frame_crc != common::crc32c(bytes.first(bytes.size() - 4U))) {
    return common::make_unexpected(corruption("distributed fragment frame checksum is invalid"));
  }

  if (((*flags & kLowerInclusive) != 0U && (*flags & kLowerPresent) == 0U) ||
      ((*flags & kUpperInclusive) != 0U && (*flags & kUpperPresent) == 0U) ||
      ((*flags & kLowerPresent) == 0U && *lower != 0) ||
      ((*flags & kUpperPresent) == 0U && *upper != 0) ||
      ((*flags & kMaximumStalenessPresent) == 0U && *maximum_staleness != 0U) ||
      ((*flags & kBarrierPresent) == 0U &&
       (*barrier_term != 0U || *barrier_context != 0U || *barrier_index != 0U))) {
    return common::make_unexpected(
        corruption("distributed fragment optional fields are not canonical"));
  }

  common::Uuid::Bytes query_id_bytes{};
  common::Uuid::Bytes database_id_bytes{};
  common::Uuid::Bytes table_id_bytes{};
  common::Uuid::Bytes tablet_id_bytes{};
  common::Uuid::Bytes schema_id_bytes{};
  std::ranges::copy(*query_bytes, query_id_bytes.begin());
  std::ranges::copy(*database_bytes, database_id_bytes.begin());
  std::ranges::copy(*table_bytes, table_id_bytes.begin());
  std::ranges::copy(*tablet_bytes, tablet_id_bytes.begin());
  std::ranges::copy(*schema_bytes, schema_id_bytes.begin());
  const auto database_id = manifest::DatabaseId::from_bytes(database_id_bytes);
  const auto table_id = schema::TableId::from_bytes(table_id_bytes);
  const auto tablet_id = schema::TabletId::from_bytes(tablet_id_bytes);
  const auto schema_id = schema::SchemaId::from_bytes(schema_id_bytes);
  if (common::Uuid{query_id_bytes}.is_nil() || !database_id.has_value() || !table_id.has_value() ||
      !tablet_id.has_value() || !schema_id.has_value()) {
    return common::make_unexpected(corruption("distributed fragment identity is invalid"));
  }

  DistributedReadConsistency decoded_consistency{DistributedReadConsistency::kLeaderLinearizable};
  switch (*consistency) {
  case static_cast<std::uint8_t>(DistributedReadConsistency::kLeaderLinearizable):
    decoded_consistency = DistributedReadConsistency::kLeaderLinearizable;
    break;
  case static_cast<std::uint8_t>(DistributedReadConsistency::kFollowerBoundedStale):
    decoded_consistency = DistributedReadConsistency::kFollowerBoundedStale;
    break;
  case static_cast<std::uint8_t>(DistributedReadConsistency::kLocalEventual):
    decoded_consistency = DistributedReadConsistency::kLocalEventual;
    break;
  default:
    return common::make_unexpected(corruption("distributed fragment consistency is invalid"));
  }

  try {
    std::vector<std::uint32_t> projection;
    projection.reserve(*projection_count);
    for (std::uint32_t index = 0U; index < *projection_count; ++index) {
      const auto ordinal = reader.read_u32_le();
      if (!ordinal.has_value()) {
        return common::make_unexpected(corruption("distributed fragment projection is truncated"));
      }
      projection.push_back(*ordinal);
    }
    if (reader.remaining() != distributed_fragment_format::kTrailerLength)
      return common::make_unexpected(corruption("distributed fragment body layout is invalid"));

    std::optional<cseg::EventTimePredicate> predicate;
    if ((*flags & (kLowerPresent | kUpperPresent)) != 0U) {
      predicate = cseg::EventTimePredicate{
          .lower = (*flags & kLowerPresent) != 0U
                       ? std::optional<cseg::EventTimeBound>{cseg::EventTimeBound{
                             .value = *lower, .inclusive = (*flags & kLowerInclusive) != 0U}}
                       : std::nullopt,
          .upper = (*flags & kUpperPresent) != 0U
                       ? std::optional<cseg::EventTimeBound>{cseg::EventTimeBound{
                             .value = *upper, .inclusive = (*flags & kUpperInclusive) != 0U}}
                       : std::nullopt};
    }
    DistributedAggregateFragment fragment{
        .query_id = common::Uuid{query_id_bytes},
        .database_id = *database_id,
        .table_id = *table_id,
        .tablet_id = *tablet_id,
        .destination_schema_id = *schema_id,
        .snapshot_generation = *snapshot_generation,
        .serving_node = *serving_node,
        .applied_position = *applied_position,
        .observed_leader_commit_position = *observed_commit,
        .placement_epoch = *placement_epoch,
        .read_policy = {.consistency = decoded_consistency,
                        .maximum_staleness_positions =
                            (*flags & kMaximumStalenessPresent) != 0U
                                ? std::optional<std::uint64_t>{*maximum_staleness}
                                : std::nullopt},
        .linearizable_barrier = (*flags & kBarrierPresent) != 0U
                                    ? std::optional<raft::ReadBarrier>{raft::ReadBarrier{
                                          *barrier_term, *barrier_context, *barrier_index}}
                                    : std::nullopt,
        .destination_column_ordinals = std::move(projection),
        .aggregate_input_index = *aggregate_input_index,
        .event_time_predicate = predicate};
    const common::Status validation = validate_fragment(fragment);
    if (!validation.is_ok())
      return common::make_unexpected(corruption("distributed fragment semantics are invalid"));
    return fragment;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed fragment decode allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed fragment decode exceeds limits"});
  }
}

EncodedDistributedGroupedFloat64Fragment::EncodedDistributedGroupedFloat64Fragment(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedGroupedFloat64Fragment::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedGroupedFloat64Fragment>
encode_distributed_grouped_float64_fragment(const DistributedGroupedFloat64Fragment& fragment) {
  if (fragment.group_key_input_index >= fragment.aggregate.destination_column_ordinals.size()) {
    return common::make_unexpected(
        invalid("distributed grouped fragment key input index is invalid"));
  }
  auto inner = encode_distributed_aggregate_fragment(fragment.aggregate);
  if (!inner.has_value())
    return common::make_unexpected(inner.error());

  try {
    const std::size_t frame_length = distributed_grouped_float64_fragment_format::kHeaderLength +
                                     inner->bytes().size() +
                                     distributed_grouped_float64_fragment_format::kTrailerLength;
    std::vector<std::byte> bytes(frame_length);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kGroupedMagic);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_grouped_float64_fragment_format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_grouped_float64_fragment_format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(distributed_grouped_float64_fragment_format::kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(frame_length);
    if (status.is_ok())
      status = writer.write_u32_le(fragment.group_key_input_index);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(inner->bytes().size()));
    if (status.is_ok())
      status = writer.zero_fill(4U);
    if (!status.is_ok() || writer.offset() != kGroupedHeaderCrcOffset) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal,
                         "distributed grouped fragment header layout is inconsistent"});
    }
    status =
        writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kGroupedHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.write_exact(inner->bytes());
    if (status.is_ok())
      status =
          writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!status.is_ok() || !writer.full()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal,
                         "distributed grouped fragment frame layout is inconsistent"});
    }
    return EncodedDistributedGroupedFloat64Fragment{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted, "distributed grouped fragment allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed grouped fragment exceeds container limits"});
  }
}

common::Result<DistributedGroupedFloat64Fragment>
decode_distributed_grouped_float64_fragment_exact(const common::ByteView bytes,
                                                  const DistributedFragmentDecodeLimits limits) {
  constexpr std::size_t kMinimumInnerLength = distributed_fragment_format::kHeaderLength +
                                              sizeof(std::uint32_t) +
                                              distributed_fragment_format::kTrailerLength;
  constexpr std::size_t kMinimumFrameLength =
      distributed_grouped_float64_fragment_format::kHeaderLength + kMinimumInnerLength +
      distributed_grouped_float64_fragment_format::kTrailerLength;
  if (bytes.size() < kMinimumFrameLength ||
      bytes.size() > distributed_grouped_float64_fragment_format::kMaximumFrameLength) {
    return common::make_unexpected(
        corruption("distributed grouped fragment frame length is invalid"));
  }
  if (!std::ranges::equal(bytes.first(kGroupedMagic.size()), kGroupedMagic)) {
    return common::make_unexpected(corruption("distributed grouped fragment magic is invalid"));
  }
  common::ByteReader header_crc_reader{
      bytes.subspan(kGroupedHeaderCrcOffset, sizeof(std::uint32_t))};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kGroupedHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("distributed grouped fragment header checksum is invalid"));
  }

  common::ByteReader reader{bytes};
  if (!reader.skip(kGroupedMagic.size()).is_ok()) {
    return common::make_unexpected(corruption("distributed grouped fragment header is truncated"));
  }
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto group_key_input_index = reader.read_u32_le();
  const auto inner_length = reader.read_u32_le();
  const auto reserved = reader.read_exact(4U);
  const auto header_crc = reader.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !group_key_input_index.has_value() ||
      !inner_length.has_value() || !reserved.has_value() || !header_crc.has_value()) {
    return common::make_unexpected(corruption("distributed grouped fragment header is truncated"));
  }
  if (*major != distributed_grouped_float64_fragment_format::kMajor ||
      *minor != distributed_grouped_float64_fragment_format::kMinor) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotSupported, "distributed grouped fragment version is unsupported"});
  }
  if (*header_length != distributed_grouped_float64_fragment_format::kHeaderLength ||
      std::ranges::any_of(*reserved,
                          [](const std::byte value) { return value != std::byte{0U}; })) {
    return common::make_unexpected(
        corruption("distributed grouped fragment header fields are invalid"));
  }
  if (*inner_length < kMinimumInnerLength ||
      *inner_length > distributed_fragment_format::kMaximumFrameLength ||
      *frame_length != distributed_grouped_float64_fragment_format::kHeaderLength +
                           static_cast<std::size_t>(*inner_length) +
                           distributed_grouped_float64_fragment_format::kTrailerLength ||
      bytes.size() != *frame_length) {
    return common::make_unexpected(
        corruption("distributed grouped fragment exact length is invalid"));
  }
  common::ByteReader trailer_reader{bytes.last(sizeof(std::uint32_t))};
  const auto stored_frame_crc = trailer_reader.read_u32_le();
  if (!stored_frame_crc.has_value() ||
      *stored_frame_crc != common::crc32c(bytes.first(bytes.size() - sizeof(std::uint32_t)))) {
    return common::make_unexpected(
        corruption("distributed grouped fragment frame checksum is invalid"));
  }
  const common::ByteView inner_bytes =
      bytes.subspan(distributed_grouped_float64_fragment_format::kHeaderLength, *inner_length);
  auto aggregate = decode_distributed_aggregate_fragment_exact(inner_bytes, limits);
  if (!aggregate.has_value())
    return common::make_unexpected(aggregate.error());
  if (*group_key_input_index >= aggregate->destination_column_ordinals.size()) {
    return common::make_unexpected(
        corruption("distributed grouped fragment key input index is invalid"));
  }
  return DistributedGroupedFloat64Fragment{.aggregate = std::move(*aggregate),
                                           .group_key_input_index = *group_key_input_index};
}

} // namespace chronos::query
