#include "chronos/query/distributed_vector_fragment.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <bitset>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'F'}, std::byte{'D'},
                                                  std::byte{'P'}, std::byte{'1'}};
inline constexpr std::uint32_t kLowerPresent = 1U << 0U;
inline constexpr std::uint32_t kLowerInclusive = 1U << 1U;
inline constexpr std::uint32_t kUpperPresent = 1U << 2U;
inline constexpr std::uint32_t kUpperInclusive = 1U << 3U;
inline constexpr std::uint32_t kMaximumStalenessPresent = 1U << 4U;
inline constexpr std::uint32_t kBarrierPresent = 1U << 5U;
inline constexpr std::uint32_t kKnownFlags = kLowerPresent | kLowerInclusive | kUpperPresent |
                                             kUpperInclusive | kMaximumStalenessPresent |
                                             kBarrierPresent;
inline constexpr std::size_t kHeaderCrcOffset = 228U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status validate_dispatch(const DistributedVectorFragmentDispatch& dispatch) {
  if (dispatch.query_id.is_nil() || dispatch.database_id.uuid().is_nil() ||
      dispatch.table_id.uuid().is_nil() || dispatch.tablet_id.uuid().is_nil() ||
      dispatch.destination_schema_id.uuid().is_nil() || dispatch.raft_group_id.is_nil() ||
      dispatch.snapshot_generation == 0U || dispatch.serving_node == 0U ||
      dispatch.placement_epoch == 0U || dispatch.destination_column_ordinals.empty() ||
      dispatch.destination_column_ordinals.size() >
          distributed_vector_fragment_format::kMaximumProjectionColumns) {
    return invalid("distributed vector fragment identity, route, or projection is invalid");
  }
  std::bitset<distributed_vector_fragment_format::kMaximumProjectionColumns> seen;
  for (const std::uint32_t ordinal : dispatch.destination_column_ordinals) {
    if (ordinal >= distributed_vector_fragment_format::kMaximumProjectionColumns || seen[ordinal])
      return invalid("distributed vector fragment projection is invalid or duplicated");
    seen.set(ordinal);
  }
  if (dispatch.event_time_predicate.has_value() &&
      !dispatch.event_time_predicate->lower.has_value() &&
      !dispatch.event_time_predicate->upper.has_value()) {
    return invalid("distributed vector fragment event predicate is empty");
  }
  switch (dispatch.read_policy.consistency) {
  case DistributedReadConsistency::kLeaderLinearizable:
    if (dispatch.read_policy.maximum_staleness_positions.has_value() ||
        !dispatch.linearizable_barrier.has_value() || dispatch.linearizable_barrier->term == 0U ||
        dispatch.linearizable_barrier->context == 0U ||
        dispatch.linearizable_barrier->read_index == 0U ||
        dispatch.applied_position < dispatch.linearizable_barrier->read_index) {
      return invalid("distributed vector fragment linearizable proof is invalid");
    }
    break;
  case DistributedReadConsistency::kFollowerBoundedStale: {
    if (!dispatch.read_policy.maximum_staleness_positions.has_value() ||
        dispatch.linearizable_barrier.has_value() ||
        dispatch.observed_leader_commit_position == 0U) {
      return invalid("distributed vector fragment bounded-stale proof is invalid");
    }
    const std::uint64_t lag =
        dispatch.observed_leader_commit_position > dispatch.applied_position
            ? dispatch.observed_leader_commit_position - dispatch.applied_position
            : 0U;
    if (lag > *dispatch.read_policy.maximum_staleness_positions)
      return invalid("distributed vector fragment bounded-stale lag exceeds its proof");
    break;
  }
  case DistributedReadConsistency::kLocalEventual:
    if (dispatch.read_policy.maximum_staleness_positions.has_value() ||
        dispatch.linearizable_barrier.has_value()) {
      return invalid("distributed vector fragment eventual proof is invalid");
    }
    break;
  default:
    return invalid("distributed vector fragment consistency is invalid");
  }
  return validate_distributed_vector_plan_intent(
      dispatch.plan, static_cast<std::uint32_t>(dispatch.destination_column_ordinals.size()));
}

} // namespace

EncodedDistributedVectorFragmentDispatch::EncodedDistributedVectorFragmentDispatch(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedVectorFragmentDispatch::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedVectorFragmentDispatch>
encode_distributed_vector_fragment_dispatch(const DistributedVectorFragmentDispatch& dispatch) {
  const common::Status validation = validate_dispatch(dispatch);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  auto encoded_plan = encode_distributed_vector_plan_intent(dispatch.plan);
  if (!encoded_plan.has_value())
    return common::make_unexpected(encoded_plan.error());
  const std::size_t frame_length =
      distributed_vector_fragment_format::kHeaderLength +
      dispatch.destination_column_ordinals.size() * sizeof(std::uint32_t) +
      encoded_plan->bytes().size() + distributed_vector_fragment_format::kTrailerLength;
  try {
    std::vector<std::byte> bytes(frame_length);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_fragment_format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_fragment_format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(distributed_vector_fragment_format::kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(frame_length);
    if (status.is_ok())
      status = writer.write_exact(dispatch.query_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(dispatch.database_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(dispatch.table_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(dispatch.tablet_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(dispatch.destination_schema_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(dispatch.raft_group_id.bytes());
    if (status.is_ok())
      status = writer.write_u64_le(dispatch.snapshot_generation);
    if (status.is_ok())
      status = writer.write_u64_le(dispatch.serving_node);
    if (status.is_ok())
      status = writer.write_u64_le(dispatch.applied_position);
    if (status.is_ok())
      status = writer.write_u64_le(dispatch.observed_leader_commit_position);
    if (status.is_ok())
      status = writer.write_u64_le(dispatch.placement_epoch);
    if (status.is_ok())
      status = writer.write_u64_le(dispatch.read_policy.maximum_staleness_positions.value_or(0U));
    if (status.is_ok())
      status = writer.write_u64_le(
          dispatch.linearizable_barrier.has_value() ? dispatch.linearizable_barrier->term : 0U);
    if (status.is_ok())
      status = writer.write_u64_le(
          dispatch.linearizable_barrier.has_value() ? dispatch.linearizable_barrier->context : 0U);
    if (status.is_ok())
      status = writer.write_u64_le(dispatch.linearizable_barrier.has_value()
                                       ? dispatch.linearizable_barrier->read_index
                                       : 0U);
    if (status.is_ok())
      status = writer.write_u32_le(
          static_cast<std::uint32_t>(dispatch.destination_column_ordinals.size()));
    std::uint32_t flags{};
    std::int64_t lower{};
    std::int64_t upper{};
    if (dispatch.event_time_predicate.has_value() &&
        dispatch.event_time_predicate->lower.has_value()) {
      flags |= kLowerPresent;
      lower = dispatch.event_time_predicate->lower->value;
      if (dispatch.event_time_predicate->lower->inclusive)
        flags |= kLowerInclusive;
    }
    if (dispatch.event_time_predicate.has_value() &&
        dispatch.event_time_predicate->upper.has_value()) {
      flags |= kUpperPresent;
      upper = dispatch.event_time_predicate->upper->value;
      if (dispatch.event_time_predicate->upper->inclusive)
        flags |= kUpperInclusive;
    }
    if (dispatch.read_policy.maximum_staleness_positions.has_value())
      flags |= kMaximumStalenessPresent;
    if (dispatch.linearizable_barrier.has_value())
      flags |= kBarrierPresent;
    if (status.is_ok())
      status = writer.write_u32_le(flags);
    if (status.is_ok())
      status = writer.write_u8(static_cast<std::uint8_t>(dispatch.read_policy.consistency));
    if (status.is_ok())
      status = writer.zero_fill(3U);
    if (status.is_ok())
      status = writer.write_i64_le(lower);
    if (status.is_ok())
      status = writer.write_i64_le(upper);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(encoded_plan->bytes().size()));
    if (status.is_ok())
      status = writer.zero_fill(4U);
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    for (const std::uint32_t ordinal : dispatch.destination_column_ordinals) {
      if (status.is_ok())
        status = writer.write_u32_le(ordinal);
    }
    if (status.is_ok())
      status = writer.write_exact(encoded_plan->bytes());
    if (status.is_ok())
      status =
          writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!status.is_ok() || !writer.full()) {
      return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                    "distributed vector fragment layout failed"});
    }
    return EncodedDistributedVectorFragmentDispatch{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed vector fragment allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector fragment exceeds container limits"});
  }
}

common::Result<DistributedVectorFragmentDispatch> decode_distributed_vector_fragment_dispatch_exact(
    const common::ByteView bytes, const DistributedVectorFragmentDecodeLimits limits) {
  if (limits.maximum_projection_columns == 0U ||
      limits.maximum_projection_columns >
          distributed_vector_fragment_format::kMaximumProjectionColumns ||
      limits.plan.maximum_input_columns == 0U ||
      limits.plan.maximum_input_columns > distributed_vector_plan_format::kMaximumInputColumns ||
      limits.plan.maximum_output_columns == 0U ||
      limits.plan.maximum_output_columns > distributed_vector_plan_format::kMaximumOutputColumns ||
      limits.plan.maximum_row_outputs == 0U ||
      limits.plan.maximum_row_outputs > distributed_vector_plan_format::kMaximumRowOutputs ||
      limits.plan.maximum_group_keys == 0U ||
      limits.plan.maximum_group_keys > distributed_vector_plan_format::kMaximumGroupKeys ||
      limits.plan.maximum_aggregates == 0U ||
      limits.plan.maximum_aggregates > distributed_vector_plan_format::kMaximumAggregates ||
      limits.plan.maximum_order_keys > distributed_vector_plan_format::kMaximumOrderKeys) {
    return common::make_unexpected(invalid("distributed vector fragment limits are invalid"));
  }
  if (bytes.size() < distributed_vector_fragment_format::kHeaderLength +
                         distributed_vector_plan_format::kHeaderLength +
                         distributed_vector_plan_format::kTrailerLength +
                         distributed_vector_fragment_format::kTrailerLength ||
      bytes.size() > distributed_vector_fragment_format::kMaximumFrameLength)
    return common::make_unexpected(corruption("distributed vector fragment length is invalid"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("distributed vector fragment magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto header_crc = header_crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(bytes.first(kHeaderCrcOffset)))
    return common::make_unexpected(
        corruption("distributed vector fragment header checksum is invalid"));

  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  std::array<common::Uuid::Bytes, 6U> identities{};
  bool identities_complete = true;
  for (auto& identity : identities) {
    const auto value = reader.read_exact(common::Uuid::kSize);
    if (!value.has_value()) {
      identities_complete = false;
      break;
    }
    std::ranges::copy(*value, identity.begin());
  }
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
  const auto flags = reader.read_u32_le();
  const auto consistency = reader.read_u8();
  const auto small_reserved = reader.read_exact(3U);
  const auto lower = reader.read_i64_le();
  const auto upper = reader.read_i64_le();
  const auto plan_length = reader.read_u32_le();
  const auto reserved = reader.read_u32_le();
  static_cast<void>(reader.skip(4U));
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !identities_complete || !snapshot_generation.has_value() ||
      !serving_node.has_value() || !applied_position.has_value() || !observed_commit.has_value() ||
      !placement_epoch.has_value() || !maximum_staleness.has_value() || !barrier_term.has_value() ||
      !barrier_context.has_value() || !barrier_index.has_value() || !projection_count.has_value() ||
      !flags.has_value() || !consistency.has_value() || !small_reserved.has_value() ||
      !lower.has_value() || !upper.has_value() || !plan_length.has_value() ||
      !reserved.has_value()) {
    return common::make_unexpected(corruption("distributed vector fragment header is truncated"));
  }
  if (*major != distributed_vector_fragment_format::kMajor ||
      *minor != distributed_vector_fragment_format::kMinor) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotSupported, "distributed vector fragment version is unsupported"});
  }
  if (*header_length != distributed_vector_fragment_format::kHeaderLength ||
      *frame_length != bytes.size() || (*flags & ~kKnownFlags) != 0U || *reserved != 0U ||
      std::ranges::any_of(*small_reserved,
                          [](const std::byte value) { return value != std::byte{}; }) ||
      *projection_count == 0U ||
      *projection_count > distributed_vector_fragment_format::kMaximumProjectionColumns ||
      *plan_length < distributed_vector_plan_format::kHeaderLength +
                         distributed_vector_plan_format::kTrailerLength ||
      *plan_length > distributed_vector_plan_format::kMaximumFrameLength) {
    return common::make_unexpected(
        corruption("distributed vector fragment header is noncanonical"));
  }
  if (*projection_count > limits.maximum_projection_columns) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector fragment projection exceeds caller limit"});
  }
  const std::size_t expected_length = distributed_vector_fragment_format::kHeaderLength +
                                      static_cast<std::size_t>(*projection_count) * 4U +
                                      *plan_length +
                                      distributed_vector_fragment_format::kTrailerLength;
  if (expected_length != bytes.size())
    return common::make_unexpected(
        corruption("distributed vector fragment encoded length is invalid"));
  common::ByteReader trailer{bytes.last(4U)};
  const auto frame_crc = trailer.read_u32_le();
  if (!frame_crc.has_value() || *frame_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("distributed vector fragment checksum is invalid"));
  if (((*flags & kLowerInclusive) != 0U && (*flags & kLowerPresent) == 0U) ||
      ((*flags & kUpperInclusive) != 0U && (*flags & kUpperPresent) == 0U) ||
      ((*flags & kLowerPresent) == 0U && *lower != 0) ||
      ((*flags & kUpperPresent) == 0U && *upper != 0) ||
      ((*flags & kMaximumStalenessPresent) == 0U && *maximum_staleness != 0U) ||
      ((*flags & kBarrierPresent) == 0U &&
       (*barrier_term != 0U || *barrier_context != 0U || *barrier_index != 0U))) {
    return common::make_unexpected(
        corruption("distributed vector fragment optional fields are noncanonical"));
  }

  const auto database_id = manifest::DatabaseId::from_bytes(identities[1U]);
  const auto table_id = schema::TableId::from_bytes(identities[2U]);
  const auto tablet_id = schema::TabletId::from_bytes(identities[3U]);
  const auto schema_id = schema::SchemaId::from_bytes(identities[4U]);
  if (common::Uuid{identities[0U]}.is_nil() || !database_id.has_value() || !table_id.has_value() ||
      !tablet_id.has_value() || !schema_id.has_value() || common::Uuid{identities[5U]}.is_nil()) {
    return common::make_unexpected(corruption("distributed vector fragment identity is invalid"));
  }
  DistributedReadConsistency decoded_consistency;
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
    return common::make_unexpected(
        corruption("distributed vector fragment consistency is invalid"));
  }
  try {
    std::vector<std::uint32_t> projection;
    projection.reserve(*projection_count);
    for (std::uint32_t index = 0U; index < *projection_count; ++index) {
      const auto ordinal = reader.read_u32_le();
      if (!ordinal.has_value())
        return common::make_unexpected(
            corruption("distributed vector fragment projection is truncated"));
      projection.push_back(*ordinal);
    }
    const auto plan = decode_distributed_vector_plan_intent_exact(
        bytes.subspan(reader.offset(), *plan_length), limits.plan);
    if (!plan.has_value())
      return common::make_unexpected(plan.error());
    static_cast<void>(reader.skip(*plan_length));
    if (reader.remaining() != distributed_vector_fragment_format::kTrailerLength)
      return common::make_unexpected(
          corruption("distributed vector fragment body layout is invalid"));
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
    DistributedVectorFragmentDispatch dispatch{
        .query_id = common::Uuid{identities[0U]},
        .database_id = *database_id,
        .table_id = *table_id,
        .tablet_id = *tablet_id,
        .destination_schema_id = *schema_id,
        .raft_group_id = common::Uuid{identities[5U]},
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
        .event_time_predicate = std::move(predicate),
        .plan = std::move(*plan)};
    const common::Status validation = validate_dispatch(dispatch);
    if (!validation.is_ok())
      return common::make_unexpected(
          corruption("distributed vector fragment semantics are invalid"));
    return dispatch;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector fragment decode allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector fragment decode exceeds container limits"});
  }
}

} // namespace chronos::query
