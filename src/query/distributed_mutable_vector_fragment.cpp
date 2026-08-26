#include "chronos/query/distributed_mutable_vector_fragment.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_vector_aggregate_exchange.hpp"

#include <algorithm>
#include <array>
#include <bitset>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'M'}, std::byte{'V'}, std::byte{'F'},
                                                  std::byte{'R'}, std::byte{'1'}};
inline constexpr std::uint32_t kLowerPresent = 1U << 0U;
inline constexpr std::uint32_t kLowerInclusive = 1U << 1U;
inline constexpr std::uint32_t kUpperPresent = 1U << 2U;
inline constexpr std::uint32_t kUpperInclusive = 1U << 3U;
inline constexpr std::uint32_t kMaximumStalenessPresent = 1U << 4U;
inline constexpr std::uint32_t kBarrierPresent = 1U << 5U;
inline constexpr std::uint32_t kKnownFlags = kLowerPresent | kLowerInclusive | kUpperPresent |
                                             kUpperInclusive | kMaximumStalenessPresent |
                                             kBarrierPresent;
inline constexpr std::size_t kHeaderCrcOffset = 240U;
inline constexpr std::size_t kMinimumFrameLength =
    distributed_mutable_vector_fragment_format::kHeaderLength + sizeof(std::uint32_t) +
    distributed_vector_plan_format::kHeaderLength + distributed_vector_plan_format::kTrailerLength +
    distributed_vector_result_schema_format::kHeaderLength +
    distributed_vector_result_schema_format::kDescriptorFixedLength + 1U +
    distributed_vector_result_schema_format::kTrailerLength +
    distributed_mutable_vector_fragment_format::kTrailerLength;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status not_supported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] common::Result<std::vector<PhysicalColumnShape>>
pre_group_shapes(const DistributedVectorPreGroupProgram& program) {
  const common::Status status = validate_distributed_vector_pre_group_program(program);
  if (!status.is_ok())
    return common::make_unexpected(status);
  try {
    std::vector<PhysicalColumnShape> shapes;
    shapes.reserve(program.outputs.size());
    for (const VectorExpression& expression : program.outputs)
      shapes.push_back({expression.result_shape().type, expression.result_shape().nullable});
    return shapes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable pre-group shape allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable pre-group shape exceeds limits"));
  }
}

[[nodiscard]] common::Status
validate_pre_group_sources(const DistributedVectorPreGroupProgram& program,
                           const schema::TableSchema& schema_value) {
  for (const VectorExpression& expression : program.outputs) {
    for (const VectorExpressionInstruction& instruction : expression.instructions()) {
      const auto* input = std::get_if<VectorInputExpression>(&instruction);
      if (input == nullptr)
        continue;
      if (input->input_column_ordinal >= schema_value.columns().size())
        return invalid("mutable pre-group source ordinal is out of bounds");
      const schema::ColumnDefinition& column = schema_value.columns()[input->input_column_ordinal];
      if (input->type != column.type() || input->nullable != column.nullable())
        return invalid("mutable pre-group source shape differs from destination schema");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_placement(const raft::TabletPlacementMetadata& placement,
                                                const schema::TableId& table_id,
                                                const schema::TabletId& tablet_id,
                                                const DistributedReadAdmission& admission,
                                                const DistributedReadConsistency) {
  if (placement.table_id != table_id || placement.tablet_id != tablet_id ||
      placement.placement_epoch == 0U || placement.replicas.empty() ||
      placement.replicas.size() > raft::MetadataLimits{}.maximum_replicas_per_tablet ||
      placement.replicas.front() == 0U || !std::ranges::is_sorted(placement.replicas) ||
      std::ranges::adjacent_find(placement.replicas) != placement.replicas.end() ||
      !std::ranges::binary_search(placement.replicas, admission.serving_node)) {
    return unavailable("mutable vector fragment placement does not authorize the serving replica");
  }
  if (placement.leader_hint.has_value() &&
      !std::ranges::binary_search(placement.replicas, *placement.leader_hint)) {
    return invalid("mutable vector fragment placement leader is not a replica");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<std::vector<PhysicalColumnShape>>
projected_shapes(const schema::TableSchema& schema_value,
                 const std::span<const std::uint32_t> ordinals) {
  try {
    std::vector<PhysicalColumnShape> projected;
    projected.reserve(ordinals.size());
    for (const std::uint32_t ordinal : ordinals) {
      if (ordinal >= schema_value.columns().size())
        return common::make_unexpected(invalid("mutable vector projection is out of bounds"));
      const schema::ColumnDefinition& column = schema_value.columns()[ordinal];
      projected.push_back({column.type(), column.nullable()});
    }
    return projected;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector projection allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable vector projection exceeds limits"));
  }
}

} // namespace

EncodedDistributedMutableVectorFragment::EncodedDistributedMutableVectorFragment(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedMutableVectorFragment::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedMutableVectorFragment>
encode_distributed_mutable_vector_fragment(const DistributedMutableVectorFragment& fragment) {
  const common::Status validation = validate_distributed_mutable_vector_fragment(fragment);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  if (fragment.pre_group_program.has_value())
    return common::make_unexpected(
        not_supported("mutable pre-group program transport is not implemented"));
  auto plan = encode_distributed_vector_plan_intent(fragment.plan);
  if (!plan.has_value())
    return common::make_unexpected(plan.error());
  auto result_schema = encode_distributed_vector_result_schema(fragment.result_schema);
  if (!result_schema.has_value())
    return common::make_unexpected(result_schema.error());
  const std::size_t frame_length = distributed_mutable_vector_fragment_format::kHeaderLength +
                                   fragment.destination_column_ordinals.size() * 4U +
                                   plan->bytes().size() + result_schema->bytes().size() +
                                   distributed_mutable_vector_fragment_format::kTrailerLength;
  try {
    std::vector<std::byte> bytes(frame_length);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_mutable_vector_fragment_format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_mutable_vector_fragment_format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(distributed_mutable_vector_fragment_format::kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(frame_length);
    if (status.is_ok())
      status = writer.write_u64_le(plan->bytes().size());
    if (status.is_ok())
      status = writer.write_u64_le(result_schema->bytes().size());
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
      status = writer.write_exact(fragment.raft_group_id.bytes());
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
    std::uint32_t flags{};
    std::int64_t lower{};
    std::int64_t upper{};
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
      status = writer.zero_fill(7U);
    if (status.is_ok())
      status = writer.write_i64_le(lower);
    if (status.is_ok())
      status = writer.write_i64_le(upper);
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(plan->bytes()));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(result_schema->bytes()));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.zero_fill(4U);
    for (const std::uint32_t ordinal : fragment.destination_column_ordinals) {
      if (status.is_ok())
        status = writer.write_u32_le(ordinal);
    }
    if (status.is_ok())
      status = writer.write_exact(plan->bytes());
    if (status.is_ok())
      status = writer.write_exact(result_schema->bytes());
    if (status.is_ok())
      status =
          writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!status.is_ok() || !writer.full()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "mutable vector fragment layout failed"});
    }
    return EncodedDistributedMutableVectorFragment{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector fragment encoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable vector fragment encoding exceeds limits"));
  }
}

common::Result<DistributedMutableVectorFragment> decode_distributed_mutable_vector_fragment_exact(
    const common::ByteView bytes, const DistributedMutableVectorFragmentDecodeLimits limits) {
  if (limits.maximum_frame_length < kMinimumFrameLength ||
      limits.maximum_frame_length >
          distributed_mutable_vector_fragment_format::kMaximumFrameLength ||
      limits.maximum_projection_columns == 0U ||
      limits.maximum_projection_columns >
          distributed_mutable_vector_fragment_format::kMaximumProjectionColumns) {
    return common::make_unexpected(invalid("mutable vector fragment decode limits are invalid"));
  }
  if (bytes.size() < kMinimumFrameLength ||
      bytes.size() > distributed_mutable_vector_fragment_format::kMaximumFrameLength)
    return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                  "mutable vector fragment length is invalid"});
  if (bytes.size() > limits.maximum_frame_length)
    return common::make_unexpected(exhausted("mutable vector fragment exceeds caller limit"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                  "mutable vector fragment magic is invalid"});
  common::ByteReader crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto header_crc = crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(bytes.first(kHeaderCrcOffset))) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "mutable vector fragment header checksum is invalid"});
  }

  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto plan_length = reader.read_u64_le();
  const auto schema_length = reader.read_u64_le();
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
  const auto small_reserved = reader.read_exact(7U);
  const auto lower = reader.read_i64_le();
  const auto upper = reader.read_i64_le();
  const auto plan_crc = reader.read_u32_le();
  const auto schema_crc = reader.read_u32_le();
  static_cast<void>(reader.skip(4U));
  const auto reserved = reader.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !plan_length.has_value() || !schema_length.has_value() ||
      !identities_complete || !serving_node.has_value() || !applied_position.has_value() ||
      !observed_commit.has_value() || !placement_epoch.has_value() ||
      !maximum_staleness.has_value() || !barrier_term.has_value() || !barrier_context.has_value() ||
      !barrier_index.has_value() || !projection_count.has_value() || !flags.has_value() ||
      !consistency.has_value() || !small_reserved.has_value() || !lower.has_value() ||
      !upper.has_value() || !plan_crc.has_value() || !schema_crc.has_value() ||
      !reserved.has_value()) {
    return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                  "mutable vector fragment header is truncated"});
  }
  if (*major != distributed_mutable_vector_fragment_format::kMajor ||
      *minor != distributed_mutable_vector_fragment_format::kMinor) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotSupported, "mutable vector fragment version is unsupported"});
  }
  if (*header_length != distributed_mutable_vector_fragment_format::kHeaderLength ||
      *frame_length != bytes.size() || (*flags & ~kKnownFlags) != 0U || *reserved != 0U ||
      std::ranges::any_of(*small_reserved,
                          [](const std::byte value) { return value != std::byte{}; }) ||
      *projection_count == 0U ||
      *projection_count > distributed_mutable_vector_fragment_format::kMaximumProjectionColumns ||
      *plan_length < distributed_vector_plan_format::kHeaderLength +
                         distributed_vector_plan_format::kTrailerLength ||
      *plan_length > distributed_vector_plan_format::kMaximumFrameLength ||
      *schema_length < distributed_vector_result_schema_format::kHeaderLength +
                           distributed_vector_result_schema_format::kDescriptorFixedLength + 1U +
                           distributed_vector_result_schema_format::kTrailerLength ||
      *schema_length > distributed_vector_result_schema_format::kMaximumFrameLength) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "mutable vector fragment header is noncanonical"});
  }
  if (*projection_count > limits.maximum_projection_columns)
    return common::make_unexpected(exhausted("mutable vector projection exceeds caller limit"));
  const std::size_t expected_length = distributed_mutable_vector_fragment_format::kHeaderLength +
                                      static_cast<std::size_t>(*projection_count) * 4U +
                                      static_cast<std::size_t>(*plan_length) +
                                      static_cast<std::size_t>(*schema_length) +
                                      distributed_mutable_vector_fragment_format::kTrailerLength;
  if (expected_length != bytes.size())
    return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                  "mutable vector encoded length is invalid"});
  common::ByteReader trailer{bytes.last(4U)};
  const auto frame_crc = trailer.read_u32_le();
  if (!frame_crc.has_value() || *frame_crc != common::crc32c(bytes.first(bytes.size() - 4U))) {
    return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                  "mutable vector fragment checksum is invalid"});
  }
  if (((*flags & kLowerInclusive) != 0U && (*flags & kLowerPresent) == 0U) ||
      ((*flags & kUpperInclusive) != 0U && (*flags & kUpperPresent) == 0U) ||
      ((*flags & kLowerPresent) == 0U && *lower != 0) ||
      ((*flags & kUpperPresent) == 0U && *upper != 0) ||
      ((*flags & kMaximumStalenessPresent) == 0U && *maximum_staleness != 0U) ||
      ((*flags & kBarrierPresent) == 0U &&
       (*barrier_term != 0U || *barrier_context != 0U || *barrier_index != 0U))) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "mutable vector optional fields are noncanonical"});
  }

  const auto database_id = manifest::DatabaseId::from_bytes(identities[1U]);
  const auto table_id = schema::TableId::from_bytes(identities[2U]);
  const auto tablet_id = schema::TabletId::from_bytes(identities[3U]);
  const auto schema_id = schema::SchemaId::from_bytes(identities[4U]);
  if (common::Uuid{identities[0U]}.is_nil() || !database_id.has_value() || !table_id.has_value() ||
      !tablet_id.has_value() || !schema_id.has_value() || common::Uuid{identities[5U]}.is_nil()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "mutable vector identity is invalid"});
  }
  DistributedReadConsistency decoded_consistency{};
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
        common::Status{common::StatusCode::kCorruption, "mutable vector consistency is invalid"});
  }

  try {
    std::vector<std::uint32_t> projection;
    projection.reserve(*projection_count);
    for (std::uint32_t index = 0U; index < *projection_count; ++index) {
      const auto ordinal = reader.read_u32_le();
      if (!ordinal.has_value())
        return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                      "mutable vector projection is truncated"});
      projection.push_back(*ordinal);
    }
    const auto plan_bytes = reader.read_exact(static_cast<std::size_t>(*plan_length));
    const auto schema_bytes = reader.read_exact(static_cast<std::size_t>(*schema_length));
    if (!plan_bytes.has_value() || !schema_bytes.has_value() ||
        reader.remaining() != distributed_mutable_vector_fragment_format::kTrailerLength ||
        common::crc32c(*plan_bytes) != *plan_crc || common::crc32c(*schema_bytes) != *schema_crc) {
      return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                    "mutable vector nested payload is invalid"});
    }
    auto plan = decode_distributed_vector_plan_intent_exact(*plan_bytes, limits.plan);
    if (!plan.has_value())
      return common::make_unexpected(plan.error());
    auto result_schema =
        decode_distributed_vector_result_schema_exact(*schema_bytes, limits.result_schema);
    if (!result_schema.has_value())
      return common::make_unexpected(result_schema.error());
    std::optional<cseg::EventTimePredicate> predicate;
    if ((*flags & (kLowerPresent | kUpperPresent)) != 0U) {
      predicate = cseg::EventTimePredicate{
          .lower = (*flags & kLowerPresent) != 0U
                       ? std::optional<cseg::EventTimeBound>{cseg::EventTimeBound{
                             *lower, (*flags & kLowerInclusive) != 0U}}
                       : std::nullopt,
          .upper = (*flags & kUpperPresent) != 0U
                       ? std::optional<cseg::EventTimeBound>{cseg::EventTimeBound{
                             *upper, (*flags & kUpperInclusive) != 0U}}
                       : std::nullopt};
    }
    DistributedMutableVectorFragment fragment{
        .query_id = common::Uuid{identities[0U]},
        .database_id = *database_id,
        .table_id = *table_id,
        .tablet_id = *tablet_id,
        .destination_schema_id = *schema_id,
        .raft_group_id = common::Uuid{identities[5U]},
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
        .event_time_predicate = predicate,
        .plan = std::move(*plan),
        .result_schema = std::move(*result_schema),
        .pre_group_program = std::nullopt};
    const common::Status validation = validate_distributed_mutable_vector_fragment(fragment);
    if (!validation.is_ok()) {
      if (validation.code() == common::StatusCode::kResourceExhausted)
        return common::make_unexpected(validation);
      return common::make_unexpected(common::Status{
          common::StatusCode::kCorruption, "mutable vector fragment semantics are invalid"});
    }
    return fragment;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector fragment decode allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable vector fragment decode exceeds limits"));
  }
}

common::Status
validate_distributed_mutable_vector_fragment(const DistributedMutableVectorFragment& fragment) {
  if (fragment.query_id.is_nil() || fragment.database_id.uuid().is_nil() ||
      fragment.table_id.uuid().is_nil() || fragment.tablet_id.uuid().is_nil() ||
      fragment.destination_schema_id.uuid().is_nil() || fragment.raft_group_id.is_nil() ||
      fragment.serving_node == 0U || fragment.applied_position == 0U ||
      fragment.placement_epoch == 0U || fragment.destination_column_ordinals.empty() ||
      fragment.destination_column_ordinals.size() > schema::kMaximumSchemaColumnCount) {
    return invalid("mutable vector fragment identity or shape is invalid");
  }
  switch (fragment.read_policy.consistency) {
  case DistributedReadConsistency::kLeaderLinearizable:
    if (fragment.read_policy.maximum_staleness_positions.has_value() ||
        !fragment.linearizable_barrier.has_value() || fragment.linearizable_barrier->term == 0U ||
        fragment.linearizable_barrier->context == 0U ||
        fragment.linearizable_barrier->read_index == 0U ||
        fragment.applied_position < fragment.linearizable_barrier->read_index) {
      return unavailable("mutable vector fragment lacks an applied linearizable barrier");
    }
    break;
  case DistributedReadConsistency::kFollowerBoundedStale: {
    if (!fragment.read_policy.maximum_staleness_positions.has_value() ||
        fragment.linearizable_barrier.has_value()) {
      return unavailable("mutable vector fragment lacks bounded-stale authority");
    }
    const std::uint64_t lag =
        fragment.observed_leader_commit_position > fragment.applied_position
            ? fragment.observed_leader_commit_position - fragment.applied_position
            : 0U;
    if (lag > *fragment.read_policy.maximum_staleness_positions)
      return unavailable("mutable vector fragment exceeds bounded-stale lag");
    break;
  }
  case DistributedReadConsistency::kLocalEventual:
    if (fragment.read_policy.maximum_staleness_positions.has_value() ||
        fragment.linearizable_barrier.has_value()) {
      return invalid("mutable vector local-eventual fragment carries stronger proof fields");
    }
    break;
  default:
    return invalid("mutable vector fragment consistency mode is invalid");
  }

  std::bitset<schema::kMaximumSchemaColumnCount> seen;
  for (const std::uint32_t ordinal : fragment.destination_column_ordinals) {
    if (ordinal >= schema::kMaximumSchemaColumnCount || seen[ordinal])
      return invalid("mutable vector fragment projection is invalid or duplicated");
    seen.set(ordinal);
  }
  if (fragment.event_time_predicate.has_value() &&
      !fragment.event_time_predicate->lower.has_value() &&
      !fragment.event_time_predicate->upper.has_value()) {
    return invalid("mutable vector fragment event-time predicate is empty");
  }
  std::vector<PhysicalColumnShape> program_shapes;
  std::uint32_t input_count =
      static_cast<std::uint32_t>(fragment.destination_column_ordinals.size());
  if (fragment.pre_group_program.has_value()) {
    if (fragment.plan.mode != DistributedVectorPlanMode::kGroupedAggregate)
      return invalid("mutable pre-group program requires a grouped aggregate plan");
    auto shapes = pre_group_shapes(*fragment.pre_group_program);
    if (!shapes.has_value())
      return shapes.error();
    program_shapes = std::move(*shapes);
    input_count = static_cast<std::uint32_t>(program_shapes.size());
  }
  common::Status plan_status = validate_distributed_vector_plan_intent(fragment.plan, input_count);
  if (!plan_status.is_ok())
    return plan_status;
  if (fragment.pre_group_program.has_value())
    return validate_distributed_vector_result_schema(fragment.plan, program_shapes,
                                                     fragment.result_schema);
  auto encoded_schema = encode_distributed_vector_result_schema(fragment.result_schema);
  return encoded_schema.has_value() ? common::Status::ok() : encoded_schema.error();
}

common::Result<DistributedMutableVectorFragment>
bind_distributed_mutable_vector_fragment(const DistributedMutableVectorFragmentBinding& binding) {
  const DistributedVectorQueryPlan& plan = binding.plan.get();
  const DistributedReadAdmission& admission = binding.admission.get();
  const ingest::TabletSnapshot& snapshot = binding.snapshot.get();
  const schema::SchemaLineage& lineage = binding.lineage.get();
  const raft::TabletPlacementMetadata& placement = binding.placement.get();
  const std::shared_ptr<const schema::TableSchema> destination = lineage.current();
  if (destination == nullptr || plan.query_id.is_nil() || plan.fragments.empty() ||
      binding.raft_group_id.is_nil()) {
    return common::make_unexpected(invalid("mutable vector fragment authority is invalid"));
  }
  const common::Status admission_status =
      validate_distributed_read_admission(plan.read_policy, plan.fragments, admission);
  if (!admission_status.is_ok())
    return common::make_unexpected(admission_status);
  const common::Status placement_status =
      validate_placement(placement, destination->table_id(), admission.tablet_id, admission,
                         plan.read_policy.consistency);
  if (!placement_status.is_ok())
    return common::make_unexpected(placement_status);
  const std::optional<head::HeadCommitPosition>& position = snapshot.applied_position();
  if (snapshot.table_id() != destination->table_id() ||
      snapshot.tablet_id() != admission.tablet_id ||
      snapshot.schema_ptr()->schema_id() != destination->schema_id() ||
      snapshot.schema_ptr()->version() != destination->version() ||
      lineage.table_id() != destination->table_id()) {
    return common::make_unexpected(unavailable("mutable vector snapshot schema identity differs"));
  }
  if (!position.has_value() || position->source != head::CommitSource::kRaft ||
      position->raft_group_id != binding.raft_group_id ||
      position->record_sequence != admission.applied_position) {
    return common::make_unexpected(
        unavailable("mutable vector snapshot differs from the admitted Raft boundary"));
  }

  std::vector<PhysicalColumnShape> program_shapes;
  if (binding.pre_group_program != nullptr) {
    const common::Status sources =
        validate_pre_group_sources(*binding.pre_group_program, *destination);
    if (!sources.is_ok())
      return common::make_unexpected(sources);
    auto shapes = pre_group_shapes(*binding.pre_group_program);
    if (!shapes.has_value())
      return common::make_unexpected(shapes.error());
    program_shapes = std::move(*shapes);
  }
  auto projected = binding.pre_group_program == nullptr
                       ? projected_shapes(*destination, binding.destination_column_ordinals)
                       : common::Result<std::vector<PhysicalColumnShape>>{program_shapes};
  if (!projected.has_value())
    return common::make_unexpected(projected.error());
  const common::Status result_status = validate_distributed_vector_result_schema(
      plan.intent, *projected, binding.result_schema.get());
  if (!result_status.is_ok())
    return common::make_unexpected(result_status);

  try {
    DistributedMutableVectorFragment fragment{
        .query_id = plan.query_id,
        .database_id = binding.database_id,
        .table_id = destination->table_id(),
        .tablet_id = snapshot.tablet_id(),
        .destination_schema_id = destination->schema_id(),
        .raft_group_id = binding.raft_group_id,
        .serving_node = admission.serving_node,
        .applied_position = admission.applied_position,
        .observed_leader_commit_position = admission.observed_leader_commit_position,
        .placement_epoch = placement.placement_epoch,
        .read_policy = plan.read_policy,
        .linearizable_barrier = admission.linearizable_barrier,
        .destination_column_ordinals = {binding.destination_column_ordinals.begin(),
                                        binding.destination_column_ordinals.end()},
        .event_time_predicate = binding.event_time_predicate,
        .plan = plan.intent,
        .result_schema = binding.result_schema.get(),
        .pre_group_program =
            binding.pre_group_program == nullptr
                ? std::nullopt
                : std::optional<DistributedVectorPreGroupProgram>{*binding.pre_group_program}};
    const common::Status structural = validate_distributed_mutable_vector_fragment(fragment);
    if (!structural.is_ok())
      return common::make_unexpected(structural);
    return fragment;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector fragment allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable vector fragment exceeds limits"));
  }
}

} // namespace chronos::query
