#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_vector_fragment.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void rewrite_checksums(std::vector<std::byte>& bytes) {
  store_u32_le(bytes, 228U, common::crc32c(common::ByteView{bytes}.first(228U)));
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

[[nodiscard]] DistributedVectorFragmentDispatch dispatch() {
  return {
      .query_id = uuid(1U),
      .database_id = manifest::DatabaseId::from_uuid(uuid(2U)).value(),
      .table_id = schema::TableId::from_uuid(uuid(3U)).value(),
      .tablet_id = schema::TabletId::from_uuid(uuid(4U)).value(),
      .destination_schema_id = schema::SchemaId::from_uuid(uuid(5U)).value(),
      .raft_group_id = uuid(6U),
      .snapshot_generation = 7U,
      .serving_node = 8U,
      .applied_position = 90U,
      .observed_leader_commit_position = 100U,
      .placement_epoch = 11U,
      .read_policy = {.consistency = DistributedReadConsistency::kFollowerBoundedStale,
                      .maximum_staleness_positions = 10U},
      .destination_column_ordinals = {4U, 1U, 7U},
      .event_time_predicate =
          cseg::EventTimePredicate{.lower = cseg::EventTimeBound{.value = -5, .inclusive = true},
                                   .upper = cseg::EventTimeBound{.value = 9, .inclusive = false}},
      .plan = {.mode = DistributedVectorPlanMode::kGroupedAggregate,
               .group_key_input_indices = {0U, 2U},
               .aggregates = {{.operation = VectorAggregateOperation::kCountStar},
                              {.operation = VectorAggregateOperation::kSum, .input_index = 1U}},
               .order_keys = {{.output_index = 3U,
                               .direction = PhysicalSortDirection::kDescending,
                               .null_placement = ScalarNullPlacement::kFirst}},
               .limit = 5U}};
}

TEST(DistributedVectorFragmentTest, RoundTripsGroupScopedSnapshotProofProjectionAndPlan) {
  const DistributedVectorFragmentDispatch expected = dispatch();
  const auto encoded = encode_distributed_vector_fragment_dispatch(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const auto decoded = decode_distributed_vector_fragment_dispatch_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
  EXPECT_EQ(decoded->plan.group_key_input_indices, (std::vector<std::uint32_t>{0U, 2U}));
  EXPECT_EQ(decoded->raft_group_id, uuid(6U));
}

TEST(DistributedVectorFragmentTest, RejectsDamageLimitsAndContradictoryAuthority) {
  const DistributedVectorFragmentDispatch expected = dispatch();
  const auto encoded = encode_distributed_vector_fragment_dispatch(expected);
  ASSERT_TRUE(encoded.has_value());
  std::vector<std::byte> bytes(encoded->bytes().begin(), encoded->bytes().end());
  EXPECT_EQ(decode_distributed_vector_fragment_dispatch_exact(
                common::ByteView{bytes}.first(bytes.size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> unsupported = bytes;
  unsupported[8U] = std::byte{2U};
  rewrite_checksums(unsupported);
  EXPECT_EQ(decode_distributed_vector_fragment_dispatch_exact(unsupported).error().code(),
            common::StatusCode::kNotSupported);

  std::vector<std::byte> damaged_plan = bytes;
  damaged_plan[232U + expected.destination_column_ordinals.size() * 4U] ^= std::byte{1U};
  store_u32_le(damaged_plan, damaged_plan.size() - 4U,
               common::crc32c(common::ByteView{damaged_plan}.first(damaged_plan.size() - 4U)));
  EXPECT_EQ(decode_distributed_vector_fragment_dispatch_exact(damaged_plan).error().code(),
            common::StatusCode::kCorruption);

  const auto encoded_plan = encode_distributed_vector_plan_intent(expected.plan);
  ASSERT_TRUE(encoded_plan.has_value());
  std::vector<std::byte> contradictory_plan = bytes;
  const std::size_t plan_offset =
      232U + expected.destination_column_ordinals.size() * sizeof(std::uint32_t);
  contradictory_plan[plan_offset + 48U] = std::byte{3U};
  store_u32_le(contradictory_plan, plan_offset + encoded_plan->bytes().size() - 4U,
               common::crc32c(common::ByteView{contradictory_plan}.subspan(
                   plan_offset, encoded_plan->bytes().size() - 4U)));
  store_u32_le(
      contradictory_plan, contradictory_plan.size() - 4U,
      common::crc32c(common::ByteView{contradictory_plan}.first(contradictory_plan.size() - 4U)));
  EXPECT_EQ(decode_distributed_vector_fragment_dispatch_exact(contradictory_plan).error().code(),
            common::StatusCode::kCorruption);

  EXPECT_EQ(decode_distributed_vector_fragment_dispatch_exact(encoded->bytes(),
                                                              {.maximum_projection_columns = 2U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  DistributedVectorFragmentDispatch invalid_plan = expected;
  invalid_plan.plan.group_key_input_indices.front() = 3U;
  EXPECT_EQ(encode_distributed_vector_fragment_dispatch(invalid_plan).error().code(),
            common::StatusCode::kInvalidArgument);
  DistributedVectorFragmentDispatch invalid_proof = expected;
  invalid_proof.read_policy.maximum_staleness_positions = 9U;
  EXPECT_EQ(encode_distributed_vector_fragment_dispatch(invalid_proof).error().code(),
            common::StatusCode::kInvalidArgument);
  DistributedVectorFragmentDispatch nil_group = expected;
  nil_group.raft_group_id = common::Uuid{};
  EXPECT_EQ(encode_distributed_vector_fragment_dispatch(nil_group).error().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
