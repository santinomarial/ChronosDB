#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_vector_fragment.hpp"
#include "chronos/query/distributed_vector_fragment_v2.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <ranges>
#include <utility>
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

void rewrite_v2_checksums(std::vector<std::byte>& bytes) {
  store_u32_le(bytes, 48U, common::crc32c(common::ByteView{bytes}.first(48U)));
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

[[nodiscard]] DistributedVectorFragmentDispatchV2 dispatch_v2() {
  return {
      .dispatch = dispatch(),
      .result_schema = {
          .columns = {
              {"key_a", schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
               false},
              {"key_b", schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
               true},
              {"rows", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false},
              {"total", schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(),
               true}}}};
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

  const DistributedVectorFragmentDispatchV2 expected_v2 = dispatch_v2();
  const auto encoded_v2 = encode_distributed_vector_fragment_dispatch_v2(expected_v2);
  ASSERT_TRUE(encoded_v2.has_value()) << encoded_v2.error().to_string();
  const auto decoded_v2 = decode_distributed_vector_fragment_dispatch_v2_exact(encoded_v2->bytes());
  ASSERT_TRUE(decoded_v2.has_value()) << decoded_v2.error().to_string();
  EXPECT_EQ(*decoded_v2, expected_v2);
  EXPECT_FALSE(decode_distributed_vector_fragment_dispatch_exact(encoded_v2->bytes()).has_value());
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
  std::vector<std::byte> unknown_consistency = bytes;
  unknown_consistency[200U] = std::byte{0xffU};
  rewrite_checksums(unknown_consistency);
  EXPECT_EQ(decode_distributed_vector_fragment_dispatch_exact(unknown_consistency).error().code(),
            common::StatusCode::kCorruption);

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

  const DistributedVectorFragmentDispatchV2 v2{
      .dispatch = expected,
      .result_schema = {
          .columns = {{"result",
                       schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(),
                       true}}}};
  const auto encoded_v2 = encode_distributed_vector_fragment_dispatch_v2(v2);
  ASSERT_TRUE(encoded_v2.has_value());
  EXPECT_EQ(decode_distributed_vector_fragment_dispatch_v2_exact(
                encoded_v2->bytes().first(encoded_v2->bytes().size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(decode_distributed_vector_fragment_dispatch_v2_exact(
                encoded_v2->bytes(), {.maximum_frame_length = encoded_v2->bytes().size() - 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  std::vector<std::byte> nested_damage(encoded_v2->bytes().begin(), encoded_v2->bytes().end());
  const std::size_t schema_offset = 64U + encoded->bytes().size();
  nested_damage[schema_offset] ^= std::byte{1U};
  const common::ByteView schema_bytes = common::ByteView{nested_damage}.subspan(
      schema_offset, nested_damage.size() - schema_offset - 4U);
  store_u32_le(nested_damage, 44U, common::crc32c(schema_bytes));
  store_u32_le(nested_damage, 48U, common::crc32c(common::ByteView{nested_damage}.first(48U)));
  store_u32_le(nested_damage, nested_damage.size() - 4U,
               common::crc32c(common::ByteView{nested_damage}.first(nested_damage.size() - 4U)));
  EXPECT_EQ(decode_distributed_vector_fragment_dispatch_v2_exact(nested_damage).error().code(),
            common::StatusCode::kCorruption);
}

TEST(DistributedVectorFragmentTest, OwnsBoundedPartialReadsAndShortWriteProgress) {
  const DistributedVectorFragmentDispatch expected = dispatch();
  const auto encoded = encode_distributed_vector_fragment_dispatch(expected);
  ASSERT_TRUE(encoded.has_value());

  for (std::size_t split = 0U; split <= encoded->bytes().size(); ++split) {
    DistributedVectorFragmentReader reader;
    const auto prefix = reader.consume(encoded->bytes().first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes, split) << "split=" << split;
    EXPECT_EQ(prefix->dispatch.has_value(), split == encoded->bytes().size()) << "split=" << split;
    const auto suffix = reader.consume(encoded->bytes().subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    EXPECT_EQ(suffix->consumed_bytes, encoded->bytes().size() - split) << "split=" << split;
    ASSERT_TRUE(prefix->dispatch.has_value() || suffix->dispatch.has_value()) << "split=" << split;
    EXPECT_TRUE(prefix->dispatch == expected || suffix->dispatch == expected) << "split=" << split;
    EXPECT_EQ(reader.buffered_bytes(), 0U) << "split=" << split;
  }

  std::vector<std::byte> coalesced(encoded->bytes().begin(), encoded->bytes().end());
  coalesced.insert(coalesced.end(), encoded->bytes().begin(), encoded->bytes().end());
  DistributedVectorFragmentReader coalesced_reader;
  const auto first = coalesced_reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->dispatch.has_value());
  EXPECT_EQ(first->consumed_bytes, encoded->bytes().size());
  const auto second =
      coalesced_reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->dispatch.has_value());
  EXPECT_EQ(second->consumed_bytes, encoded->bytes().size());

  std::vector<std::byte> corrupt(encoded->bytes().begin(), encoded->bytes().end());
  corrupt.front() ^= std::byte{1U};
  DistributedVectorFragmentReader failed_reader;
  const auto rejected = failed_reader.consume(corrupt);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_TRUE(failed_reader.failed());
  EXPECT_EQ(failed_reader.buffered_bytes(), distributed_vector_fragment_format::kHeaderLength);
  EXPECT_EQ(failed_reader.consume(encoded->bytes()).error(), rejected.error());

  DistributedVectorFragmentReader limited_reader(
      {.maximum_frame_length = encoded->bytes().size() - 1U});
  EXPECT_EQ(limited_reader.consume(encoded->bytes()).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(decode_distributed_vector_fragment_dispatch_exact(
                encoded->bytes(), {.maximum_frame_length = encoded->bytes().size() - 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  auto cursor = DistributedVectorFragmentWriteCursor::create(expected);
  ASSERT_TRUE(cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), encoded->bytes()));
  ASSERT_TRUE(cursor->consume_written(23U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 23U);
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), encoded->bytes().subspan(23U)));
  EXPECT_EQ(cursor->consume_written(cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 23U);
  DistributedVectorFragmentWriteCursor moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(cursor->pending_write().empty());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
}

TEST(DistributedVectorFragmentTest, V2OwnsBoundedPartialReadsAndShortWriteProgress) {
  const DistributedVectorFragmentDispatchV2 expected = dispatch_v2();
  const auto encoded = encode_distributed_vector_fragment_dispatch_v2(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();

  for (std::size_t split = 0U; split <= encoded->bytes().size(); ++split) {
    DistributedVectorFragmentV2Reader reader;
    const auto prefix = reader.consume(encoded->bytes().first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes, split) << "split=" << split;
    EXPECT_EQ(prefix->dispatch.has_value(), split == encoded->bytes().size()) << "split=" << split;
    const auto suffix = reader.consume(encoded->bytes().subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    EXPECT_EQ(suffix->consumed_bytes, encoded->bytes().size() - split) << "split=" << split;
    ASSERT_TRUE(prefix->dispatch.has_value() || suffix->dispatch.has_value()) << "split=" << split;
    EXPECT_TRUE(prefix->dispatch == expected || suffix->dispatch == expected) << "split=" << split;
    EXPECT_EQ(reader.buffered_bytes(), 0U) << "split=" << split;
  }

  std::vector<std::byte> coalesced(encoded->bytes().begin(), encoded->bytes().end());
  coalesced.insert(coalesced.end(), encoded->bytes().begin(), encoded->bytes().end());
  DistributedVectorFragmentV2Reader coalesced_reader;
  const auto first = coalesced_reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->dispatch.has_value());
  EXPECT_EQ(first->consumed_bytes, encoded->bytes().size());
  const auto second =
      coalesced_reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->dispatch.has_value());
  EXPECT_EQ(second->consumed_bytes, encoded->bytes().size());

  std::vector<std::byte> corrupt(encoded->bytes().begin(), encoded->bytes().end());
  corrupt.front() ^= std::byte{1U};
  DistributedVectorFragmentV2Reader failed_reader;
  const auto rejected = failed_reader.consume(corrupt);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_TRUE(failed_reader.failed());
  EXPECT_EQ(failed_reader.buffered_bytes(), distributed_vector_fragment_v2_format::kHeaderLength);
  EXPECT_EQ(failed_reader.consume(encoded->bytes()).error(), rejected.error());

  const auto nested_dispatch = encode_distributed_vector_fragment_dispatch(expected.dispatch);
  const auto nested_schema = encode_distributed_vector_result_schema(expected.result_schema);
  ASSERT_TRUE(nested_dispatch.has_value());
  ASSERT_TRUE(nested_schema.has_value());
  DistributedVectorFragmentV2DecodeLimits limits;
  limits.dispatch.maximum_frame_length = nested_dispatch->bytes().size() - 1U;
  DistributedVectorFragmentV2Reader limited_reader{limits};
  EXPECT_EQ(limited_reader.consume(encoded->bytes()).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(limited_reader.failed());
  EXPECT_EQ(
      decode_distributed_vector_fragment_dispatch_v2_exact(encoded->bytes(), limits).error().code(),
      common::StatusCode::kResourceExhausted);

  limits = {};
  limits.result_schema.maximum_frame_length = nested_schema->bytes().size() - 1U;
  EXPECT_EQ(
      decode_distributed_vector_fragment_dispatch_v2_exact(encoded->bytes(), limits).error().code(),
      common::StatusCode::kResourceExhausted);

  std::vector<std::byte> future(encoded->bytes().begin(), encoded->bytes().end());
  future[8U] = std::byte{3U};
  rewrite_v2_checksums(future);
  DistributedVectorFragmentV2Reader future_reader;
  EXPECT_EQ(future_reader.consume(future).error().code(), common::StatusCode::kNotSupported);
  const auto v1 = encode_distributed_vector_fragment_dispatch(expected.dispatch);
  ASSERT_TRUE(v1.has_value());
  EXPECT_EQ(decode_distributed_vector_fragment_dispatch_v2_exact(v1->bytes()).error().code(),
            common::StatusCode::kCorruption);

  auto cursor = DistributedVectorFragmentV2WriteCursor::create(expected);
  ASSERT_TRUE(cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), encoded->bytes()));
  ASSERT_TRUE(cursor->consume_written(31U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 31U);
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), encoded->bytes().subspan(31U)));
  EXPECT_EQ(cursor->consume_written(cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 31U);
  DistributedVectorFragmentV2WriteCursor moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(cursor->pending_write().empty());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
}

} // namespace
} // namespace chronos::query
