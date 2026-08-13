#include "chronos/cluster/distributed_grouped_query_transport.hpp"
#include "chronos/cluster/distributed_vector_query_transport.hpp"
#include "chronos/common/crc32c.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] query::DistributedVectorFragmentDispatch dispatch() {
  return {.query_id = uuid(1U),
          .database_id = id<manifest::DatabaseId>(2U),
          .table_id = id<schema::TableId>(3U),
          .tablet_id = id<schema::TabletId>(4U),
          .destination_schema_id = id<schema::SchemaId>(5U),
          .raft_group_id = uuid(9U),
          .snapshot_generation = 6U,
          .serving_node = 2U,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 8U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U, 1U},
          .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                   .group_key_input_indices = {0U},
                   .aggregates = {{.operation = query::VectorAggregateOperation::kSum,
                                   .input_index = 1U}},
                   .order_keys = {{.output_index = 1U,
                                   .direction = query::PhysicalSortDirection::kDescending}},
                   .limit = 3U}};
}

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void rewrite_checksums(std::vector<std::byte>& bytes) {
  const common::ByteView payload =
      common::ByteView{bytes}.subspan(kDistributedVectorQueryRequestHeaderSize,
                                      bytes.size() - kDistributedVectorQueryRequestHeaderSize -
                                          kDistributedVectorQueryRequestTrailerSize);
  store_u32(bytes, 48U, common::crc32c(payload));
  store_u32(bytes, 76U, common::crc32c(common::ByteView{bytes}.first(76U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedVectorQueryTransportTest, RoundTripsDistinctProofBoundRequest) {
  const DistributedVectorQueryRequest request{1U, 2U, dispatch()};
  const auto encoded = encode_distributed_vector_query_request_v1(request);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_LE(encoded->size(), kMaximumDistributedVectorQueryRequestSize);
  const auto decoded = decode_distributed_vector_query_request_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, request);
  EXPECT_EQ(decode_distributed_grouped_query_request_v1(*encoded).error().code(),
            common::StatusCode::kCorruption);

  DistributedVectorQueryRequest invalid_route = request;
  invalid_route.target_node_id = invalid_route.source_node_id;
  EXPECT_EQ(encode_distributed_vector_query_request_v1(invalid_route).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorQueryTransportTest, RejectsOuterAndNestedDamageAndFutureVersion) {
  const auto encoded = encode_distributed_vector_query_request_v1({1U, 2U, dispatch()});
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ(decode_distributed_vector_query_request_v1(
                common::ByteView{*encoded}.first(encoded->size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> future = *encoded;
  store_u16(future, 8U, 2U);
  rewrite_checksums(future);
  EXPECT_EQ(decode_distributed_vector_query_request_v1(future).error().code(),
            common::StatusCode::kNotSupported);

  std::vector<std::byte> nested = *encoded;
  nested[kDistributedVectorQueryRequestHeaderSize + 24U] ^= std::byte{1U};
  rewrite_checksums(nested);
  EXPECT_EQ(decode_distributed_vector_query_request_v1(nested).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> reserved = *encoded;
  reserved[52U] = std::byte{1U};
  rewrite_checksums(reserved);
  EXPECT_EQ(decode_distributed_vector_query_request_v1(reserved).error().code(),
            common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::cluster
