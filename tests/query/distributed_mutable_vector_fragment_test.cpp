#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_fragment_worker.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8U> kMutableMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'M'},
    std::byte{'V'}, std::byte{'F'}, std::byte{'R'}, std::byte{'1'}};

[[nodiscard]] common::Uuid uuid(const std::uint16_t value) {
  common::Uuid::Bytes bytes{};
  bytes[14] = static_cast<std::byte>((value >> 8U) & 0xffU);
  bytes[15] = static_cast<std::byte>(value & 0xffU);
  return common::Uuid{bytes};
}

[[nodiscard]] ingest::Sha256Digest digest(const std::uint8_t seed) {
  ingest::Sha256Digest::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return ingest::Sha256Digest{bytes};
}

void store_u16_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void store_u32_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void refresh_checksums(std::vector<std::byte>& bytes) {
  store_u32_le(bytes, 240U, common::crc32c(common::ByteView{bytes}.first(240U)));
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

class RowCounter final : public DistributedVectorRowsChunkConsumerV2 {
public:
  common::Status consume(const VectorChunk& chunk) override {
    if (chunk.column_count() != 1U)
      return {common::StatusCode::kCorruption, "mutable fragment test width differs"};
    rows += chunk.selected_row_count();
    ++chunks;
    return common::Status::ok();
  }

  std::size_t rows{};
  std::size_t chunks{};
};

struct Fixture {
  std::shared_ptr<const schema::TableSchema> schema_value{columnar::test::batch_schema()};
  schema::TabletId tablet_id{columnar::test::id<schema::TabletId>(52U)};
  common::Uuid group_id{uuid(80U)};
  manifest::DatabaseId database_id{manifest::DatabaseId::from_uuid(uuid(81U)).value()};
  ingest::TabletState tablet{
      ingest::TabletState::create(
          schema_value, tablet_id,
          {.head_capacity = {.row_capacity = 4U, .variable_value_bytes = {0U, 2U, 0U}},
           .maximum_schema_versions = 1U,
           .maximum_sealed_generations = 1U,
           .maximum_retry_entries = 2U,
           .flush_queue = nullptr})
          .value()};
  schema::SchemaLineage lineage{schema::SchemaLineage::create(*schema_value).value()};

  [[nodiscard]] ingest::TabletSnapshot append() {
    auto batch = std::make_shared<const columnar::OwnedColumnarBatch>(
        columnar::OwnedColumnarBatch::create(schema_value, columnar::test::batch_columns())
            .value());
    const ingest::RetryIdentity retry{.client_id = ingest::test::request_id<ingest::ClientId>(1U),
                                      .client_batch_id =
                                          ingest::test::request_id<ingest::ClientBatchId>(33U)};
    const ingest::ColumnarAppendMutationIdentity mutation{
        .table_id = schema_value->table_id(), .tablet_id = tablet_id, .request_digest = digest(1U)};
    auto prepared = tablet.prepare_append(retry, mutation, std::move(batch));
    EXPECT_TRUE(prepared.has_value()) << prepared.error().to_string();
    EXPECT_TRUE(prepared->mark_wal_started().is_ok());
    auto published = prepared->publish(head::HeadCommitPosition::raft(group_id, 5U));
    EXPECT_TRUE(published.has_value()) << published.error().to_string();
    return std::move(published->snapshot);
  }
};

TEST(DistributedMutableVectorFragmentTest,
     BindsAndExecutesOneExactCommittedMutableTabletPublication) {
  Fixture fixture;
  const ingest::TabletSnapshot snapshot = fixture.append();
  const raft::ReadBarrier barrier{.term = 3U, .context = 4U, .read_index = 5U};
  const DistributedVectorQueryPlan plan{
      .query_id = uuid(82U),
      .read_policy = {.consistency = DistributedReadConsistency::kLeaderLinearizable},
      .fragments = {{.tablet_id = fixture.tablet_id,
                     .minimum_event_time = 0,
                     .maximum_event_time = 0,
                     .leader_node = 11U,
                     .local_applied_position = 5U,
                     .known_leader_commit_position = 5U}},
      .intent = {.mode = DistributedVectorPlanMode::kRows, .row_output_indices = {1U}}};
  const DistributedReadAdmission admission{.tablet_id = fixture.tablet_id,
                                           .serving_node = 11U,
                                           .applied_position = 5U,
                                           .observed_leader_commit_position = 5U,
                                           .linearizable_barrier = barrier};
  const raft::TabletPlacementMetadata placement{.table_id = fixture.schema_value->table_id(),
                                                .tablet_id = fixture.tablet_id,
                                                .placement_epoch = 7U,
                                                .replicas = {11U, 12U},
                                                .leader_hint = 12U};
  const std::array<std::uint32_t, 3U> projection{0U, 1U, 2U};
  const DistributedVectorResultSchema result_schema{
      .columns = {{"tag", fixture.schema_value->columns()[1].type(), true}}};
  auto fragment =
      bind_distributed_mutable_vector_fragment({.plan = std::cref(plan),
                                                .admission = std::cref(admission),
                                                .database_id = fixture.database_id,
                                                .snapshot = std::cref(snapshot),
                                                .lineage = std::cref(fixture.lineage),
                                                .raft_group_id = fixture.group_id,
                                                .placement = std::cref(placement),
                                                .destination_column_ordinals = projection,
                                                .event_time_predicate = std::nullopt,
                                                .result_schema = std::cref(result_schema)});
  ASSERT_TRUE(fragment.has_value()) << fragment.error().to_string();
  EXPECT_EQ(fragment->applied_position, 5U);
  EXPECT_EQ(fragment->raft_group_id, fixture.group_id);
  auto encoded = encode_distributed_mutable_vector_fragment(*fragment);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_TRUE(std::ranges::equal(encoded->bytes().first(8U), kMutableMagic));
  auto decoded = decode_distributed_mutable_vector_fragment_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, *fragment);
  EXPECT_EQ(decode_distributed_vector_fragment_dispatch_v2_exact(encoded->bytes()).error().code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> damaged{encoded->bytes().begin(), encoded->bytes().end()};
  damaged[136U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_mutable_vector_fragment_exact(damaged).error().code(),
            common::StatusCode::kCorruption);
  damaged.assign(encoded->bytes().begin(), encoded->bytes().end());
  damaged.back() ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_mutable_vector_fragment_exact(damaged).error().code(),
            common::StatusCode::kCorruption);
  damaged.assign(encoded->bytes().begin(), encoded->bytes().end());
  store_u16_le(damaged, 8U, 2U);
  refresh_checksums(damaged);
  EXPECT_EQ(decode_distributed_mutable_vector_fragment_exact(damaged).error().code(),
            common::StatusCode::kNotSupported);
  damaged.assign(encoded->bytes().begin(), encoded->bytes().end());
  damaged[distributed_mutable_vector_fragment_format::kHeaderLength + projection.size() * 4U] ^=
      std::byte{1U};
  store_u32_le(damaged, damaged.size() - 4U,
               common::crc32c(common::ByteView{damaged}.first(damaged.size() - 4U)));
  EXPECT_EQ(decode_distributed_mutable_vector_fragment_exact(damaged).error().code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(decode_distributed_mutable_vector_fragment_exact(
                encoded->bytes(), {.maximum_frame_length = encoded->bytes().size() - 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(decode_distributed_mutable_vector_fragment_exact(encoded->bytes(),
                                                             {.maximum_projection_columns = 2U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  RowCounter consumer;
  auto executed =
      execute_distributed_mutable_vector_rows_fragment({.fragment = std::cref(*fragment),
                                                        .snapshot = std::cref(snapshot),
                                                        .lineage = std::cref(fixture.lineage),
                                                        .placement = std::cref(placement),
                                                        .raft_group_id = fixture.group_id,
                                                        .local_node = 11U,
                                                        .local_linearizable_barrier = barrier,
                                                        .limits = {}},
                                                       consumer);
  ASSERT_TRUE(executed.has_value()) << executed.error().to_string();
  EXPECT_EQ(executed->output_rows, 2U);
  EXPECT_EQ(executed->output_chunks, 1U);
  EXPECT_EQ(consumer.rows, 2U);
  EXPECT_EQ(consumer.chunks, 1U);

  auto later = *fragment;
  later.applied_position = 6U;
  RowCounter rejected_consumer;
  auto rejected =
      execute_distributed_mutable_vector_rows_fragment({.fragment = std::cref(later),
                                                        .snapshot = std::cref(snapshot),
                                                        .lineage = std::cref(fixture.lineage),
                                                        .placement = std::cref(placement),
                                                        .raft_group_id = fixture.group_id,
                                                        .local_node = 11U,
                                                        .local_linearizable_barrier = barrier,
                                                        .limits = {}},
                                                       rejected_consumer);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(rejected_consumer.rows, 0U);
}

TEST(DistributedMutableVectorFragmentTest, RejectsMixedAdmissionAndPublicationAuthority) {
  Fixture fixture;
  const ingest::TabletSnapshot snapshot = fixture.append();
  const DistributedVectorQueryPlan plan{
      .query_id = uuid(83U),
      .read_policy = {.consistency = DistributedReadConsistency::kLeaderLinearizable},
      .fragments = {{.tablet_id = fixture.tablet_id,
                     .leader_node = 11U,
                     .local_applied_position = 6U,
                     .known_leader_commit_position = 6U}},
      .intent = {.mode = DistributedVectorPlanMode::kRows, .row_output_indices = {0U}}};
  const DistributedReadAdmission admission{
      .tablet_id = fixture.tablet_id,
      .serving_node = 11U,
      .applied_position = 6U,
      .observed_leader_commit_position = 6U,
      .linearizable_barrier = raft::ReadBarrier{.term = 3U, .context = 4U, .read_index = 5U}};
  const raft::TabletPlacementMetadata placement{.table_id = fixture.schema_value->table_id(),
                                                .tablet_id = fixture.tablet_id,
                                                .placement_epoch = 7U,
                                                .replicas = {11U},
                                                .leader_hint = 11U};
  const std::array<std::uint32_t, 1U> projection{0U};
  const DistributedVectorResultSchema result_schema{
      .columns = {{"ts", fixture.schema_value->columns()[0].type(), false}}};
  auto fragment =
      bind_distributed_mutable_vector_fragment({.plan = std::cref(plan),
                                                .admission = std::cref(admission),
                                                .database_id = fixture.database_id,
                                                .snapshot = std::cref(snapshot),
                                                .lineage = std::cref(fixture.lineage),
                                                .raft_group_id = fixture.group_id,
                                                .placement = std::cref(placement),
                                                .destination_column_ordinals = projection,
                                                .event_time_predicate = std::nullopt,
                                                .result_schema = std::cref(result_schema)});
  ASSERT_FALSE(fragment.has_value());
  EXPECT_EQ(fragment.error().code(), common::StatusCode::kUnavailable);
}

} // namespace
} // namespace chronos::query
