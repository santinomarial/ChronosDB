#include "chronos/query/committed_temporal_command.hpp"
#include "columnar/columnar_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] wal::WalId wal_id(const std::uint8_t seed) {
  wal::WalId value;
  value.bytes.front() = std::byte{seed};
  return value;
}

[[nodiscard]] std::vector<TemporalMutationDescriptor> descriptors(const TemporalMutationKind kind) {
  return {{{std::byte{1U}}, 100, 110, kind}, {{std::byte{2U}}, 200, 220, kind}};
}

TEST(CommittedTemporalCommandTest, ConvertsCanonicalCellsAndPublishesOneAtomicPosition) {
  const std::shared_ptr<const schema::TableSchema> retained = columnar::test::batch_schema();
  auto batch = columnar::OwnedColumnarBatch::create(retained, columnar::test::batch_columns());
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  auto encoded =
      encode_temporal_command_v1(*batch, descriptors(TemporalMutationKind::kOriginal), 1000);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_temporal_command_v1(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  auto provider = TemporalSnapshotProvider::create(retained);
  ASSERT_TRUE(provider.has_value()) << provider.error().to_string();

  auto applied = apply_committed_temporal_command(*decoded, *retained, 7U, wal_id(9U), **provider);
  ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
  EXPECT_EQ(applied->system_commit_position, 7U);
  EXPECT_EQ(applied->applied_mutation_count, 2U);
  EXPECT_EQ((*provider)->latest_commit_position(), 7U);
  EXPECT_EQ((*provider)->logical_row_count(), 2U);
  EXPECT_EQ((*provider)->version_count(), 2U);

  auto snapshot = (*provider)->resolve(retained, std::nullopt);
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  ASSERT_EQ((*snapshot)->rows().size(), 2U);
  EXPECT_EQ((*snapshot)->rows()[0].wal_id, common::Uuid{wal_id(9U).bytes});
  EXPECT_EQ((*snapshot)->rows()[0].record_sequence, 7U);
  EXPECT_EQ((*snapshot)->rows()[0].system_commit_position, 7U);

  auto verified = verify_retained_temporal_command(*decoded, *retained, 7U, wal_id(9U), **provider);
  ASSERT_TRUE(verified.has_value()) << verified.error().to_string();
  auto different_time =
      encode_temporal_command_v1(*batch, descriptors(TemporalMutationKind::kOriginal), 1001);
  ASSERT_TRUE(different_time.has_value());
  auto different_decoded = decode_temporal_command_v1(different_time->bytes());
  ASSERT_TRUE(different_decoded.has_value());
  EXPECT_EQ(
      verify_retained_temporal_command(*different_decoded, *retained, 7U, wal_id(9U), **provider)
          .error()
          .code(),
      common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::query
