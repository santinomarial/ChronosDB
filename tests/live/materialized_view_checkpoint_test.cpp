#include "chronos/common/crc32c.hpp"
#include "chronos/live/materialized_view_checkpoint.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet_id() {
  return schema::TabletId::from_uuid(uuid(std::byte{0x31U})).value();
}

[[nodiscard]] wal::WalId wal_id() {
  wal::WalId value;
  value.bytes.fill(std::byte{0x42U});
  return value;
}

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value);
  bytes[offset + 1U] = static_cast<std::byte>(value >> 8U);
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index) {
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
  }
}

void refresh_checksums(std::vector<std::byte>& bytes) {
  store_u32(bytes, 144U, 0U);
  store_u32(bytes, 144U,
            common::crc32c(common::ByteView{bytes}.first(kMaterializedViewCheckpointHeaderSize)));
  store_u32(bytes, bytes.size() - kMaterializedViewCheckpointTrailerSize,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() -
                                                         kMaterializedViewCheckpointTrailerSize)));
}

[[nodiscard]] WindowedMaterializedViewCheckpoint checkpoint() {
  const schema::TabletId tablet = tablet_id();
  const wal::WalId wal = wal_id();
  auto view = WindowedMaterializedView::create(tablet, wal, WindowDefinition{10, 5, 2, 16U, 16U});
  EXPECT_TRUE(view.has_value());
  EXPECT_TRUE(view->apply_committed(SourcePosition{tablet, wal, 1U},
                                    MaterializedViewInput{{1U, 7, 1U, 10.0, 2.0}, false})
                  .has_value());
  EXPECT_TRUE(view->apply_committed(SourcePosition{tablet, wal, 2U},
                                    MaterializedViewInput{{2U, 8, 2U, 20.0, 1.0}, false})
                  .has_value());
  EXPECT_TRUE(view->advance_watermark(12).has_value());
  return std::move(view->checkpoint().value());
}

[[nodiscard]] BoundMaterializedViewCheckpoint bound_checkpoint() {
  PlanFingerprint plan{};
  plan.fill(std::byte{0x77U});
  return BoundMaterializedViewCheckpoint{
      .identity = {.database_id = uuid(std::byte{0x51U}),
                   .view_id = uuid(std::byte{0x52U}),
                   .table_id = schema::TableId::from_uuid(uuid(std::byte{0x53U})).value(),
                   .schema_id = schema::SchemaId::from_uuid(uuid(std::byte{0x54U})).value(),
                   .schema_version = schema::SchemaVersion::initial(),
                   .plan_fingerprint = plan},
      .state = checkpoint()};
}

TEST(MaterializedViewCheckpointTest, RoundTripsExactStateAndContinuation) {
  const auto original = checkpoint();
  auto encoded = encode_windowed_materialized_view_checkpoint_v1(original);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(), kMaterializedViewCheckpointHeaderSize +
                                 original.rows.size() * kMaterializedViewCheckpointRowSize +
                                 original.windows.size() * kMaterializedViewCheckpointWindowSize +
                                 4U * kMaterializedViewCheckpointRowSize +
                                 kMaterializedViewCheckpointTrailerSize);

  auto decoded = decode_windowed_materialized_view_checkpoint_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, original);

  auto before = WindowedMaterializedView::restore(original);
  auto after = WindowedMaterializedView::restore(*decoded);
  ASSERT_TRUE(before.has_value());
  ASSERT_TRUE(after.has_value());
  const SourcePosition next{tablet_id(), wal_id(), 3U};
  const MaterializedViewInput correction{{1U, 1, 3U, 30.0, 2.0}, false};
  EXPECT_EQ(after->apply_committed(next, correction), before->apply_committed(next, correction));
}

TEST(MaterializedViewCheckpointTest, RejectsCorruptionUnknownVersionAndDecodeLimits) {
  auto encoded = encode_windowed_materialized_view_checkpoint_v1(checkpoint()).value();
  encoded[kMaterializedViewCheckpointHeaderSize] ^= std::byte{0x01U};
  EXPECT_EQ(decode_windowed_materialized_view_checkpoint_v1(encoded).error().code(),
            common::StatusCode::kCorruption);

  encoded = encode_windowed_materialized_view_checkpoint_v1(checkpoint()).value();
  store_u16(encoded, 8U, 2U);
  refresh_checksums(encoded);
  EXPECT_EQ(decode_windowed_materialized_view_checkpoint_v1(encoded).error().code(),
            common::StatusCode::kNotSupported);

  encoded = encode_windowed_materialized_view_checkpoint_v1(checkpoint()).value();
  MaterializedViewCheckpointCodecLimits limits;
  limits.maximum_rows = 1U;
  EXPECT_EQ(decode_windowed_materialized_view_checkpoint_v1(encoded, limits).error().code(),
            common::StatusCode::kResourceExhausted);
}

TEST(MaterializedViewCheckpointTest, BoundEnvelopeRejectsCrossViewIdentityLoss) {
  auto original = bound_checkpoint();
  original.checkpoint_generation = 7U;
  auto encoded = encode_bound_materialized_view_checkpoint_v1(original);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_bound_materialized_view_checkpoint_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->identity, original.identity);
  EXPECT_EQ(decoded->checkpoint_generation, original.checkpoint_generation);
  EXPECT_EQ(*decoded, original);

  std::fill_n(encoded->begin() + 32, 16U, std::byte{0U});
  refresh_checksums(*encoded);
  EXPECT_EQ(decode_bound_materialized_view_checkpoint_v1(*encoded).error().code(),
            common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::live
