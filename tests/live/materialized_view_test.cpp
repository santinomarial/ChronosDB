#include "chronos/live/materialized_view.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <utility>

namespace chronos::live {
namespace {

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet_id() {
  auto value = schema::TabletId::from_uuid(uuid(std::byte{1}));
  EXPECT_TRUE(value.has_value());
  return *value;
}

[[nodiscard]] wal::WalId wal_id() {
  wal::WalId value{};
  value.bytes.fill(std::byte{2});
  return value;
}

TEST(MaterializedViewTest, AppliesCorrectionsAndFinalizesTumblingWindow) {
  const auto tablet = tablet_id();
  const auto wal = wal_id();
  auto view = WindowedMaterializedView::create(tablet, wal, WindowDefinition{10, 10, 2, 16U, 16U});
  ASSERT_TRUE(view.has_value()) << view.error().to_string();

  auto first = view->apply_committed(SourcePosition{tablet, wal, 1U},
                                     MaterializedViewInput{{1U, 1, 1U, 10.0, 2.0}, false});
  ASSERT_TRUE(first.has_value());
  ASSERT_EQ(first->size(), 1U);
  EXPECT_EQ(first->front().window, (WindowKey{0, 10}));
  EXPECT_EQ(first->front().status, WindowResultStatus::kProvisional);
  EXPECT_EQ(first->front().value.count, 1U);

  auto second = view->apply_committed(SourcePosition{tablet, wal, 2U},
                                      MaterializedViewInput{{2U, 2, 2U, 20.0, 1.0}, false});
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->front().status, WindowResultStatus::kCorrected);
  EXPECT_DOUBLE_EQ(second->front().value.sum, 30.0);
  EXPECT_NEAR(*second->front().value.vwap, 40.0 / 3.0, 1e-12);

  auto finalized = view->advance_watermark(12);
  ASSERT_TRUE(finalized.has_value());
  ASSERT_EQ(finalized->size(), 1U);
  EXPECT_EQ(finalized->front().status, WindowResultStatus::kFinalized);

  auto correction = view->apply_committed(SourcePosition{tablet, wal, 3U},
                                          MaterializedViewInput{{1U, 1, 1U, 30.0, 2.0}, false});
  ASSERT_TRUE(correction.has_value());
  EXPECT_EQ(correction->front().status, WindowResultStatus::kCorrected);
  EXPECT_DOUBLE_EQ(correction->front().value.sum, 50.0);
  EXPECT_EQ(view->applied_position().record_sequence, 3U);

  auto checkpoint = view->checkpoint();
  ASSERT_TRUE(checkpoint.has_value()) << checkpoint.error().to_string();
  auto restored = WindowedMaterializedView::restore(std::move(*checkpoint));
  ASSERT_TRUE(restored.has_value()) << restored.error().to_string();
  EXPECT_EQ(restored->applied_position(), view->applied_position());
  EXPECT_EQ(restored->watermark(), view->watermark());
  EXPECT_EQ(restored->retained_rows(), view->retained_rows());
  EXPECT_EQ(restored->open_windows(), view->open_windows());

  const SourcePosition fourth{tablet, wal, 4U};
  const MaterializedViewInput tombstone{{1U, 1, 1U, 0.0, 1.0}, true};
  auto original_change = view->apply_committed(fourth, tombstone);
  auto restored_change = restored->apply_committed(fourth, tombstone);
  ASSERT_TRUE(original_change.has_value());
  ASSERT_TRUE(restored_change.has_value());
  EXPECT_EQ(*restored_change, *original_change);
}

TEST(MaterializedViewTest, SlidingWindowUpdatesEveryOverlappingWindow) {
  const auto tablet = tablet_id();
  const auto wal = wal_id();
  auto view = WindowedMaterializedView::create(tablet, wal, WindowDefinition{10, 5, 0, 16U, 16U});
  ASSERT_TRUE(view.has_value());
  auto changes = view->apply_committed(SourcePosition{tablet, wal, 1U},
                                       MaterializedViewInput{{1U, 7, 1U, 5.0, 1.0}, false});
  ASSERT_TRUE(changes.has_value());
  ASSERT_EQ(changes->size(), 2U);
  EXPECT_EQ((*changes)[0].window, (WindowKey{0, 10}));
  EXPECT_EQ((*changes)[1].window, (WindowKey{5, 15}));
}

TEST(MaterializedViewTest, RejectsCheckpointWhoseWindowRowsDisagree) {
  const auto tablet = tablet_id();
  const auto wal = wal_id();
  auto view = WindowedMaterializedView::create(tablet, wal, WindowDefinition{10, 10, 0, 16U, 16U});
  ASSERT_TRUE(view.has_value());
  ASSERT_TRUE(view->apply_committed(SourcePosition{tablet, wal, 1U},
                                    MaterializedViewInput{{1U, 1, 1U, 5.0, 1.0}, false})
                  .has_value());
  auto checkpoint = view->checkpoint();
  ASSERT_TRUE(checkpoint.has_value());
  checkpoint->windows.front().aggregate.rows.clear();
  EXPECT_EQ(WindowedMaterializedView::restore(std::move(*checkpoint)).error().code(),
            common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::live
