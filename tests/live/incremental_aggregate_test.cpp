#include "chronos/live/incremental_aggregate.hpp"

#include <gtest/gtest.h>
#include <limits>

namespace chronos::live {
namespace {

TEST(IncrementalAggregateTest, MaintainsCountSumExtremaVwapOhlcAndWelfordState) {
  IncrementalAggregateSet state;
  ASSERT_TRUE(state.upsert(AggregateInput{1U, 100, 10U, 10.0, 2.0}).is_ok());
  ASSERT_TRUE(state.upsert(AggregateInput{2U, 200, 20U, 20.0, 1.0}).is_ok());

  auto value = state.snapshot();
  EXPECT_EQ(value.count, 2U);
  EXPECT_DOUBLE_EQ(value.sum, 30.0);
  ASSERT_TRUE(value.minimum.has_value());
  ASSERT_TRUE(value.maximum.has_value());
  ASSERT_TRUE(value.vwap.has_value());
  ASSERT_TRUE(value.ohlc.has_value());
  ASSERT_TRUE(value.variance_population.has_value());
  ASSERT_TRUE(value.variance_sample.has_value());
  EXPECT_DOUBLE_EQ(*value.minimum, 10.0);
  EXPECT_DOUBLE_EQ(*value.maximum, 20.0);
  EXPECT_NEAR(*value.vwap, 40.0 / 3.0, 1e-12);
  EXPECT_EQ(*value.ohlc, (OhlcValue{10.0, 20.0, 10.0, 20.0}));
  EXPECT_DOUBLE_EQ(*value.variance_population, 25.0);
  EXPECT_DOUBLE_EQ(*value.variance_sample, 50.0);

  ASSERT_TRUE(state.upsert(AggregateInput{1U, 100, 10U, 30.0, 2.0}).is_ok());
  value = state.snapshot();
  EXPECT_EQ(value.count, 2U);
  EXPECT_DOUBLE_EQ(value.sum, 50.0);
  EXPECT_EQ(*value.ohlc, (OhlcValue{30.0, 30.0, 20.0, 20.0}));
  EXPECT_NEAR(*value.vwap, 80.0 / 3.0, 1e-12);

  ASSERT_TRUE(state.erase(2U).is_ok());
  value = state.snapshot();
  EXPECT_EQ(value.count, 1U);
  EXPECT_DOUBLE_EQ(value.sum, 30.0);
  EXPECT_FALSE(value.variance_sample.has_value());
  EXPECT_DOUBLE_EQ(*value.variance_population, 0.0);

  auto checkpoint = state.checkpoint();
  ASSERT_TRUE(checkpoint.has_value()) << checkpoint.error().to_string();
  const IncrementalAggregateCheckpoint original_checkpoint = *checkpoint;
  auto restored = IncrementalAggregateSet::restore(*checkpoint);
  ASSERT_TRUE(restored.has_value()) << restored.error().to_string();
  EXPECT_EQ(*checkpoint, original_checkpoint);
  EXPECT_EQ(restored->snapshot(), state.snapshot());
  ASSERT_TRUE(restored->upsert(AggregateInput{3U, 300, 30U, 40.0, 1.0}).is_ok());
  ASSERT_TRUE(state.upsert(AggregateInput{3U, 300, 30U, 40.0, 1.0}).is_ok());
  EXPECT_EQ(restored->snapshot(), state.snapshot());
}

TEST(IncrementalAggregateTest, TombstoneIsIdempotent) {
  IncrementalAggregateSet state;
  EXPECT_TRUE(state.erase(99U).is_ok());
  EXPECT_EQ(state.snapshot().count, 0U);
  EXPECT_EQ(state.retained_rows(), 0U);
}

TEST(IncrementalAggregateTest, RejectsNonFiniteInputAndNonCanonicalCheckpoint) {
  IncrementalAggregateSet state;
  EXPECT_EQ(
      state.upsert(AggregateInput{1U, 1, 1U, std::numeric_limits<double>::quiet_NaN(), 1.0}).code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(state.upsert(AggregateInput{1U, 1, 0U, 1.0, 1.0}).code(),
            common::StatusCode::kInvalidArgument);
  IncrementalAggregateCheckpoint corrupt{.rows = {{2U, 1, 1U, 1.0, 1.0}, {1U, 2, 2U, 2.0, 1.0}},
                                         .count = 2U,
                                         .sum = 3.0,
                                         .weighted_sum = 3.0,
                                         .weight_sum = 2.0,
                                         .mean = 1.5,
                                         .m2 = 0.5};
  EXPECT_EQ(IncrementalAggregateSet::restore(corrupt).error().code(),
            common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::live
