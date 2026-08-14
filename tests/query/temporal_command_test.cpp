#include "chronos/query/temporal_command.hpp"
#include "columnar/columnar_test_support.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::query {
namespace {

TEST(TemporalCommandTest, DefaultsToSixtyFourMebibytesOfMutationMetadata) {
  const TemporalCommandLimits limits;

  EXPECT_EQ(limits.maximum_metadata_bytes, std::size_t{64U} * 1024U * 1024U);
}

TEST(TemporalCommandTest, RoundTripsCanonicalBatchAndMutationMetadata) {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns());
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  std::vector<TemporalMutationDescriptor> descriptors{
      {{std::byte{1U}}, 100, 110, TemporalMutationKind::kOriginal},
      {{std::byte{2U}}, 200, 220, TemporalMutationKind::kCorrection}};
  auto encoded = encode_temporal_command_v1(*batch, descriptors, 1000);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_temporal_command_v1(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->batch().row_count(), 2U);
  EXPECT_EQ(decoded->batch().schema_id(), batch->schema().schema_id());
  EXPECT_EQ(decoded->system_commit_time_ns(), 1000);
  ASSERT_EQ(decoded->mutations().size(), 2U);
  EXPECT_EQ(decoded->mutations()[1].kind, TemporalMutationKind::kCorrection);
  EXPECT_EQ(decoded->mutations()[1].event_time_ns, 200);

  std::vector<std::byte> damaged{encoded->bytes().begin(), encoded->bytes().end()};
  damaged.back() ^= std::byte{1U};
  EXPECT_EQ(decode_temporal_command_v1(damaged).error().code(), common::StatusCode::kCorruption);

  descriptors[1].logical_identity = descriptors[0].logical_identity;
  EXPECT_EQ(encode_temporal_command_v1(*batch, descriptors, 1000).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(descriptors.size(), 2U);
}

} // namespace
} // namespace chronos::query
