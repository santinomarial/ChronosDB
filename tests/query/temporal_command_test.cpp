#include "chronos/common/crc32c.hpp"
#include "chronos/query/temporal_command.hpp"
#include "columnar/columnar_test_support.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <ranges>
#include <vector>

namespace chronos::query {
namespace {

// Generated independently from Temporal Mutation Command v1 with Python struct packing and a
// standalone CRC32C implementation. The embedded 400-byte region is the independently reviewed
// Columnar Batch v1 golden fixture; these literals cover its command envelope/header and metadata
// suffix, while the full-payload CRC below binds all three regions.
constexpr std::array<std::uint8_t, 112U> kGoldenCommandPrefix{
    0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x43, 0x48, 0x52, 0x4e, 0x54, 0x4d, 0x50, 0x00, 0x01, 0x00, 0x00, 0x00, 0x60, 0x00, 0x00, 0x00,
    0x26, 0x02, 0x00, 0x00, 0x90, 0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x32, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0xc7, 0x4b, 0x67, 0x48, 0x78, 0x04, 0xcf, 0xcb, 0x89, 0x8a, 0x63, 0x0e, 0x00, 0x00, 0x00, 0x00};

constexpr std::array<std::uint8_t, 54U> kGoldenCommandSuffix{
    0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x6e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00,
    0x00, 0x01, 0x00, 0x00, 0x00, 0xc8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xdc,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x22, 0x6e, 0x4a, 0xaa};

template <std::size_t Size>
[[nodiscard]] std::array<std::byte, Size> as_bytes(const std::array<std::uint8_t, Size>& values) {
  std::array<std::byte, Size> bytes{};
  std::ranges::transform(values, bytes.begin(),
                         [](const std::uint8_t value) { return static_cast<std::byte>(value); });
  return bytes;
}

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
  ASSERT_EQ(encoded->bytes().size(), 566U);
  EXPECT_TRUE(std::ranges::equal(encoded->bytes().first(kGoldenCommandPrefix.size()),
                                 as_bytes(kGoldenCommandPrefix)));
  EXPECT_TRUE(std::ranges::equal(encoded->bytes().last(kGoldenCommandSuffix.size()),
                                 as_bytes(kGoldenCommandSuffix)));
  EXPECT_EQ(common::crc32c(encoded->bytes()), 0x6c55'5a2fU);
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
