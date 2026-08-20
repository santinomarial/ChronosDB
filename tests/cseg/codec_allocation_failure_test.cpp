#include "chronos/common/status.hpp"
#include "chronos/cseg/compression.hpp"
#include "chronos/cseg/metadata_codec.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "cseg/cseg_test_fixture.hpp"
#include "support/failing_allocator.hpp"

#include <algorithm>
#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::cseg {
namespace {

template <typename Operation>
[[nodiscard]] auto run_with_allocation_failure(const std::size_t fail_after, std::size_t& observed,
                                               Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    observed = failure.observed_allocations();
    failure.disable();
  }
  return std::move(*result);
}

template <typename Operation> void expect_resource_exhaustion_until_success(Operation&& operation) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto decoded = run_with_allocation_failure(fail_after, observed, operation);
    EXPECT_GT(observed, 0U);
    if (decoded.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(decoded.error().kind(), CsegMetadataDecodeErrorKind::kResourceLimit);
    EXPECT_EQ(decoded.error().status().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

template <typename Operation>
void expect_resource_exhaustion_until_corruption(Operation&& operation) {
  bool reached_corruption = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto decoded = run_with_allocation_failure(fail_after, observed, operation);
    EXPECT_GT(observed, 0U);
    ASSERT_FALSE(decoded.has_value());
    if (decoded.error().kind() == CsegMetadataDecodeErrorKind::kCorruption) {
      reached_corruption = true;
      break;
    }
    EXPECT_EQ(decoded.error().kind(), CsegMetadataDecodeErrorKind::kResourceLimit);
    EXPECT_EQ(decoded.error().status().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_corruption);
}

TEST(CsegCodecAllocationFailureTest, MetadataDecodeClassifiesEveryV1AndV2AllocationFailure) {
  const EncodedCsegPart v1 = test::make_valid_part();
  const auto v1_metadata = decode_cseg_v1_metadata_prefix(v1.bytes());
  ASSERT_TRUE(v1_metadata.has_value());
  const common::ByteView v1_bytes = v1.bytes().first(v1_metadata->encoded_metadata().size());
  expect_resource_exhaustion_until_success([&] { return decode_cseg_v1_metadata_exact(v1_bytes); });

  const EncodedCsegPart v2 = test::make_valid_temporal_part();
  const auto v2_metadata = decode_cseg_v2_temporal_metadata_prefix(v2.bytes());
  ASSERT_TRUE(v2_metadata.has_value());
  const common::ByteView v2_bytes = v2.bytes().first(v2_metadata->encoded_metadata().size());
  expect_resource_exhaustion_until_success(
      [&] { return decode_cseg_v2_temporal_metadata_exact(v2_bytes); });
}

TEST(CsegCodecAllocationFailureTest, RawPartDecodeClassifiesEveryV1AndV2AllocationFailure) {
  const EncodedCsegPart v1 = test::make_valid_part();
  expect_resource_exhaustion_until_success([&] { return decode_cseg_v1_part_exact(v1.bytes()); });

  const EncodedCsegPart v2 = test::make_valid_temporal_part();
  expect_resource_exhaustion_until_success(
      [&] { return decode_cseg_v2_temporal_part_exact(v2.bytes()); });
}

TEST(CsegCodecAllocationFailureTest, CompressedPartDecodeClassifiesPageOutputAllocationFailure) {
  const EncodedCsegPart encoded =
      test::make_valid_part_with_rows(1'024U, 1'024U, PageCompression::kZstd);
  const auto canonical = decode_cseg_v1_part_exact(encoded.bytes());
  ASSERT_TRUE(canonical.has_value());
  ASSERT_TRUE(
      std::ranges::any_of(canonical->metadata().pages(), [](const CsegPageDescriptor& page) {
        return page.compression == PageCompression::kZstd;
      }));

  expect_resource_exhaustion_until_success(
      [&] { return decode_cseg_v1_part_exact(encoded.bytes()); });
}

TEST(CsegCodecAllocationFailureTest, ExactDecodersClassifyTrailingSuffixAllocationFailures) {
  const EncodedCsegPart v1 = test::make_valid_part();
  const auto v1_metadata = decode_cseg_v1_metadata_prefix(v1.bytes());
  ASSERT_TRUE(v1_metadata.has_value());
  std::vector<std::byte> v1_metadata_suffix(
      v1.bytes().begin(),
      v1.bytes().begin() + static_cast<std::ptrdiff_t>(v1_metadata->encoded_metadata().size()));
  v1_metadata_suffix.push_back(std::byte{0U});
  expect_resource_exhaustion_until_corruption(
      [&] { return decode_cseg_v1_metadata_exact(v1_metadata_suffix); });

  const EncodedCsegPart v2 = test::make_valid_temporal_part();
  const auto v2_metadata = decode_cseg_v2_temporal_metadata_prefix(v2.bytes());
  ASSERT_TRUE(v2_metadata.has_value());
  std::vector<std::byte> v2_metadata_suffix(
      v2.bytes().begin(),
      v2.bytes().begin() + static_cast<std::ptrdiff_t>(v2_metadata->encoded_metadata().size()));
  v2_metadata_suffix.push_back(std::byte{0U});
  expect_resource_exhaustion_until_corruption(
      [&] { return decode_cseg_v2_temporal_metadata_exact(v2_metadata_suffix); });

  std::vector<std::byte> v1_part_suffix{v1.bytes().begin(), v1.bytes().end()};
  v1_part_suffix.push_back(std::byte{0U});
  expect_resource_exhaustion_until_corruption(
      [&] { return decode_cseg_v1_part_exact(v1_part_suffix); });

  std::vector<std::byte> v2_part_suffix{v2.bytes().begin(), v2.bytes().end()};
  v2_part_suffix.push_back(std::byte{0U});
  expect_resource_exhaustion_until_corruption(
      [&] { return decode_cseg_v2_temporal_part_exact(v2_part_suffix); });
}

} // namespace
} // namespace chronos::cseg
