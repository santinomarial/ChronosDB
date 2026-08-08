#include "chronos/common/status.hpp"
#include "chronos/cseg/compression.hpp"
#include "chronos/cseg/format.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

namespace chronos::cseg {
namespace {

[[nodiscard]] std::vector<std::byte> provider_frame(const common::ByteView input,
                                                    const bool checksum) {
  ZSTD_CCtx* context = ZSTD_createCCtx();
  EXPECT_NE(context, nullptr);
  EXPECT_FALSE(ZSTD_isError(ZSTD_CCtx_setParameter(context, ZSTD_c_compressionLevel, 3)));
  EXPECT_FALSE(ZSTD_isError(ZSTD_CCtx_setParameter(context, ZSTD_c_contentSizeFlag, 1)));
  EXPECT_FALSE(
      ZSTD_isError(ZSTD_CCtx_setParameter(context, ZSTD_c_checksumFlag, checksum ? 1 : 0)));
  EXPECT_FALSE(ZSTD_isError(ZSTD_CCtx_setParameter(context, ZSTD_c_dictIDFlag, 0)));
  EXPECT_FALSE(ZSTD_isError(ZSTD_CCtx_setParameter(context, ZSTD_c_nbWorkers, 0)));
  std::vector<std::byte> output(ZSTD_compressBound(input.size()));
  const std::size_t size =
      ZSTD_compress2(context, output.data(), output.size(), input.data(), input.size());
  EXPECT_FALSE(ZSTD_isError(size));
  ZSTD_freeCCtx(context);
  output.resize(size);
  return output;
}

TEST(CsegCompressionTest, NoneCopiesExactBytesAndRequiresExactDecodedLength) {
  const std::vector<std::byte> input{std::byte{0x00}, std::byte{0x7f}, std::byte{0xff}};
  const common::Result<StoredPage> stored = compress_cseg_page_v1(input, PageCompression::kNone);
  ASSERT_TRUE(stored.has_value());
  EXPECT_EQ(stored->compression(), PageCompression::kNone);
  EXPECT_TRUE(std::ranges::equal(stored->bytes(), input));

  const auto decoded =
      decompress_cseg_page_v1(stored->bytes(), stored->compression(), input.size());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(*decoded, input);
  const auto wrong =
      decompress_cseg_page_v1(stored->bytes(), stored->compression(), input.size() + 1U);
  ASSERT_FALSE(wrong.has_value());
  EXPECT_EQ(wrong.error().code(), common::StatusCode::kCorruption);
}

TEST(CsegCompressionTest, ZstdUsesDeterministicCanonicalFramePropertiesAndRoundTrips) {
  const std::vector<std::byte> input(16'384U, std::byte{0x41});
  const common::Result<StoredPage> first = compress_cseg_page_v1(input, PageCompression::kZstd);
  const common::Result<StoredPage> second = compress_cseg_page_v1(input, PageCompression::kZstd);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  ASSERT_EQ(first->compression(), PageCompression::kZstd);
  EXPECT_LT(first->size(), input.size());
  EXPECT_TRUE(std::ranges::equal(first->bytes(), second->bytes()));

  ZSTD_frameHeader header{};
  ASSERT_EQ(ZSTD_getFrameHeader(&header, first->bytes().data(), first->size()), 0U);
  EXPECT_EQ(header.frameType, ZSTD_frame);
  EXPECT_EQ(header.frameContentSize, input.size());
  EXPECT_LE(header.windowSize, format::kMaximumZstdWindowSize);
  EXPECT_EQ(header.dictID, 0U);
  EXPECT_EQ(header.checksumFlag, 1U);

  const auto decoded = decompress_cseg_page_v1(first->bytes(), first->compression(), input.size());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(*decoded, input);
}

TEST(CsegCompressionTest, ZstdFallsBackToRawWhenTheFrameIsNotSmaller) {
  const std::vector<std::byte> input{std::byte{0x12}};
  const common::Result<StoredPage> stored = compress_cseg_page_v1(input, PageCompression::kZstd);
  ASSERT_TRUE(stored.has_value());
  EXPECT_EQ(stored->compression(), PageCompression::kNone);
  EXPECT_TRUE(std::ranges::equal(stored->bytes(), input));
}

TEST(CsegCompressionTest, RejectsMissingChecksumTrailingFrameBytesAndCorruptChecksum) {
  const std::vector<std::byte> input(4'096U, std::byte{0x55});
  const std::vector<std::byte> without_checksum = provider_frame(input, false);
  const auto missing =
      decompress_cseg_page_v1(without_checksum, PageCompression::kZstd, input.size());
  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().code(), common::StatusCode::kCorruption);

  const StoredPage canonical = compress_cseg_page_v1(input, PageCompression::kZstd).value();
  std::vector<std::byte> trailing{canonical.bytes().begin(), canonical.bytes().end()};
  trailing.push_back(std::byte{0U});
  const auto extra = decompress_cseg_page_v1(trailing, PageCompression::kZstd, input.size());
  ASSERT_FALSE(extra.has_value());
  EXPECT_EQ(extra.error().code(), common::StatusCode::kCorruption);

  std::vector<std::byte> corrupt{canonical.bytes().begin(), canonical.bytes().end()};
  corrupt.back() ^= std::byte{1U};
  const auto checksum = decompress_cseg_page_v1(corrupt, PageCompression::kZstd, input.size());
  ASSERT_FALSE(checksum.has_value());
  EXPECT_EQ(checksum.error().code(), common::StatusCode::kCorruption);
}

TEST(CsegCompressionTest, EnforcesLimitsBeforeReadingOrAllocating) {
  std::byte sentinel{0U};
  const common::ByteView oversized_uncompressed{&sentinel,
                                                format::kMaximumUncompressedPageLength + 1U};
  const auto encode = compress_cseg_page_v1(oversized_uncompressed, PageCompression::kNone);
  ASSERT_FALSE(encode.has_value());
  EXPECT_EQ(encode.error().code(), common::StatusCode::kResourceExhausted);

  const common::ByteView oversized_stored{&sentinel, format::kMaximumStoredPageLength + 1U};
  const auto stored = decompress_cseg_page_v1(oversized_stored, PageCompression::kZstd, 1U);
  ASSERT_FALSE(stored.has_value());
  EXPECT_EQ(stored.error().code(), common::StatusCode::kResourceExhausted);

  const auto output =
      decompress_cseg_page_v1(common::ByteView{&sentinel, 1U}, PageCompression::kNone,
                              format::kMaximumUncompressedPageLength + 1U);
  ASSERT_FALSE(output.has_value());
  EXPECT_EQ(output.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(CsegCompressionTest, RejectsEmptyInputs) {
  const auto empty = compress_cseg_page_v1({}, PageCompression::kNone);
  ASSERT_FALSE(empty.has_value());
  EXPECT_EQ(empty.error().code(), common::StatusCode::kInvalidArgument);

  const auto stored = decompress_cseg_page_v1({}, PageCompression::kNone, 1U);
  ASSERT_FALSE(stored.has_value());
  EXPECT_EQ(stored.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::cseg
