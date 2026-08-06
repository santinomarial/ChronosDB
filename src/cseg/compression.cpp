#include "chronos/cseg/compression.hpp"

#include "chronos/cseg/format.hpp"

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

namespace chronos::cseg {
namespace {

struct CctxDeleter {
  void operator()(ZSTD_CCtx* context) const noexcept {
    ZSTD_freeCCtx(context);
  }
};

struct DctxDeleter {
  void operator()(ZSTD_DCtx* context) const noexcept {
    ZSTD_freeDCtx(context);
  }
};

using Cctx = std::unique_ptr<ZSTD_CCtx, CctxDeleter>;
using Dctx = std::unique_ptr<ZSTD_DCtx, DctxDeleter>;

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status exhausted(const std::string_view message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::string{message}};
}

[[nodiscard]] common::Status corrupt(const std::string_view message) {
  return common::Status{common::StatusCode::kCorruption, std::string{message}};
}

[[nodiscard]] common::Status provider_error(const std::string_view operation,
                                            const std::size_t code) {
  return common::Status{common::StatusCode::kInternal,
                        std::string{operation} + ": " + ZSTD_getErrorName(code)};
}

[[nodiscard]] common::Status set_parameter(ZSTD_CCtx* context, const ZSTD_cParameter parameter,
                                           const int value) {
  const std::size_t result = ZSTD_CCtx_setParameter(context, parameter, value);
  return ZSTD_isError(result) != 0 ? provider_error("Zstandard parameter setup failed", result)
                                   : common::Status::ok();
}

} // namespace

StoredPage::StoredPage(const PageCompression compression, std::vector<std::byte> bytes) noexcept
    : compression_(compression), bytes_(std::move(bytes)) {}

common::ByteView StoredPage::bytes() const noexcept {
  return bytes_;
}

std::size_t StoredPage::size() const noexcept {
  return bytes_.size();
}

common::Result<StoredPage> compress_cseg_page_v1(const common::ByteView page,
                                                 const PageCompression policy) {
  if (page.empty()) {
    return common::make_unexpected(invalid("CSEG uncompressed page must be nonempty"));
  }
  if (page.size() > format::kMaximumUncompressedPageLength) {
    return common::make_unexpected(exhausted("CSEG uncompressed page exceeds the v1 limit"));
  }
  if (policy == PageCompression::kNone) {
    return StoredPage{PageCompression::kNone, std::vector<std::byte>{page.begin(), page.end()}};
  }
  if (policy != PageCompression::kZstd) {
    return common::make_unexpected(invalid("CSEG compression policy is unassigned"));
  }

  Cctx context{ZSTD_createCCtx()};
  if (!context) {
    return common::make_unexpected(exhausted("Zstandard compression context allocation failed"));
  }
  for (const auto [parameter, value] :
       {std::pair{ZSTD_c_compressionLevel, 3}, std::pair{ZSTD_c_contentSizeFlag, 1},
        std::pair{ZSTD_c_checksumFlag, 1}, std::pair{ZSTD_c_dictIDFlag, 0},
        std::pair{ZSTD_c_nbWorkers, 0}}) {
    const common::Status status = set_parameter(context.get(), parameter, value);
    if (!status.is_ok()) {
      return common::make_unexpected(status);
    }
  }

  std::vector<std::byte> output(ZSTD_compressBound(page.size()));
  const std::size_t compressed =
      ZSTD_compress2(context.get(), output.data(), output.size(), page.data(), page.size());
  if (ZSTD_isError(compressed) != 0) {
    return common::make_unexpected(provider_error("Zstandard compression failed", compressed));
  }
  if (compressed >= page.size()) {
    return StoredPage{PageCompression::kNone, std::vector<std::byte>{page.begin(), page.end()}};
  }
  output.resize(compressed);
  return StoredPage{PageCompression::kZstd, std::move(output)};
}

common::Result<std::vector<std::byte>>
decompress_cseg_page_v1(const common::ByteView stored, const PageCompression compression,
                        const std::uint64_t expected_uncompressed_length) {
  if (expected_uncompressed_length == 0U) {
    return common::make_unexpected(invalid("CSEG expected page length must be nonzero"));
  }
  if (expected_uncompressed_length > format::kMaximumUncompressedPageLength) {
    return common::make_unexpected(exhausted("CSEG expected page length exceeds the v1 limit"));
  }
  if (stored.empty()) {
    return common::make_unexpected(corrupt("CSEG stored page is empty"));
  }
  if (stored.size() > format::kMaximumStoredPageLength) {
    return common::make_unexpected(exhausted("CSEG stored page exceeds the v1 limit"));
  }
  if (compression == PageCompression::kNone) {
    if (stored.size() != expected_uncompressed_length) {
      return common::make_unexpected(corrupt("raw CSEG page length does not match its descriptor"));
    }
    return std::vector<std::byte>{stored.begin(), stored.end()};
  }
  if (compression != PageCompression::kZstd) {
    return common::make_unexpected(invalid("CSEG compression code is unassigned"));
  }
  if (stored.size() >= expected_uncompressed_length) {
    return common::make_unexpected(corrupt("Zstandard CSEG page is not canonically smaller"));
  }

  ZSTD_FrameHeader header{};
  const std::size_t header_result = ZSTD_getFrameHeader(&header, stored.data(), stored.size());
  if (ZSTD_isError(header_result) != 0 || header_result != 0U) {
    return common::make_unexpected(corrupt("CSEG Zstandard frame header is invalid or incomplete"));
  }
  if (header.frameType != ZSTD_frame || header.frameContentSize != expected_uncompressed_length ||
      header.windowSize > format::kMaximumZstdWindowSize || header.dictID != 0U ||
      header.checksumFlag == 0U) {
    return common::make_unexpected(corrupt("CSEG Zstandard frame properties are noncanonical"));
  }
  const std::size_t frame_size = ZSTD_findFrameCompressedSize(stored.data(), stored.size());
  if (ZSTD_isError(frame_size) != 0 || frame_size != stored.size()) {
    return common::make_unexpected(corrupt("CSEG Zstandard page is not exactly one frame"));
  }

  Dctx context{ZSTD_createDCtx()};
  if (!context) {
    return common::make_unexpected(exhausted("Zstandard decompression context allocation failed"));
  }
  std::vector<std::byte> output(static_cast<std::size_t>(expected_uncompressed_length));
  const std::size_t decoded = ZSTD_decompressDCtx(context.get(), output.data(), output.size(),
                                                  stored.data(), stored.size());
  if (ZSTD_isError(decoded) != 0 || decoded != output.size()) {
    return common::make_unexpected(corrupt("CSEG Zstandard page decompression failed"));
  }
  return output;
}

} // namespace chronos::cseg
