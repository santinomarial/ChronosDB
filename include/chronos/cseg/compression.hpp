#ifndef CHRONOS_CSEG_COMPRESSION_HPP_
#define CHRONOS_CSEG_COMPRESSION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/cseg/format.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::cseg {

// The frozen CSEG page descriptor stores this code in a 16-bit field.
// NOLINTNEXTLINE(performance-enum-size)
enum class PageCompression : std::uint16_t {
  kNone = format::kNoCompression,
  kZstd = format::kZstdCompression,
};

class StoredPage {
public:
  StoredPage() = delete;

  [[nodiscard]] constexpr PageCompression compression() const noexcept {
    return compression_;
  }
  [[nodiscard]] common::ByteView bytes() const noexcept;
  [[nodiscard]] std::size_t size() const noexcept;

private:
  StoredPage(PageCompression compression, std::vector<std::byte> bytes) noexcept;

  PageCompression compression_;
  std::vector<std::byte> bytes_;

  friend common::Result<StoredPage> compress_cseg_page_v1(common::ByteView page,
                                                          PageCompression policy);
};

// NONE copies the input. ZSTD uses the canonical v1 settings and falls back to NONE unless the
// complete frame is smaller. Inputs are bounded by the frozen uncompressed-page limit.
[[nodiscard]] common::Result<StoredPage> compress_cseg_page_v1(common::ByteView page,
                                                               PageCompression policy);

// Returns exactly expected_uncompressed_length owned bytes. ZSTD framing, content size, checksum,
// dictionary absence, window, single-frame consumption, and canonical size reduction are checked
// before or during bounded decompression.
[[nodiscard]] common::Result<std::vector<std::byte>>
decompress_cseg_page_v1(common::ByteView stored, PageCompression compression,
                        std::uint64_t expected_uncompressed_length);

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_COMPRESSION_HPP_
