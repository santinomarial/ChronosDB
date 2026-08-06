#ifndef CHRONOS_CSEG_PAGE_CODEC_HPP_
#define CHRONOS_CSEG_PAGE_CODEC_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/cseg/compression.hpp"
#include "chronos/cseg/metadata_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::cseg {

// Owns exactly one stored CSEG page and the descriptor fields derived from its canonical physical
// input. File offsets and descriptor ordinals belong to metadata/file layout planning.
class EncodedCsegPage {
public:
  EncodedCsegPage() = delete;
  EncodedCsegPage(const EncodedCsegPage&) = delete;
  EncodedCsegPage& operator=(const EncodedCsegPage&) = delete;
  EncodedCsegPage(EncodedCsegPage&&) noexcept = default;
  EncodedCsegPage& operator=(EncodedCsegPage&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;
  [[nodiscard]] std::size_t size() const noexcept;
  [[nodiscard]] PageCompression compression() const noexcept;
  [[nodiscard]] constexpr std::uint32_t page_crc32c() const noexcept {
    return metadata_.page_crc32c;
  }
  [[nodiscard]] CsegPageMetadataInput metadata() const noexcept;

private:
  EncodedCsegPage(StoredPage stored, CsegPageMetadataInput metadata) noexcept;

  StoredPage stored_;
  CsegPageMetadataInput metadata_;

  friend common::Result<EncodedCsegPage>
  encode_cseg_v1_page(const columnar::PhysicalColumnView& column, PageCompression policy);
};

// Deterministically encodes PLAIN, applies the explicit compression policy, and computes CRC32C
// over exactly the resulting stored bytes.
[[nodiscard]] common::Result<EncodedCsegPage>
encode_cseg_v1_page(const columnar::PhysicalColumnView& column, PageCompression policy);

// A validated physical page. NONE pages borrow stored_bytes passed to decode_cseg_v1_page(); ZSTD
// pages own their bounded decompressed bytes. The stored-byte owner must therefore outlive a raw
// result. This value is move-only because its physical view may refer to its owned vector.
class DecodedCsegPage {
public:
  DecodedCsegPage() = delete;
  DecodedCsegPage(const DecodedCsegPage&) = delete;
  DecodedCsegPage& operator=(const DecodedCsegPage&) = delete;
  DecodedCsegPage(DecodedCsegPage&&) noexcept = default;
  DecodedCsegPage& operator=(DecodedCsegPage&&) = delete;

  [[nodiscard]] constexpr bool owns_uncompressed_bytes() const noexcept {
    return !owned_uncompressed_.empty();
  }
  [[nodiscard]] constexpr common::ByteView uncompressed_bytes() const noexcept {
    return uncompressed_bytes_;
  }
  [[nodiscard]] constexpr const columnar::PhysicalColumnView& physical() const noexcept {
    return physical_;
  }

private:
  DecodedCsegPage(std::vector<std::byte> owned_uncompressed, common::ByteView uncompressed_bytes,
                  columnar::PhysicalColumnView physical) noexcept;

  std::vector<std::byte> owned_uncompressed_;
  common::ByteView uncompressed_bytes_;
  columnar::PhysicalColumnView physical_;

  friend common::Result<DecodedCsegPage> decode_cseg_v1_page(common::ByteView stored_bytes,
                                                             const CsegColumnDescriptor& column,
                                                             const CsegPageDescriptor& page);
};

// The column and page descriptors must come from integrity-verified metadata. This function still
// fail-closes on forged typed inputs: it validates fixed bounds and assigned compression, verifies
// the stored-byte CRC before provider entry, performs bounded canonical decompression, and finally
// validates the complete PLAIN payload.
[[nodiscard]] common::Result<DecodedCsegPage>
decode_cseg_v1_page(common::ByteView stored_bytes, const CsegColumnDescriptor& column,
                    const CsegPageDescriptor& page);

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_PAGE_CODEC_HPP_
