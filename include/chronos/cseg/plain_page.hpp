#ifndef CHRONOS_CSEG_PLAIN_PAGE_HPP_
#define CHRONOS_CSEG_PLAIN_PAGE_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/cseg/metadata_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::cseg {

// Owns exactly one uncompressed PLAIN page payload: validity, offsets, then values, with no
// internal alignment. Compression and page/file framing are separate stages.
class EncodedCsegPlainPage {
public:
  EncodedCsegPlainPage() = delete;
  EncodedCsegPlainPage(const EncodedCsegPlainPage&) = delete;
  EncodedCsegPlainPage& operator=(const EncodedCsegPlainPage&) = delete;
  EncodedCsegPlainPage(EncodedCsegPlainPage&&) noexcept = default;
  EncodedCsegPlainPage& operator=(EncodedCsegPlainPage&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;
  [[nodiscard]] std::size_t size() const noexcept;
  [[nodiscard]] constexpr std::uint64_t validity_length() const noexcept {
    return validity_length_;
  }
  [[nodiscard]] constexpr std::uint64_t offsets_length() const noexcept {
    return offsets_length_;
  }
  [[nodiscard]] constexpr std::uint64_t values_length() const noexcept {
    return values_length_;
  }

private:
  EncodedCsegPlainPage(std::vector<std::byte> bytes, std::uint64_t validity_length,
                       std::uint64_t offsets_length, std::uint64_t values_length) noexcept;

  std::vector<std::byte> bytes_;
  std::uint64_t validity_length_;
  std::uint64_t offsets_length_;
  std::uint64_t values_length_;

  friend common::Result<EncodedCsegPlainPage>
  encode_cseg_v1_plain_page(const columnar::PhysicalColumnView& column);
};

// The physical view must already satisfy the shared Columnar Batch v1 buffer rules. Encoding is a
// deterministic exact copy and rejects a payload above the frozen CSEG page limit.
[[nodiscard]] common::Result<EncodedCsegPlainPage>
encode_cseg_v1_plain_page(const columnar::PhysicalColumnView& column);

// Borrows one complete immutable uncompressed page payload. The payload owner must outlive this
// value and every physical cell view obtained from it.
class DecodedCsegPlainPageView {
public:
  DecodedCsegPlainPageView() = delete;

  [[nodiscard]] constexpr const columnar::PhysicalColumnView& physical() const noexcept {
    return physical_;
  }
  [[nodiscard]] constexpr common::ByteView encoded_bytes() const noexcept {
    return encoded_bytes_;
  }

private:
  DecodedCsegPlainPageView(columnar::PhysicalColumnView physical,
                           common::ByteView encoded_bytes) noexcept;

  columnar::PhysicalColumnView physical_;
  common::ByteView encoded_bytes_;

  friend common::Result<DecodedCsegPlainPageView>
  decode_cseg_v1_plain_page(common::ByteView uncompressed_payload,
                            const CsegColumnDescriptor& column, const CsegPageDescriptor& page);
};

// Validates an already decompressed payload against its checksum-protected descriptors. Page CRC
// verification and decompression must occur first; failures here classify as corruption.
[[nodiscard]] common::Result<DecodedCsegPlainPageView>
decode_cseg_v1_plain_page(common::ByteView uncompressed_payload, const CsegColumnDescriptor& column,
                          const CsegPageDescriptor& page);

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_PLAIN_PAGE_HPP_
