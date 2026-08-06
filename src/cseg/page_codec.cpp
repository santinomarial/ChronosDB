#include "chronos/cseg/page_codec.hpp"

#include "chronos/common/crc32c.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/plain_page.hpp"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::cseg {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}

[[nodiscard]] common::Status validate_stored_input(const common::ByteView stored_bytes,
                                                   const CsegPageDescriptor& page) {
  if (page.stored_length == 0U || page.stored_length > format::kMaximumStoredPageLength ||
      page.stored_length != stored_bytes.size()) {
    return status(common::StatusCode::kCorruption,
                  "CSEG stored page size does not match a bounded nonzero descriptor");
  }
  if (page.uncompressed_length == 0U ||
      page.uncompressed_length > format::kMaximumUncompressedPageLength) {
    return status(common::StatusCode::kCorruption,
                  "CSEG uncompressed page length is outside the v1 bounds");
  }
  const auto compression_code = static_cast<std::uint16_t>(page.compression);
  if (compression_code == 0U) {
    return status(common::StatusCode::kCorruption, "CSEG compression code zero is invalid");
  }
  if (page.compression != PageCompression::kNone && page.compression != PageCompression::kZstd) {
    return status(common::StatusCode::kNotSupported, "CSEG compression code is unsupported");
  }
  return common::Status::ok();
}

} // namespace

EncodedCsegPage::EncodedCsegPage(StoredPage stored, const CsegPageMetadataInput metadata) noexcept
    : stored_(std::move(stored)), metadata_(metadata) {}

common::ByteView EncodedCsegPage::bytes() const noexcept {
  return stored_.bytes();
}

std::size_t EncodedCsegPage::size() const noexcept {
  return stored_.size();
}

PageCompression EncodedCsegPage::compression() const noexcept {
  return stored_.compression();
}

CsegPageMetadataInput EncodedCsegPage::metadata() const noexcept {
  return metadata_;
}

common::Result<EncodedCsegPage> encode_cseg_v1_page(const columnar::PhysicalColumnView& column,
                                                    const PageCompression policy) {
  if (policy != PageCompression::kNone && policy != PageCompression::kZstd) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "CSEG compression policy is unassigned"));
  }
  common::Result<EncodedCsegPlainPage> plain = encode_cseg_v1_plain_page(column);
  if (!plain.has_value()) {
    return common::make_unexpected(plain.error());
  }
  common::Result<StoredPage> stored = compress_cseg_page_v1(plain->bytes(), policy);
  if (!stored.has_value()) {
    return common::make_unexpected(stored.error());
  }
  const std::uint32_t checksum = common::crc32c(stored->bytes());
  const CsegPageMetadataInput metadata{.compression = stored->compression(),
                                       .row_count = column.row_count(),
                                       .null_count = column.null_count(),
                                       .stored_length = stored->size(),
                                       .uncompressed_length = plain->size(),
                                       .validity_length = plain->validity_length(),
                                       .offsets_length = plain->offsets_length(),
                                       .values_length = plain->values_length(),
                                       .page_crc32c = checksum};
  return EncodedCsegPage{std::move(*stored), metadata};
}

DecodedCsegPage::DecodedCsegPage(std::vector<std::byte> owned_uncompressed,
                                 const common::ByteView uncompressed_bytes,
                                 const columnar::PhysicalColumnView physical) noexcept
    : owned_uncompressed_(std::move(owned_uncompressed)), uncompressed_bytes_(uncompressed_bytes),
      physical_(physical) {}

common::Result<DecodedCsegPage> decode_cseg_v1_page(const common::ByteView stored_bytes,
                                                    const CsegColumnDescriptor& column,
                                                    const CsegPageDescriptor& page) {
  const common::Status input_status = validate_stored_input(stored_bytes, page);
  if (!input_status.is_ok()) {
    return common::make_unexpected(input_status);
  }
  if (common::crc32c(stored_bytes) != page.page_crc32c) {
    return common::make_unexpected(
        status(common::StatusCode::kCorruption, "CSEG stored page CRC32C mismatch"));
  }

  if (page.compression == PageCompression::kNone) {
    const common::Result<DecodedCsegPlainPageView> plain =
        decode_cseg_v1_plain_page(stored_bytes, column, page);
    if (!plain.has_value()) {
      return common::make_unexpected(plain.error());
    }
    return DecodedCsegPage{{}, stored_bytes, plain->physical()};
  }

  common::Result<std::vector<std::byte>> uncompressed =
      decompress_cseg_page_v1(stored_bytes, page.compression, page.uncompressed_length);
  if (!uncompressed.has_value()) {
    return common::make_unexpected(uncompressed.error());
  }
  const common::ByteView uncompressed_bytes = *uncompressed;
  const common::Result<DecodedCsegPlainPageView> plain =
      decode_cseg_v1_plain_page(uncompressed_bytes, column, page);
  if (!plain.has_value()) {
    return common::make_unexpected(plain.error());
  }
  return DecodedCsegPage{std::move(*uncompressed), uncompressed_bytes, plain->physical()};
}

} // namespace chronos::cseg
