#ifndef CHRONOS_CSEG_PART_CODEC_HPP_
#define CHRONOS_CSEG_PART_CODEC_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/cseg/metadata_codec.hpp"
#include "chronos/cseg/page_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>

namespace chronos::cseg {

struct CsegPartEncodeInput {
  PartId part_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  std::uint64_t row_count{};
  std::uint32_t event_time_column_ordinal{};
  std::uint32_t ordering_column_count{};
  std::int64_t minimum_event_time{};
  std::int64_t maximum_event_time{};
  std::span<const CsegColumnDescriptor> columns;
  std::span<const CsegGranuleDescriptor> granules;
  std::span<const EncodedCsegPage> pages;
};

// Owns exactly one canonical CSEG v1 file image. The bytes are immutable through this interface
// and remain valid across moves of the owner.
class EncodedCsegPart {
public:
  EncodedCsegPart() = delete;
  EncodedCsegPart(const EncodedCsegPart&) = delete;
  EncodedCsegPart& operator=(const EncodedCsegPart&) = delete;
  EncodedCsegPart(EncodedCsegPart&&) noexcept = default;
  EncodedCsegPart& operator=(EncodedCsegPart&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;
  [[nodiscard]] std::size_t size() const noexcept;

private:
  explicit EncodedCsegPart(std::vector<std::byte> bytes) noexcept;

  std::vector<std::byte> bytes_;

  friend common::Result<EncodedCsegPart> encode_cseg_v1_part(const CsegPartEncodeInput& input);
};

// Composes metadata followed by pages in granule-major, stored-column-major order. Pages must have
// been produced by encode_cseg_v1_page(); metadata validation enforces their row/type shape.
[[nodiscard]] common::Result<EncodedCsegPart> encode_cseg_v1_part(const CsegPartEncodeInput& input);

using CsegPartDecodeErrorKind = CsegMetadataDecodeErrorKind;
using CsegPartDecodeError = CsegMetadataDecodeError;

// Borrows the complete immutable encoded part. Construction validates metadata, every stored-page
// CRC and PLAIN payload, and every alignment byte. It does not perform catalog binding, system-row
// semantic validation, event-time recomputation, or global sort validation; those are separate
// acceptance stages. The encoded owner must outlive this view and decoded raw pages obtained from
// it.
class DecodedCsegPartView {
public:
  DecodedCsegPartView() = delete;

  [[nodiscard]] constexpr const DecodedCsegMetadataView& metadata() const noexcept {
    return metadata_;
  }
  [[nodiscard]] constexpr common::ByteView encoded_part() const noexcept {
    return encoded_part_;
  }
  [[nodiscard]] common::ByteView stored_page(std::size_t page_index) const noexcept;
  [[nodiscard]] common::Result<DecodedCsegPage> decode_page(std::size_t page_index) const;

private:
  DecodedCsegPartView(DecodedCsegMetadataView metadata, common::ByteView encoded_part) noexcept;

  DecodedCsegMetadataView metadata_;
  common::ByteView encoded_part_;

  friend std::expected<DecodedCsegPartView, CsegPartDecodeError>
  decode_cseg_v1_part_prefix(common::ByteView bytes, CsegMetadataDecodeLimits limits);
};

using CsegPartDecodeResult = std::expected<DecodedCsegPartView, CsegPartDecodeError>;

// Decodes one structurally and physically valid CSEG part from the start of bytes. A valid short
// prefix reports kIncomplete with the exact next required size; following bytes are left to the
// caller. All pages are checked before success, so this is not a projected read API.
[[nodiscard]] CsegPartDecodeResult decode_cseg_v1_part_prefix(common::ByteView bytes,
                                                              CsegMetadataDecodeLimits limits = {});

// Requires exactly one complete CSEG part and rejects trailing bytes.
[[nodiscard]] CsegPartDecodeResult decode_cseg_v1_part_exact(common::ByteView bytes,
                                                             CsegMetadataDecodeLimits limits = {});

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_PART_CODEC_HPP_
