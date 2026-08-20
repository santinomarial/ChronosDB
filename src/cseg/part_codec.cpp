#include "chronos/cseg/part_codec.hpp"

#include "chronos/cseg/layout.hpp"

#include <algorithm>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::cseg {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}

[[nodiscard]] CsegPartDecodeError incomplete(const std::string_view message,
                                             const std::uint64_t required_size) {
  return {CsegPartDecodeErrorKind::kIncomplete, status(common::StatusCode::kOutOfRange, message),
          required_size};
}

[[nodiscard]] CsegPartDecodeError corruption(const std::string_view message) {
  return {CsegPartDecodeErrorKind::kCorruption, status(common::StatusCode::kCorruption, message)};
}

[[nodiscard]] CsegPartDecodeError resource_limit(const std::string_view message) {
  return {CsegPartDecodeErrorKind::kResourceLimit,
          status(common::StatusCode::kResourceExhausted, message)};
}

[[nodiscard]] CsegPartDecodeError page_error(const common::Status& page_status) {
  switch (page_status.code()) {
  case common::StatusCode::kNotSupported:
    return {CsegPartDecodeErrorKind::kUnsupported, page_status};
  case common::StatusCode::kResourceExhausted:
    return {CsegPartDecodeErrorKind::kResourceLimit, page_status};
  default:
    return {CsegPartDecodeErrorKind::kCorruption,
            status(common::StatusCode::kCorruption, page_status.message())};
  }
}

[[nodiscard]] bool all_zero(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0}; });
}

} // namespace

EncodedCsegPart::EncodedCsegPart(std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedCsegPart::bytes() const noexcept {
  return bytes_;
}

std::size_t EncodedCsegPart::size() const noexcept {
  return bytes_.size();
}

std::size_t EncodedCsegPart::retained_buffer_bytes() const noexcept {
  return bytes_.capacity();
}

common::Result<EncodedCsegPart> encode_cseg_part(const CsegPartEncodeInput& input,
                                                 const std::uint16_t format_major) {
  std::vector<CsegPageMetadataInput> page_metadata;
  page_metadata.reserve(input.pages.size());
  for (const EncodedCsegPage& page : input.pages) {
    page_metadata.push_back(page.metadata());
  }

  const CsegMetadataEncodeInput metadata_input{
      .part_id = input.part_id,
      .table_id = input.table_id,
      .tablet_id = input.tablet_id,
      .schema_id = input.schema_id,
      .schema_version = input.schema_version,
      .row_count = input.row_count,
      .event_time_column_ordinal = input.event_time_column_ordinal,
      .ordering_column_count = input.ordering_column_count,
      .minimum_event_time = input.minimum_event_time,
      .maximum_event_time = input.maximum_event_time,
      .columns = input.columns,
      .granules = input.granules,
      .pages = page_metadata,
  };
  const common::Result<EncodedCsegMetadata> metadata =
      format_major == format::kFormatMajor ? encode_cseg_v1_metadata(metadata_input)
                                           : encode_cseg_v2_temporal_metadata(metadata_input);
  if (!metadata.has_value()) {
    return common::make_unexpected(metadata.error());
  }
  if (metadata->total_length() > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "CSEG part does not fit this platform"));
  }

  std::vector<std::byte> storage(static_cast<std::size_t>(metadata->total_length()), std::byte{0});
  std::ranges::copy(metadata->bytes(), storage.begin());
  const common::MutableByteView output{storage};
  std::uint64_t cursor = metadata->size();
  for (const EncodedCsegPage& page : input.pages) {
    const common::Result<CsegPageLayout> placement = plan_cseg_v1_page_layout(cursor, page.size());
    if (!placement.has_value()) {
      return common::make_unexpected(placement.error());
    }
    std::ranges::copy(page.bytes(),
                      output.subspan(static_cast<std::size_t>(cursor), page.size()).begin());
    cursor = placement->next_offset;
  }
  if (cursor != metadata->total_length()) {
    return common::make_unexpected(
        status(common::StatusCode::kInternal, "CSEG composed part length disagrees with metadata"));
  }
  return EncodedCsegPart{std::move(storage)};
}

common::Result<EncodedCsegPart> encode_cseg_v1_part(const CsegPartEncodeInput& input) {
  return encode_cseg_part(input, format::kFormatMajor);
}

common::Result<EncodedCsegPart> encode_cseg_v2_temporal_part(const CsegPartEncodeInput& input) {
  return encode_cseg_part(input, temporal_format::kFormatMajor);
}

DecodedCsegPartView::DecodedCsegPartView(DecodedCsegMetadataView metadata,
                                         const common::ByteView encoded_part) noexcept
    : metadata_(std::move(metadata)), encoded_part_(encoded_part) {}

common::ByteView DecodedCsegPartView::stored_page(const std::size_t page_index) const noexcept {
  if (page_index >= metadata_.pages().size()) {
    return {};
  }
  const CsegPageDescriptor& page = metadata_.pages()[page_index];
  return encoded_part_.subspan(static_cast<std::size_t>(page.page_offset),
                               static_cast<std::size_t>(page.stored_length));
}

common::Result<DecodedCsegPage>
DecodedCsegPartView::decode_page(const std::size_t page_index) const {
  if (page_index >= metadata_.pages().size()) {
    return common::make_unexpected(
        status(common::StatusCode::kOutOfRange, "CSEG page index is outside the part directory"));
  }
  const CsegPageDescriptor& page = metadata_.pages()[page_index];
  return decode_cseg_v1_page(stored_page(page_index),
                             metadata_.columns()[page.stored_column_ordinal], page);
}

CsegPartDecodeResult decode_cseg_part_prefix(const common::ByteView bytes,
                                             const CsegMetadataDecodeLimits limits,
                                             const std::uint16_t expected_major) {
  CsegMetadataDecodeResult metadata = expected_major == format::kFormatMajor
                                          ? decode_cseg_v1_metadata_prefix(bytes, limits)
                                          : decode_cseg_v2_temporal_metadata_prefix(bytes, limits);
  if (!metadata.has_value()) {
    return std::unexpected(metadata.error());
  }
  if (metadata->total_length() > std::numeric_limits<std::size_t>::max()) {
    return std::unexpected(CsegPartDecodeError{
        CsegPartDecodeErrorKind::kResourceLimit,
        status(common::StatusCode::kResourceExhausted, "CSEG part does not fit this platform")});
  }
  const std::size_t total_length = static_cast<std::size_t>(metadata->total_length());
  if (bytes.size() < total_length) {
    return std::unexpected(incomplete("CSEG part prefix is incomplete", metadata->total_length()));
  }
  const common::ByteView part = bytes.first(total_length);
  const std::span<const CsegPageDescriptor> pages = metadata->pages();
  for (std::size_t index = 0U; index < pages.size(); ++index) {
    const CsegPageDescriptor& page = pages[index];
    const std::size_t offset = static_cast<std::size_t>(page.page_offset);
    const std::size_t stored_length = static_cast<std::size_t>(page.stored_length);
    const common::ByteView stored = part.subspan(offset, stored_length);
    const common::Result<DecodedCsegPage> decoded =
        decode_cseg_v1_page(stored, metadata->columns()[page.stored_column_ordinal], page);
    if (!decoded.has_value()) {
      return std::unexpected(page_error(decoded.error()));
    }
    const std::size_t page_end = offset + stored_length;
    const std::size_t next_offset = index + 1U == pages.size()
                                        ? total_length
                                        : static_cast<std::size_t>(pages[index + 1U].page_offset);
    if (!all_zero(part.subspan(page_end, next_offset - page_end))) {
      return std::unexpected(corruption("CSEG page alignment padding is nonzero"));
    }
  }
  return DecodedCsegPartView{std::move(*metadata), part};
}

CsegPartDecodeResult decode_cseg_v1_part_prefix(const common::ByteView bytes,
                                                const CsegMetadataDecodeLimits limits) {
  try {
    return decode_cseg_part_prefix(bytes, limits, format::kFormatMajor);
  } catch (const std::bad_alloc&) {
    return std::unexpected(resource_limit("CSEG part decode allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(resource_limit("CSEG part decode exceeds container limits"));
  }
}

CsegPartDecodeResult decode_cseg_v2_temporal_part_prefix(const common::ByteView bytes,
                                                         const CsegMetadataDecodeLimits limits) {
  try {
    return decode_cseg_part_prefix(bytes, limits, temporal_format::kFormatMajor);
  } catch (const std::bad_alloc&) {
    return std::unexpected(resource_limit("CSEG v2 part decode allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(resource_limit("CSEG v2 part decode exceeds container limits"));
  }
}

CsegPartDecodeResult decode_cseg_v1_part_exact(const common::ByteView bytes,
                                               const CsegMetadataDecodeLimits limits) {
  try {
    CsegPartDecodeResult decoded = decode_cseg_v1_part_prefix(bytes, limits);
    if (!decoded.has_value()) {
      return decoded;
    }
    if (bytes.size() != decoded->encoded_part().size()) {
      return std::unexpected(corruption("CSEG part exact decoder rejects trailing bytes"));
    }
    return decoded;
  } catch (const std::bad_alloc&) {
    return std::unexpected(resource_limit("CSEG part exact decode allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(resource_limit("CSEG part exact decode exceeds container limits"));
  }
}

CsegPartDecodeResult decode_cseg_v2_temporal_part_exact(const common::ByteView bytes,
                                                        const CsegMetadataDecodeLimits limits) {
  try {
    CsegPartDecodeResult decoded = decode_cseg_v2_temporal_part_prefix(bytes, limits);
    if (!decoded.has_value()) {
      return decoded;
    }
    if (bytes.size() != decoded->encoded_part().size()) {
      return std::unexpected(corruption("CSEG part exact decoder rejects trailing bytes"));
    }
    return decoded;
  } catch (const std::bad_alloc&) {
    return std::unexpected(resource_limit("CSEG v2 part exact decode allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(resource_limit("CSEG v2 part exact decode exceeds container limits"));
  }
}

common::Result<EncodedCsegPart> adopt_cseg_v2_temporal_part(const common::ByteView bytes,
                                                            const CsegMetadataDecodeLimits limits) {
  CsegPartDecodeResult decoded = decode_cseg_v2_temporal_part_exact(bytes, limits);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error().status());
  try {
    return EncodedCsegPart{std::vector<std::byte>{bytes.begin(), bytes.end()}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Cannot allocate adopted CSEG v2 image"});
  } catch (const std::length_error&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Adopted CSEG v2 image exceeds limits"});
  }
}

} // namespace chronos::cseg
