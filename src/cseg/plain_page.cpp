#include "chronos/cseg/plain_page.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/cseg/format.hpp"

#include <algorithm>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::cseg {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}

} // namespace

EncodedCsegPlainPage::EncodedCsegPlainPage(
    std::vector<std::byte> bytes,
    // The parameters preserve the normative buffer order.
    const std::uint64_t validity_length, // NOLINT(bugprone-easily-swappable-parameters)
    const std::uint64_t offsets_length, const std::uint64_t values_length) noexcept
    : bytes_(std::move(bytes)), validity_length_(validity_length), offsets_length_(offsets_length),
      values_length_(values_length) {}

common::ByteView EncodedCsegPlainPage::bytes() const noexcept {
  return bytes_;
}

std::size_t EncodedCsegPlainPage::size() const noexcept {
  return bytes_.size();
}

common::Result<EncodedCsegPlainPage>
encode_cseg_v1_plain_page(const columnar::PhysicalColumnView& column) {
  if (column.buffer_bytes() > format::kMaximumUncompressedPageLength) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "CSEG PLAIN page exceeds the v1 uncompressed limit"));
  }
  std::vector<std::byte> bytes;
  bytes.reserve(column.buffer_bytes());
  bytes.insert(bytes.end(), column.validity().begin(), column.validity().end());
  bytes.insert(bytes.end(), column.offsets().begin(), column.offsets().end());
  bytes.insert(bytes.end(), column.values().begin(), column.values().end());
  return EncodedCsegPlainPage{std::move(bytes),
                              static_cast<std::uint64_t>(column.validity().size()),
                              static_cast<std::uint64_t>(column.offsets().size()),
                              static_cast<std::uint64_t>(column.values().size())};
}

DecodedCsegPlainPageView::DecodedCsegPlainPageView(columnar::PhysicalColumnView physical,
                                                   const common::ByteView encoded_bytes) noexcept
    : physical_(physical), encoded_bytes_(encoded_bytes) {}

common::Result<DecodedCsegPlainPageView>
decode_cseg_v1_plain_page(const common::ByteView uncompressed_payload,
                          const CsegColumnDescriptor& column, const CsegPageDescriptor& page) {
  if (page.uncompressed_length == 0U ||
      page.uncompressed_length > format::kMaximumUncompressedPageLength ||
      page.uncompressed_length != uncompressed_payload.size()) {
    return common::make_unexpected(
        status(common::StatusCode::kCorruption,
               "CSEG PLAIN payload size does not match a bounded nonzero page descriptor"));
  }
  const std::optional<std::uint64_t> validity_and_offsets =
      common::checked_add(page.validity_length, page.offsets_length);
  const std::optional<std::uint64_t> total =
      validity_and_offsets.has_value()
          ? common::checked_add(*validity_and_offsets, page.values_length)
          : std::nullopt;
  if (!total.has_value() || *total != page.uncompressed_length ||
      page.validity_length > std::numeric_limits<std::size_t>::max() ||
      page.offsets_length > std::numeric_limits<std::size_t>::max() ||
      page.values_length > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(
        status(common::StatusCode::kCorruption, "CSEG PLAIN buffer lengths are invalid"));
  }
  const std::size_t validity_length = static_cast<std::size_t>(page.validity_length);
  const std::size_t offsets_length = static_cast<std::size_t>(page.offsets_length);
  const columnar::ColumnVectorBufferView buffers{
      .validity = uncompressed_payload.first(validity_length),
      .offsets = uncompressed_payload.subspan(validity_length, offsets_length),
      .values = uncompressed_payload.last(static_cast<std::size_t>(page.values_length)),
  };
  const common::Result<columnar::PhysicalColumnView> physical =
      columnar::PhysicalColumnView::create({.type = column.logical_type,
                                            .nullable = column.nullable,
                                            .row_count = page.row_count,
                                            .null_count = page.null_count},
                                           buffers);
  if (!physical.has_value()) {
    return common::make_unexpected(status(common::StatusCode::kCorruption,
                                          std::string{"CSEG PLAIN physical payload is invalid: "} +
                                              physical.error().message()));
  }
  return DecodedCsegPlainPageView{*physical, uncompressed_payload};
}

} // namespace chronos::cseg
