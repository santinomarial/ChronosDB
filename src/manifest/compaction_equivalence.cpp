#include "chronos/manifest/compaction_equivalence.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/result.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "cseg/sort_order_internal.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return status(common::StatusCode::kInvalidArgument, message);
}

[[nodiscard]] common::Status corruption(const std::string_view message) {
  return status(common::StatusCode::kCorruption, message);
}

[[nodiscard]] common::Status exhausted(const std::string_view message) {
  return status(common::StatusCode::kResourceExhausted, message);
}

[[nodiscard]] bool canonical_images(const std::span<const CompactionPartImage> images) noexcept {
  if (images.empty()) {
    return false;
  }
  for (std::size_t index = 0U; index < images.size(); ++index) {
    if (images[index].bytes.empty() ||
        (index != 0U && !(images[index - 1U].part_id < images[index].part_id))) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] common::Result<cseg::detail::SortCellView>
sort_cell(const columnar::PhysicalColumnView& column, const std::uint32_t row) {
  const common::Result<columnar::ColumnCellView> cell = column.cell(row);
  if (!cell.has_value()) {
    return common::make_unexpected(cell.error());
  }
  if (cell->is_null()) {
    return cseg::detail::SortCellView{.is_null = true};
  }
  if (column.type().kind() == schema::LogicalTypeKind::kBool) {
    const common::Result<bool> value = cell->boolean();
    if (!value.has_value()) {
      return common::make_unexpected(value.error());
    }
    return cseg::detail::SortCellView{.is_boolean = true, .boolean = *value};
  }
  const common::Result<common::ByteView> bytes = cell->bytes();
  if (!bytes.has_value()) {
    return common::make_unexpected(bytes.error());
  }
  return cseg::detail::SortCellView{.bytes = *bytes};
}

class PartCursor {
public:
  explicit PartCursor(cseg::DecodedCsegPartView part) noexcept : part_(std::move(part)) {}

  [[nodiscard]] common::Status initialize() {
    return load_granule();
  }

  [[nodiscard]] bool done() const noexcept {
    return granule_index_ == part_.metadata().granules().size();
  }

  [[nodiscard]] const columnar::PhysicalColumnView& column(const std::size_t ordinal) const {
    return pages_[ordinal].physical();
  }

  [[nodiscard]] std::uint32_t row() const noexcept {
    return row_;
  }

  [[nodiscard]] common::Status advance() {
    if (done()) {
      return corruption("CSEG compaction cursor advanced past its end");
    }
    ++row_;
    if (row_ < part_.metadata().granules()[granule_index_].row_count) {
      return common::Status::ok();
    }
    ++granule_index_;
    row_ = 0U;
    return load_granule();
  }

private:
  [[nodiscard]] common::Status load_granule() {
    pages_.clear();
    if (done()) {
      return common::Status::ok();
    }
    const cseg::CsegGranuleDescriptor& granule = part_.metadata().granules()[granule_index_];
    pages_.reserve(part_.metadata().columns().size());
    for (std::size_t ordinal = 0U; ordinal < part_.metadata().columns().size(); ++ordinal) {
      common::Result<cseg::DecodedCsegPage> page =
          part_.decode_page(static_cast<std::size_t>(granule.first_page_index) + ordinal);
      if (!page.has_value()) {
        return page.error();
      }
      pages_.push_back(std::move(*page));
    }
    return common::Status::ok();
  }

  cseg::DecodedCsegPartView part_;
  std::size_t granule_index_{};
  std::uint32_t row_{};
  std::vector<cseg::DecodedCsegPage> pages_;
};

struct Ordering {
  std::span<const std::uint32_t> user_key_ordinals;
  std::size_t system_start{};
};

[[nodiscard]] common::Result<int> compare_column(const PartCursor& left, const PartCursor& right,
                                                 const std::size_t ordinal,
                                                 const schema::LogicalTypeKind kind) {
  const common::Result<cseg::detail::SortCellView> lhs =
      sort_cell(left.column(ordinal), left.row());
  const common::Result<cseg::detail::SortCellView> rhs =
      sort_cell(right.column(ordinal), right.row());
  if (!lhs.has_value()) {
    return common::make_unexpected(lhs.error());
  }
  if (!rhs.has_value()) {
    return common::make_unexpected(rhs.error());
  }
  return cseg::detail::compare_sort_cells(kind, *lhs, *rhs);
}

[[nodiscard]] common::Result<int> compare_rows(const PartCursor& left, const PartCursor& right,
                                               const Ordering ordering) {
  for (const std::uint32_t ordinal : ordering.user_key_ordinals) {
    const common::Result<int> compared =
        compare_column(left, right, ordinal, left.column(ordinal).type().kind());
    if (!compared.has_value() || *compared != 0) {
      return compared;
    }
  }
  constexpr schema::LogicalTypeKind kKinds[]{schema::LogicalTypeKind::kUuid,
                                             schema::LogicalTypeKind::kUInt64,
                                             schema::LogicalTypeKind::kUInt32};
  for (std::size_t system = 0U; system < std::size(kKinds); ++system) {
    const common::Result<int> compared =
        compare_column(left, right, ordering.system_start + system, kKinds[system]);
    if (!compared.has_value() || *compared != 0) {
      return compared;
    }
  }
  return 0;
}

[[nodiscard]] common::Result<bool> same_cell(const PartCursor& left, const PartCursor& right,
                                             const std::size_t ordinal) {
  const common::Result<columnar::ColumnCellView> lhs = left.column(ordinal).cell(left.row());
  const common::Result<columnar::ColumnCellView> rhs = right.column(ordinal).cell(right.row());
  if (!lhs.has_value()) {
    return common::make_unexpected(lhs.error());
  }
  if (!rhs.has_value()) {
    return common::make_unexpected(rhs.error());
  }
  if (lhs->kind() != rhs->kind()) {
    return false;
  }
  if (lhs->is_null()) {
    return true;
  }
  if (left.column(ordinal).type().kind() == schema::LogicalTypeKind::kBool) {
    const common::Result<bool> left_value = lhs->boolean();
    const common::Result<bool> right_value = rhs->boolean();
    if (!left_value.has_value()) {
      return common::make_unexpected(left_value.error());
    }
    if (!right_value.has_value()) {
      return common::make_unexpected(right_value.error());
    }
    return *left_value == *right_value;
  }
  const common::Result<common::ByteView> left_value = lhs->bytes();
  const common::Result<common::ByteView> right_value = rhs->bytes();
  if (!left_value.has_value()) {
    return common::make_unexpected(left_value.error());
  }
  if (!right_value.has_value()) {
    return common::make_unexpected(right_value.error());
  }
  return std::ranges::equal(*left_value, *right_value);
}

class MergedStream {
public:
  MergedStream(std::vector<PartCursor> cursors, Ordering ordering,
               const wal::WalId& wal_id) noexcept
      : cursors_(std::move(cursors)), ordering_(ordering), wal_id_(wal_id) {}

  [[nodiscard]] common::Result<PartCursor*> next() {
    PartCursor* selected = nullptr;
    for (PartCursor& cursor : cursors_) {
      if (cursor.done()) {
        continue;
      }
      if (selected == nullptr) {
        selected = &cursor;
        continue;
      }
      const common::Result<int> compared = compare_rows(cursor, *selected, ordering_);
      if (!compared.has_value()) {
        return common::make_unexpected(compared.error());
      }
      if (*compared == 0) {
        return common::make_unexpected(
            corruption("CSEG compaction side contains a duplicate physical row tuple"));
      }
      if (*compared < 0) {
        selected = &cursor;
      }
    }
    if (selected == nullptr) {
      return selected;
    }
    const common::Result<columnar::ColumnCellView> wal_cell =
        selected->column(ordering_.system_start).cell(selected->row());
    if (!wal_cell.has_value() || wal_cell->is_null()) {
      return common::make_unexpected(corruption("CSEG compaction WAL cell is inaccessible"));
    }
    const common::Result<common::ByteView> bytes = wal_cell->bytes();
    if (!bytes.has_value() || !std::ranges::equal(*bytes, wal_id_.bytes)) {
      return common::make_unexpected(invalid("CSEG compaction part uses an unexpected WAL"));
    }
    return selected;
  }

private:
  std::vector<PartCursor> cursors_;
  Ordering ordering_;
  wal::WalId wal_id_;
};

[[nodiscard]] common::Result<std::vector<PartCursor>>
open_parts(const std::span<const CompactionPartImage> images, const schema::TableSchema& schema,
           const schema::TabletId& tablet_id, const CompactionEquivalenceLimits limits,
           std::uint64_t& row_count, std::uint64_t& resident_page_bound) {
  std::vector<PartCursor> cursors;
  cursors.reserve(images.size());
  row_count = 0U;
  for (const CompactionPartImage& image : images) {
    cseg::CsegPartDecodeResult decoded =
        cseg::decode_cseg_v1_part_exact(image.bytes, limits.decode);
    if (!decoded.has_value()) {
      return common::make_unexpected(decoded.error().status());
    }
    if (decoded->metadata().part_id() != image.part_id) {
      return common::make_unexpected(
          invalid("CSEG compaction image identity disagrees with bytes"));
    }
    common::Status validated =
        cseg::validate_cseg_v1_part(*decoded, schema, tablet_id, limits.validation);
    if (!validated.is_ok()) {
      return common::make_unexpected(std::move(validated));
    }
    const std::optional<std::uint64_t> next =
        common::checked_add(row_count, decoded->metadata().row_count());
    if (!next.has_value() || *next > limits.max_rows_per_side) {
      return common::make_unexpected(exhausted("CSEG compaction row limit exceeded"));
    }
    row_count = *next;
    std::uint64_t largest_granule = 0U;
    for (const cseg::CsegGranuleDescriptor& granule : decoded->metadata().granules()) {
      std::uint64_t granule_bytes = 0U;
      for (std::size_t ordinal = 0U; ordinal < decoded->metadata().columns().size(); ++ordinal) {
        const cseg::CsegPageDescriptor& page =
            decoded->metadata()
                .pages()[static_cast<std::size_t>(granule.first_page_index) + ordinal];
        const std::optional<std::uint64_t> with_page =
            common::checked_add(granule_bytes, page.uncompressed_length);
        if (!with_page.has_value()) {
          return common::make_unexpected(
              exhausted("CSEG compaction resident-page accounting overflows"));
        }
        granule_bytes = *with_page;
      }
      largest_granule = std::max(largest_granule, granule_bytes);
    }
    const std::optional<std::uint64_t> with_part =
        common::checked_add(resident_page_bound, largest_granule);
    if (!with_part.has_value() || *with_part > limits.max_resident_page_bytes) {
      return common::make_unexpected(exhausted("CSEG compaction resident-page limit exceeded"));
    }
    resident_page_bound = *with_part;
    PartCursor cursor{std::move(*decoded)};
    common::Status initialized = cursor.initialize();
    if (!initialized.is_ok()) {
      return common::make_unexpected(std::move(initialized));
    }
    cursors.push_back(std::move(cursor));
  }
  return cursors;
}

} // namespace

common::Status validate_append_only_cseg_v1_equivalence(
    const std::span<const CompactionPartImage> inputs,
    const std::span<const CompactionPartImage> outputs, const schema::TableSchema& schema,
    const schema::TabletId& tablet_id, const wal::WalId& wal_id,
    const CompactionEquivalenceLimits limits) {
  if (!canonical_images(inputs) || !canonical_images(outputs)) {
    return invalid("CSEG compaction image sets must be nonempty and strictly PartId-sorted");
  }
  if (inputs.size() > limits.max_parts_per_side || outputs.size() > limits.max_parts_per_side ||
      limits.max_parts_per_side == 0U || limits.max_rows_per_side == 0U ||
      limits.max_resident_page_bytes == 0U || !wal_id.is_valid()) {
    return invalid("CSEG compaction equivalence limits or WAL identity are invalid");
  }
  for (const CompactionPartImage& output : outputs) {
    const auto reused = std::ranges::lower_bound(
        inputs, output.part_id, {}, [](const CompactionPartImage& image) { return image.part_id; });
    if (reused != inputs.end() && reused->part_id == output.part_id) {
      return invalid("CSEG compaction output PartId is not fresh");
    }
  }
  try {
    std::vector<std::uint32_t> key_ordinals;
    key_ordinals.reserve(schema.physical_ordering_key().size());
    for (const schema::ColumnId& id : schema.physical_ordering_key()) {
      const std::optional<std::size_t> ordinal = schema.column_ordinal(id);
      if (!ordinal.has_value() || *ordinal > cseg::format::kMaximumUserColumnCount) {
        return invalid("CSEG compaction schema ordering key is invalid");
      }
      key_ordinals.push_back(static_cast<std::uint32_t>(*ordinal));
    }
    const Ordering ordering{.user_key_ordinals = key_ordinals,
                            .system_start = schema.columns().size()};
    std::uint64_t input_rows = 0U;
    std::uint64_t output_rows = 0U;
    std::uint64_t resident_page_bound = 0U;
    common::Result<std::vector<PartCursor>> input_cursors =
        open_parts(inputs, schema, tablet_id, limits, input_rows, resident_page_bound);
    if (!input_cursors.has_value()) {
      return input_cursors.error();
    }
    common::Result<std::vector<PartCursor>> output_cursors =
        open_parts(outputs, schema, tablet_id, limits, output_rows, resident_page_bound);
    if (!output_cursors.has_value()) {
      return output_cursors.error();
    }
    if (input_rows != output_rows) {
      return invalid("CSEG compaction input and output row counts differ");
    }
    MergedStream input_stream{std::move(*input_cursors), ordering, wal_id};
    MergedStream output_stream{std::move(*output_cursors), ordering, wal_id};
    for (std::uint64_t row = 0U; row < input_rows; ++row) {
      common::Result<PartCursor*> input = input_stream.next();
      common::Result<PartCursor*> output = output_stream.next();
      if (!input.has_value()) {
        return input.error();
      }
      if (!output.has_value()) {
        return output.error();
      }
      if (*input == nullptr || *output == nullptr) {
        return corruption("CSEG compaction row stream ended before its declared count");
      }
      const common::Result<int> ordered = compare_rows(**input, **output, ordering);
      if (!ordered.has_value()) {
        return ordered.error();
      }
      if (*ordered != 0) {
        return invalid("CSEG compaction output changed a physical row identity");
      }
      const std::size_t stored_columns = schema.columns().size() + cseg::format::kSystemColumnCount;
      for (std::size_t ordinal = 0U; ordinal < stored_columns; ++ordinal) {
        const common::Result<bool> same = same_cell(**input, **output, ordinal);
        if (!same.has_value()) {
          return same.error();
        }
        if (!*same) {
          return invalid("CSEG compaction output changed a stored row value");
        }
      }
      common::Status input_advance = (*input)->advance();
      common::Status output_advance = (*output)->advance();
      if (!input_advance.is_ok()) {
        return input_advance;
      }
      if (!output_advance.is_ok()) {
        return output_advance;
      }
    }
    common::Result<PartCursor*> input_tail = input_stream.next();
    common::Result<PartCursor*> output_tail = output_stream.next();
    if (!input_tail.has_value()) {
      return input_tail.error();
    }
    if (!output_tail.has_value()) {
      return output_tail.error();
    }
    if (*input_tail != nullptr || *output_tail != nullptr) {
      return corruption("CSEG compaction row stream exceeded its declared count");
    }
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    return exhausted("CSEG compaction equivalence allocation failed");
  } catch (const std::length_error&) {
    return exhausted("CSEG compaction equivalence allocation length is unsupported");
  }
}

} // namespace chronos::manifest
