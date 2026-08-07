#include "chronos/cseg/projected_reader.hpp"

#include "chronos/common/checked_math.hpp"
#include "system_rows_internal.hpp"

#include <algorithm>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::cseg {
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

[[nodiscard]] CsegProjectedReaderOpenError open_error(const CsegMetadataDecodeError& error) {
  using OpenKind = CsegProjectedReaderOpenErrorKind;
  OpenKind kind = OpenKind::kCorruption;
  switch (error.kind()) {
  case CsegMetadataDecodeErrorKind::kIncomplete:
    kind = OpenKind::kIncomplete;
    break;
  case CsegMetadataDecodeErrorKind::kCorruption:
    kind = OpenKind::kCorruption;
    break;
  case CsegMetadataDecodeErrorKind::kUnsupported:
    kind = OpenKind::kUnsupported;
    break;
  case CsegMetadataDecodeErrorKind::kResourceLimit:
    kind = OpenKind::kResourceLimit;
    break;
  }
  return {kind, error.status(), error.required_size()};
}

[[nodiscard]] CsegProjectedReaderOpenError incomplete(const std::uint64_t required_size) {
  return {
      CsegProjectedReaderOpenErrorKind::kIncomplete,
      status(common::StatusCode::kOutOfRange, "CSEG projected-reader part prefix is incomplete"),
      required_size};
}

[[nodiscard]] CsegProjectedReaderOpenError invalid_open(const common::Status& error) {
  const CsegProjectedReaderOpenErrorKind kind =
      error.code() == common::StatusCode::kNotFound
          ? CsegProjectedReaderOpenErrorKind::kNotFound
          : CsegProjectedReaderOpenErrorKind::kInvalidArgument;
  return {kind, error};
}

[[nodiscard]] bool all_zero(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0}; });
}

[[nodiscard]] std::size_t fixed_width(const schema::LogicalTypeKind kind) noexcept {
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kUInt8:
    return 1U;
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kUInt16:
    return 2U;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kDate:
    return 4U;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kTimestampNs:
    return 8U;
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kUuid:
    return 16U;
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return 0U;
  }
  return 0U;
}

[[nodiscard]] common::Result<std::uint64_t> null_column_bytes(const schema::LogicalType& type,
                                                              const std::uint32_t row_count) {
  const std::uint64_t validity = columnar::bitmap_size(row_count);
  std::uint64_t second_buffer = 0U;
  if (type.is_variable_width()) {
    const std::optional<std::uint64_t> offset_count =
        common::checked_add<std::uint64_t>(row_count, 1U);
    if (!offset_count.has_value()) {
      return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                            "CSEG synthesized offset count overflows"));
    }
    const std::optional<std::uint64_t> offset_bytes =
        common::checked_multiply(*offset_count, static_cast<std::uint64_t>(sizeof(std::uint32_t)));
    if (!offset_bytes.has_value()) {
      return common::make_unexpected(
          status(common::StatusCode::kResourceExhausted, "CSEG synthesized offset bytes overflow"));
    }
    second_buffer = *offset_bytes;
  } else if (type.kind() == schema::LogicalTypeKind::kBool) {
    second_buffer = validity;
  } else {
    const std::optional<std::uint64_t> value_bytes =
        common::checked_multiply(static_cast<std::uint64_t>(row_count),
                                 static_cast<std::uint64_t>(fixed_width(type.kind())));
    if (!value_bytes.has_value()) {
      return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                            "CSEG synthesized fixed-width bytes overflow"));
    }
    second_buffer = *value_bytes;
  }
  const std::optional<std::uint64_t> total = common::checked_add(validity, second_buffer);
  if (!total.has_value()) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "CSEG synthesized column byte accounting overflows"));
  }
  return *total;
}

[[nodiscard]] common::Result<columnar::OwnedColumnVector>
synthesize_null_column(const schema::ColumnDefinition& definition, const std::uint32_t row_count) {
  if (!definition.nullable()) {
    return common::make_unexpected(
        status(common::StatusCode::kInternal,
               "CSEG schema projection attempted to synthesize a non-nullable column"));
  }
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(row_count), std::byte{0});
  if (definition.type().is_variable_width()) {
    buffers.offsets.resize((static_cast<std::size_t>(row_count) + 1U) * sizeof(std::uint32_t),
                           std::byte{0});
  } else if (definition.type().kind() == schema::LogicalTypeKind::kBool) {
    buffers.values.resize(columnar::bitmap_size(row_count), std::byte{0});
  } else {
    buffers.values.resize(
        static_cast<std::size_t>(row_count) * fixed_width(definition.type().kind()), std::byte{0});
  }
  return columnar::OwnedColumnVector::create({.column_id = definition.id(),
                                              .type = definition.type(),
                                              .nullable = true,
                                              .row_count = row_count,
                                              .null_count = row_count},
                                             std::move(buffers));
}

struct ByteAccountingInput {
  std::uint64_t amount;
  std::uint64_t limit;
};

[[nodiscard]] common::Status add_bytes(std::uint64_t& total, const ByteAccountingInput input) {
  const std::optional<std::uint64_t> next = common::checked_add(total, input.amount);
  if (!next.has_value() || *next > input.limit) {
    return status(common::StatusCode::kResourceExhausted,
                  "CSEG projected read exceeds its decoded-buffer limit");
  }
  total = *next;
  return common::Status::ok();
}

[[nodiscard]] common::Status add_exact_bytes(std::uint64_t& total, const std::uint64_t amount) {
  const std::optional<std::uint64_t> next = common::checked_add(total, amount);
  if (!next.has_value()) {
    return status(common::StatusCode::kResourceExhausted,
                  "CSEG projected read byte accounting overflows");
  }
  total = *next;
  return common::Status::ok();
}

[[nodiscard]] std::size_t saturating_add(const std::size_t left, const std::size_t right) noexcept {
  const std::optional<std::size_t> total = common::checked_add(left, right);
  return total.value_or(std::numeric_limits<std::size_t>::max());
}

template <typename Value>
[[nodiscard]] std::size_t retained_vector_objects(const std::vector<Value>& values) noexcept {
  const std::optional<std::size_t> bytes =
      common::checked_multiply(values.capacity(), sizeof(Value));
  return bytes.value_or(std::numeric_limits<std::size_t>::max());
}

} // namespace

CsegProjectedReaderOpenError::CsegProjectedReaderOpenError(
    const CsegProjectedReaderOpenErrorKind kind, common::Status status_value,
    const std::uint64_t required_size) noexcept
    : kind_(kind), status_(std::move(status_value)), required_size_(required_size) {}

CsegProjectedGranuleReadPlan::CsegProjectedGranuleReadPlan(
    const CsegProjectedReaderView* reader, const std::size_t granule_ordinal,
    const CsegGranuleDescriptor& descriptor,
    const std::span<const std::uint32_t> destination_column_ordinals,
    const Accounting accounting) noexcept
    : reader_(reader), granule_ordinal_(granule_ordinal), first_row_(descriptor.first_row),
      row_count_(descriptor.row_count), destination_column_ordinals_(destination_column_ordinals),
      source_user_page_count_(accounting.source_user_page_count),
      synthesized_column_count_(accounting.synthesized_column_count),
      decoded_buffer_bytes_(accounting.decoded_buffer_bytes),
      owned_buffer_bytes_(accounting.owned_buffer_bytes),
      borrowed_buffer_bytes_(accounting.borrowed_buffer_bytes) {}

CsegProjectedColumnView::CsegProjectedColumnView(
    const schema::ColumnId column_id, const columnar::PhysicalColumnView physical) noexcept
    : column_id_(column_id), physical_(physical) {}

ProjectedCsegGranule::ProjectedCsegGranule(
    const std::size_t granule_ordinal, const CsegGranuleDescriptor& descriptor,
    std::shared_ptr<const schema::TableSchema> schema_value,
    std::vector<DecodedCsegPage> decoded_pages,
    std::vector<columnar::OwnedColumnVector> synthesized_columns,
    std::vector<CsegProjectedColumnView> columns, const std::size_t system_page_start) noexcept
    : granule_ordinal_(granule_ordinal), first_row_(descriptor.first_row),
      row_count_(descriptor.row_count), schema_(std::move(schema_value)),
      decoded_pages_(std::move(decoded_pages)),
      synthesized_columns_(std::move(synthesized_columns)), columns_(std::move(columns)),
      system_page_start_(system_page_start) {}

const CsegProjectedColumnView*
ProjectedCsegGranule::column(const std::size_t ordinal) const noexcept {
  return ordinal < columns_.size() ? &columns_[ordinal] : nullptr;
}

const columnar::PhysicalColumnView& ProjectedCsegGranule::wal_id() const noexcept {
  return decoded_pages_[system_page_start_].physical();
}

const columnar::PhysicalColumnView& ProjectedCsegGranule::record_sequence() const noexcept {
  return decoded_pages_[system_page_start_ + 1U].physical();
}

const columnar::PhysicalColumnView& ProjectedCsegGranule::row_ordinal() const noexcept {
  return decoded_pages_[system_page_start_ + 2U].physical();
}

const columnar::PhysicalColumnView& ProjectedCsegGranule::operation() const noexcept {
  return decoded_pages_[system_page_start_ + 3U].physical();
}

std::size_t ProjectedCsegGranule::buffer_bytes() const noexcept {
  std::size_t total = 0U;
  for (const DecodedCsegPage& page : decoded_pages_)
    total = saturating_add(total, page.uncompressed_bytes().size());
  for (const columnar::OwnedColumnVector& column : synthesized_columns_)
    total = saturating_add(total, column.buffer_bytes());
  return total;
}

std::size_t ProjectedCsegGranule::retained_buffer_bytes() const noexcept {
  std::size_t total = retained_vector_objects(decoded_pages_);
  total = saturating_add(total, retained_vector_objects(synthesized_columns_));
  total = saturating_add(total, retained_vector_objects(columns_));
  for (const DecodedCsegPage& page : decoded_pages_)
    total = saturating_add(total, page.retained_buffer_bytes());
  for (const columnar::OwnedColumnVector& column : synthesized_columns_)
    total = saturating_add(total, column.retained_buffer_bytes());
  return total;
}

CsegProjectedReaderView::CsegProjectedReaderView(
    DecodedCsegMetadataView metadata, const common::ByteView encoded_part,
    std::shared_ptr<const schema::TableSchema> source_schema,
    std::shared_ptr<const schema::TableSchema> destination_schema,
    schema::SchemaProjection projection, const CsegProjectedReaderLimits limits) noexcept
    : metadata_(std::move(metadata)), encoded_part_(encoded_part),
      source_schema_(std::move(source_schema)), destination_schema_(std::move(destination_schema)),
      projection_(std::move(projection)), limits_(limits) {}

common::Result<CsegProjectedGranuleReadPlan> CsegProjectedReaderView::plan_granule(
    const std::size_t granule_ordinal,
    const std::span<const std::uint32_t> destination_column_ordinals) const {
  if (granule_ordinal >= metadata_.granules().size()) {
    return common::make_unexpected(
        status(common::StatusCode::kOutOfRange, "CSEG granule ordinal is outside the directory"));
  }
  if (destination_column_ordinals.size() > limits_.max_projected_columns) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted,
               "CSEG projected column count exceeds its configured limit"));
  }

  const CsegGranuleDescriptor& granule = metadata_.granules()[granule_ordinal];
  std::bitset<schema::kMaximumSchemaColumnCount> seen;
  std::size_t source_user_page_count = 0U;
  std::size_t synthesized_column_count = 0U;
  std::uint64_t decoded_bytes = 0U;
  std::uint64_t owned_bytes = 0U;
  std::uint64_t borrowed_bytes = 0U;

  const auto account_page = [this, &decoded_bytes, &owned_bytes,
                             &borrowed_bytes](const std::size_t page_index) -> common::Status {
    const CsegPageDescriptor& page = metadata_.pages()[page_index];
    common::Status accounting =
        add_bytes(decoded_bytes,
                  {.amount = page.uncompressed_length, .limit = limits_.max_decoded_buffer_bytes});
    if (!accounting.is_ok())
      return accounting;
    std::uint64_t& ownership_total =
        page.compression == PageCompression::kNone ? borrowed_bytes : owned_bytes;
    return add_exact_bytes(ownership_total, page.uncompressed_length);
  };

  for (const std::uint32_t destination_ordinal : destination_column_ordinals) {
    if (destination_ordinal >= projection_.entries().size()) {
      return common::make_unexpected(
          invalid("CSEG projection destination ordinal is outside the destination schema"));
    }
    if (seen[destination_ordinal]) {
      return common::make_unexpected(
          invalid("CSEG projection destination ordinals are not unique"));
    }
    seen[destination_ordinal] = true;
    const schema::ProjectionEntry& entry = projection_.entries()[destination_ordinal];
    const schema::ColumnDefinition& definition =
        destination_schema_->columns()[destination_ordinal];
    const auto source_ordinal = entry.ancestor_ordinal();
    if (source_ordinal.has_value()) {
      const std::size_t page_index =
          static_cast<std::size_t>(granule.first_page_index) + *source_ordinal;
      common::Status accounting = account_page(page_index);
      if (!accounting.is_ok())
        return common::make_unexpected(std::move(accounting));
      ++source_user_page_count;
    } else {
      const common::Result<std::uint64_t> bytes =
          null_column_bytes(definition.type(), granule.row_count);
      if (!bytes.has_value())
        return common::make_unexpected(bytes.error());
      common::Status accounting =
          add_bytes(decoded_bytes, {.amount = *bytes, .limit = limits_.max_decoded_buffer_bytes});
      if (!accounting.is_ok())
        return common::make_unexpected(std::move(accounting));
      accounting = add_exact_bytes(owned_bytes, *bytes);
      if (!accounting.is_ok())
        return common::make_unexpected(std::move(accounting));
      ++synthesized_column_count;
    }
  }

  const std::size_t source_user_count = source_schema_->columns().size();
  for (std::size_t system = 0U; system < format::kSystemColumnCount; ++system) {
    const std::size_t page_index =
        static_cast<std::size_t>(granule.first_page_index) + source_user_count + system;
    common::Status accounting = account_page(page_index);
    if (!accounting.is_ok())
      return common::make_unexpected(std::move(accounting));
  }

  return CsegProjectedGranuleReadPlan{this,
                                      granule_ordinal,
                                      granule,
                                      destination_column_ordinals,
                                      {.source_user_page_count = source_user_page_count,
                                       .synthesized_column_count = synthesized_column_count,
                                       .decoded_buffer_bytes = decoded_bytes,
                                       .owned_buffer_bytes = owned_bytes,
                                       .borrowed_buffer_bytes = borrowed_bytes}};
}

common::Result<ProjectedCsegGranule> CsegProjectedReaderView::read_granule(
    const std::size_t granule_ordinal,
    const std::span<const std::uint32_t> destination_column_ordinals) const {
  common::Result<CsegProjectedGranuleReadPlan> plan =
      plan_granule(granule_ordinal, destination_column_ordinals);
  if (!plan.has_value())
    return common::make_unexpected(plan.error());
  return execute_granule_plan(*plan);
}

common::Result<ProjectedCsegGranule>
CsegProjectedReaderView::read_granule(const CsegProjectedGranuleReadPlan& plan) const {
  if (plan.reader_ != this) {
    return common::make_unexpected(
        invalid("CSEG projected granule plan belongs to another reader"));
  }
  common::Result<CsegProjectedGranuleReadPlan> refreshed =
      plan_granule(plan.granule_ordinal_, plan.destination_column_ordinals_);
  if (!refreshed.has_value())
    return common::make_unexpected(refreshed.error());
  if (refreshed->first_row_ != plan.first_row_ || refreshed->row_count_ != plan.row_count_ ||
      refreshed->source_user_page_count_ != plan.source_user_page_count_ ||
      refreshed->synthesized_column_count_ != plan.synthesized_column_count_ ||
      refreshed->decoded_buffer_bytes_ != plan.decoded_buffer_bytes_ ||
      refreshed->owned_buffer_bytes_ != plan.owned_buffer_bytes_ ||
      refreshed->borrowed_buffer_bytes_ != plan.borrowed_buffer_bytes_) {
    return common::make_unexpected(
        invalid("CSEG projected granule plan no longer matches its borrowed request"));
  }
  return execute_granule_plan(*refreshed);
}

common::Result<ProjectedCsegGranule>
CsegProjectedReaderView::execute_granule_plan(const CsegProjectedGranuleReadPlan& plan) const {
  try {
    const CsegGranuleDescriptor& granule = metadata_.granules()[plan.granule_ordinal_];
    const std::span<const std::uint32_t> destination_column_ordinals =
        plan.destination_column_ordinals_;

    const auto decode_page =
        [this](const std::size_t page_index) -> common::Result<DecodedCsegPage> {
      const CsegPageDescriptor& descriptor = metadata_.pages()[page_index];
      const std::size_t offset = static_cast<std::size_t>(descriptor.page_offset);
      const std::size_t stored_length = static_cast<std::size_t>(descriptor.stored_length);
      const common::ByteView stored = encoded_part_.subspan(offset, stored_length);
      common::Result<DecodedCsegPage> decoded = decode_cseg_v1_page(
          stored, metadata_.columns()[descriptor.stored_column_ordinal], descriptor);
      if (!decoded.has_value()) {
        return decoded;
      }
      const std::size_t page_end = offset + stored_length;
      const std::size_t next_offset =
          page_index + 1U == metadata_.pages().size()
              ? encoded_part_.size()
              : static_cast<std::size_t>(metadata_.pages()[page_index + 1U].page_offset);
      if (!all_zero(encoded_part_.subspan(page_end, next_offset - page_end))) {
        return common::make_unexpected(
            corruption("CSEG projected page alignment padding is nonzero"));
      }
      return decoded;
    };

    std::vector<DecodedCsegPage> decoded_pages;
    decoded_pages.reserve(plan.decoded_page_count());
    for (const std::uint32_t destination_ordinal : destination_column_ordinals) {
      const schema::ProjectionEntry& entry = projection_.entries()[destination_ordinal];
      const auto source_ordinal = entry.ancestor_ordinal();
      if (source_ordinal.has_value()) {
        const std::size_t page_index =
            static_cast<std::size_t>(granule.first_page_index) + *source_ordinal;
        common::Result<DecodedCsegPage> page = decode_page(page_index);
        if (!page.has_value())
          return common::make_unexpected(page.error());
        decoded_pages.push_back(std::move(*page));
      }
    }
    const std::size_t system_page_start = decoded_pages.size();
    const std::size_t source_user_count = source_schema_->columns().size();
    for (std::size_t system_index = 0U; system_index < format::kSystemColumnCount; ++system_index) {
      const std::size_t page_index =
          static_cast<std::size_t>(granule.first_page_index) + source_user_count + system_index;
      common::Result<DecodedCsegPage> page = decode_page(page_index);
      if (!page.has_value())
        return common::make_unexpected(page.error());
      decoded_pages.push_back(std::move(*page));
    }
    common::Status system = detail::validate_cseg_v1_system_rows(
        {.wal_id = decoded_pages[system_page_start].physical(),
         .record_sequence = decoded_pages[system_page_start + 1U].physical(),
         .row_ordinal = decoded_pages[system_page_start + 2U].physical(),
         .operation = decoded_pages[system_page_start + 3U].physical()},
        granule.row_count);
    if (!system.is_ok())
      return common::make_unexpected(std::move(system));

    std::vector<columnar::OwnedColumnVector> synthesized_columns;
    synthesized_columns.reserve(plan.synthesized_column_count_);
    for (const std::uint32_t destination_ordinal : destination_column_ordinals) {
      const schema::ProjectionEntry& entry = projection_.entries()[destination_ordinal];
      const auto source_ordinal = entry.ancestor_ordinal();
      if (!source_ordinal.has_value()) {
        common::Result<columnar::OwnedColumnVector> column = synthesize_null_column(
            destination_schema_->columns()[destination_ordinal], granule.row_count);
        if (!column.has_value())
          return common::make_unexpected(column.error());
        synthesized_columns.push_back(std::move(*column));
      }
    }

    std::vector<CsegProjectedColumnView> columns;
    columns.reserve(destination_column_ordinals.size());
    std::size_t source_storage_index = 0U;
    std::size_t synthesized_storage_index = 0U;
    for (const std::uint32_t destination_ordinal : destination_column_ordinals) {
      const schema::ProjectionEntry& entry = projection_.entries()[destination_ordinal];
      const schema::ColumnId column_id = destination_schema_->columns()[destination_ordinal].id();
      const auto source_ordinal = entry.ancestor_ordinal();
      if (source_ordinal.has_value()) {
        columns.push_back(
            CsegProjectedColumnView{column_id, decoded_pages[source_storage_index].physical()});
        ++source_storage_index;
      } else {
        const columnar::ColumnVectorView view =
            synthesized_columns[synthesized_storage_index].view();
        columns.push_back(CsegProjectedColumnView{column_id, view.physical()});
        ++synthesized_storage_index;
      }
    }
    return ProjectedCsegGranule{plan.granule_ordinal_,
                                granule,
                                destination_schema_,
                                std::move(decoded_pages),
                                std::move(synthesized_columns),
                                std::move(columns),
                                system_page_start};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "CSEG projected granule allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "CSEG projected granule exceeds container limits"));
  }
}

CsegProjectedReaderOpenResult open_cseg_v1_projected_reader_prefix(
    const common::ByteView bytes, const schema::SchemaLineage& lineage,
    const schema::SchemaId destination_schema_id, const schema::TabletId& target_tablet,
    const CsegProjectedReaderLimits limits) {
  if (limits.max_decoded_buffer_bytes == 0U) {
    return std::unexpected(CsegProjectedReaderOpenError{
        CsegProjectedReaderOpenErrorKind::kInvalidArgument,
        invalid("CSEG projected-reader decoded-buffer limit must be nonzero")});
  }
  CsegMetadataDecodeResult metadata = decode_cseg_v1_metadata_prefix(bytes, limits.metadata);
  if (!metadata.has_value()) {
    return std::unexpected(open_error(metadata.error()));
  }
  if (metadata->total_length() > std::numeric_limits<std::size_t>::max()) {
    return std::unexpected(CsegProjectedReaderOpenError{
        CsegProjectedReaderOpenErrorKind::kResourceLimit,
        status(common::StatusCode::kResourceExhausted,
               "CSEG projected-reader part does not fit this platform")});
  }
  const std::size_t total_length = static_cast<std::size_t>(metadata->total_length());
  if (bytes.size() < total_length) {
    return std::unexpected(incomplete(metadata->total_length()));
  }

  std::shared_ptr<const schema::TableSchema> source_schema = lineage.find(metadata->schema_id());
  if (source_schema == nullptr) {
    return std::unexpected(
        CsegProjectedReaderOpenError{CsegProjectedReaderOpenErrorKind::kNotFound,
                                     status(common::StatusCode::kNotFound,
                                            "CSEG source schema is not retained in the lineage")});
  }
  common::Status binding =
      validate_cseg_v1_metadata_schema(*metadata, *source_schema, target_tablet);
  if (!binding.is_ok()) {
    return std::unexpected(invalid_open(binding));
  }
  std::shared_ptr<const schema::TableSchema> destination_schema =
      lineage.find(destination_schema_id);
  if (destination_schema == nullptr) {
    return std::unexpected(CsegProjectedReaderOpenError{
        CsegProjectedReaderOpenErrorKind::kNotFound,
        status(common::StatusCode::kNotFound,
               "CSEG destination schema is not retained in the lineage")});
  }
  common::Result<schema::SchemaProjection> projection = lineage.projection(
      {.ancestor_schema_id = metadata->schema_id(), .descendant_schema_id = destination_schema_id});
  if (!projection.has_value()) {
    return std::unexpected(invalid_open(projection.error()));
  }
  return CsegProjectedReaderView{std::move(*metadata),     bytes.first(total_length),
                                 std::move(source_schema), std::move(destination_schema),
                                 std::move(*projection),   limits};
}

CsegProjectedReaderOpenResult open_cseg_v1_projected_reader_exact(
    const common::ByteView bytes, const schema::SchemaLineage& lineage,
    const schema::SchemaId destination_schema_id, const schema::TabletId& target_tablet,
    const CsegProjectedReaderLimits limits) {
  CsegProjectedReaderOpenResult reader = open_cseg_v1_projected_reader_prefix(
      bytes, lineage, destination_schema_id, target_tablet, limits);
  if (!reader.has_value()) {
    return reader;
  }
  if (reader->encoded_part().size() != bytes.size()) {
    return std::unexpected(CsegProjectedReaderOpenError{
        CsegProjectedReaderOpenErrorKind::kCorruption,
        corruption("CSEG projected-reader exact open rejects trailing bytes")});
  }
  return reader;
}

} // namespace chronos::cseg
