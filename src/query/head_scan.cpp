#include "chronos/query/head_scan.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::size_t kConservativeAllocationOverheadBytes = 64U;

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status not_found(std::string message) {
  return common::Status{common::StatusCode::kNotFound, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] common::Result<std::size_t> add(const std::size_t left, const std::size_t right,
                                              const char* const message) {
  const std::optional<std::size_t> total = common::checked_add(left, right);
  if (!total.has_value())
    return common::make_unexpected(exhausted(message));
  return *total;
}

[[nodiscard]] common::Result<std::size_t>
bytes_for(const std::size_t count, const std::size_t width, const char* const message) {
  const std::optional<std::size_t> bytes = common::checked_multiply(count, width);
  if (!bytes.has_value())
    return common::make_unexpected(exhausted(message));
  return *bytes;
}

[[nodiscard]] common::Result<std::size_t> offset_bytes_for_rows(const std::size_t row_count,
                                                                const char* const message) {
  common::Result<std::size_t> offset_count = add(row_count, 1U, message);
  return offset_count.has_value() ? bytes_for(*offset_count, sizeof(std::uint32_t), message)
                                  : offset_count;
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

[[nodiscard]] common::Result<std::size_t>
source_charge(const head::HeadSnapshot& snapshot, const schema::TableSchema& destination_schema,
              const std::vector<std::uint32_t>& destination_column_ordinals) {
  std::size_t total = snapshot.retained_buffer_bytes();
  common::Result<std::size_t> projection =
      bytes_for(destination_schema.columns().size(), sizeof(schema::ProjectionEntry),
                "head scan schema-projection accounting overflowed");
  if (!projection.has_value())
    return projection;
  common::Result<std::size_t> next =
      add(total, *projection, "head scan source accounting overflowed");
  if (!next.has_value())
    return next;
  common::Result<std::size_t> ordinals =
      bytes_for(destination_column_ordinals.capacity(), sizeof(std::uint32_t),
                "head scan ordinal accounting overflowed");
  if (!ordinals.has_value())
    return ordinals;
  next = add(*next, *ordinals, "head scan source accounting overflowed");
  if (!next.has_value())
    return next;
  constexpr std::size_t objects =
      sizeof(head::HeadSnapshot) + sizeof(std::shared_ptr<const schema::TableSchema>) +
      sizeof(schema::SchemaProjection) + sizeof(std::vector<std::uint32_t>) +
      sizeof(HeadScanLimits) + sizeof(QueryMemoryReservation) + sizeof(HeadScanOperator) + 128U;
  next = add(*next, objects, "head scan source accounting overflowed");
  if (!next.has_value())
    return next;
  common::Result<std::size_t> overhead =
      bytes_for(4U, kConservativeAllocationOverheadBytes,
                "head scan source allocation accounting overflowed");
  return overhead.has_value()
             ? add(*next, *overhead, "head scan source allocation accounting overflowed")
             : overhead;
}

[[nodiscard]] common::Result<std::shared_ptr<const schema::TableSchema>> validate_head_scan_request(
    const head::HeadSnapshot& snapshot, const schema::SchemaLineage& lineage,
    const schema::SchemaId destination_schema_id, const schema::TabletId& target_tablet,
    const std::vector<std::uint32_t>& destination_column_ordinals, const HeadScanLimits limits) {
  if (limits.chunk.maximum_rows == 0U || limits.chunk.maximum_columns == 0U ||
      limits.chunk.maximum_buffer_bytes == 0U || limits.chunk.maximum_retained_buffer_bytes == 0U) {
    return common::make_unexpected(invalid("head scan chunk limits must be nonzero"));
  }
  if (snapshot.tablet_id() != target_tablet)
    return common::make_unexpected(invalid("head scan snapshot belongs to another tablet"));
  const std::shared_ptr<const schema::TableSchema> source_schema =
      lineage.find(snapshot.schema_ptr()->schema_id());
  if (source_schema == nullptr)
    return common::make_unexpected(not_found("head scan source schema is not retained"));
  if (*source_schema != *snapshot.schema_ptr() ||
      source_schema->table_id() != snapshot.table_id()) {
    return common::make_unexpected(
        invalid("head scan snapshot disagrees with its retained schema"));
  }
  std::shared_ptr<const schema::TableSchema> destination_schema =
      lineage.find(destination_schema_id);
  if (destination_schema == nullptr)
    return common::make_unexpected(not_found("head scan destination schema is not retained"));
  if (destination_schema->table_id() != snapshot.table_id()) {
    return common::make_unexpected(
        invalid("head scan destination schema belongs to another table"));
  }
  if (destination_column_ordinals.size() > limits.chunk.maximum_columns)
    return common::make_unexpected(exhausted("head scan projection exceeds its column limit"));
  std::bitset<schema::kMaximumSchemaColumnCount> seen;
  for (const std::uint32_t ordinal : destination_column_ordinals) {
    if (ordinal >= destination_schema->columns().size())
      return common::make_unexpected(invalid("head scan projection ordinal is outside the schema"));
    if (seen[ordinal])
      return common::make_unexpected(invalid("head scan projection ordinals are not unique"));
    seen[ordinal] = true;
  }
  return destination_schema;
}

[[nodiscard]] common::Status validate_column_shape(const head::HeadSnapshot& snapshot,
                                                   const std::size_t ordinal) {
  common::Result<head::HeadColumnView> view = snapshot.column(ordinal);
  if (!view.has_value())
    return internal("head scan cannot access a schema-bound source column");
  const schema::ColumnDefinition& definition = snapshot.schema_ptr()->columns()[ordinal];
  if (view->column_id() != definition.id() || view->type() != definition.type() ||
      view->nullable() != definition.nullable() || view->row_count() != snapshot.row_count()) {
    return internal("head scan source column disagrees with its pinned schema");
  }
  const std::size_t rows = snapshot.row_count();
  if (view->validity().size() != (definition.nullable() ? rows : 0U))
    return internal("head scan source validity storage has the wrong shape");
  if (definition.type().kind() == schema::LogicalTypeKind::kBool) {
    if (view->boolean_values().size() != rows || !view->fixed_values().empty() ||
        !view->variable_offsets().empty() || !view->variable_values().empty()) {
      return internal("head scan source Boolean storage has the wrong shape");
    }
    return common::Status::ok();
  }
  if (definition.type().is_variable_width()) {
    const std::optional<std::size_t> offset_count = common::checked_add(rows, std::size_t{1U});
    if (!offset_count.has_value())
      return internal("head scan source offset count overflowed");
    if (!view->boolean_values().empty() || !view->fixed_values().empty() ||
        view->variable_offsets().size() != *offset_count ||
        view->variable_values().size() > std::numeric_limits<std::uint32_t>::max()) {
      return internal("head scan source variable storage has the wrong shape");
    }
    return common::Status::ok();
  }
  common::Result<std::size_t> expected = bytes_for(rows, fixed_width(definition.type().kind()),
                                                   "head scan source column size overflowed");
  if (!expected.has_value())
    return expected.error();
  if (!view->boolean_values().empty() || !view->variable_offsets().empty() ||
      !view->variable_values().empty() || view->fixed_values().size() != *expected) {
    return internal("head scan source fixed-width storage has the wrong shape");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_projection(const head::HeadSnapshot& snapshot,
                    const schema::TableSchema& destination_schema,
                    const schema::SchemaProjection& projection,
                    const std::vector<std::uint32_t>& destination_column_ordinals) {
  for (const std::uint32_t destination_ordinal : destination_column_ordinals) {
    const schema::ProjectionEntry& entry = projection.entries()[destination_ordinal];
    const schema::ColumnDefinition& destination = destination_schema.columns()[destination_ordinal];
    if (entry.descendant_ordinal() != destination_ordinal ||
        entry.descendant_column_id() != destination.id()) {
      return internal("head scan schema projection has an inconsistent destination entry");
    }
    const std::optional<std::size_t>& projected_source = entry.ancestor_ordinal();
    if (!projected_source.has_value()) {
      if (!destination.nullable())
        return internal("head scan cannot synthesize a non-nullable destination column");
      continue;
    }
    const std::size_t source_ordinal = projected_source.value();
    if (source_ordinal >= snapshot.schema_ptr()->columns().size())
      return internal("head scan schema projection names a missing source column");
    const schema::ColumnDefinition& source = snapshot.schema_ptr()->columns()[source_ordinal];
    if (source.id() != destination.id() || source.type() != destination.type() ||
        source.nullable() != destination.nullable()) {
      return internal("head scan schema projection changes a retained column shape");
    }
    common::Status shape = validate_column_shape(snapshot, source_ordinal);
    if (!shape.is_ok())
      return shape;
  }
  return common::Status::ok();
}

struct HeadChunkPlan {
  std::uint32_t first_row{};
  std::uint32_t row_count{};
  std::size_t logical_buffer_bytes{};
  std::size_t retained_charge{};
};

[[nodiscard]] common::Result<HeadChunkPlan>
plan_chunk(const head::HeadSnapshot& snapshot, const schema::TableSchema& destination_schema,
           const schema::SchemaProjection& projection,
           const std::vector<std::uint32_t>& destination_column_ordinals,
           const std::uint32_t first_row, const std::uint32_t row_count,
           const HeadScanLimits limits) {
  common::Result<std::size_t> total =
      bytes_for(row_count, sizeof(std::uint32_t), "head scan selection accounting overflowed");
  if (!total.has_value())
    return common::make_unexpected(total.error());
  for (const std::uint32_t destination_ordinal : destination_column_ordinals) {
    const schema::ColumnDefinition& destination = destination_schema.columns()[destination_ordinal];
    if (destination.nullable()) {
      total = add(*total, columnar::bitmap_size(row_count),
                  "head scan logical buffer accounting overflowed");
      if (!total.has_value())
        return common::make_unexpected(total.error());
    }
    if (destination.type().kind() == schema::LogicalTypeKind::kBool) {
      total = add(*total, columnar::bitmap_size(row_count),
                  "head scan logical buffer accounting overflowed");
    } else if (destination.type().is_variable_width()) {
      common::Result<std::size_t> offsets =
          offset_bytes_for_rows(row_count, "head scan offset accounting overflowed");
      if (!offsets.has_value())
        return common::make_unexpected(offsets.error());
      total = add(*total, *offsets, "head scan logical buffer accounting overflowed");
      if (total.has_value()) {
        const schema::ProjectionEntry& entry = projection.entries()[destination_ordinal];
        if (entry.ancestor_ordinal().has_value()) {
          common::Result<head::HeadColumnView> source = snapshot.column(*entry.ancestor_ordinal());
          if (!source.has_value())
            return common::make_unexpected(internal("head scan lost a projected source column"));
          const std::size_t begin = source->variable_offsets()[first_row];
          const std::size_t end = source->variable_offsets()[first_row + row_count];
          if (end < begin || end > source->variable_values().size()) {
            return common::make_unexpected(
                internal("head scan source variable range is outside its published frontier"));
          }
          total = add(*total, end - begin, "head scan logical buffer accounting overflowed");
        }
      }
    } else {
      common::Result<std::size_t> values =
          bytes_for(row_count, fixed_width(destination.type().kind()),
                    "head scan fixed-width accounting overflowed");
      if (!values.has_value())
        return common::make_unexpected(values.error());
      total = add(*total, *values, "head scan logical buffer accounting overflowed");
    }
    if (!total.has_value())
      return common::make_unexpected(total.error());
  }
  if (*total > limits.chunk.maximum_buffer_bytes)
    return common::make_unexpected(exhausted("head scan output exceeds its logical-byte limit"));

  common::Result<std::size_t> containers =
      bytes_for(destination_column_ordinals.size(), sizeof(columnar::OwnedPhysicalColumn),
                "head scan output-container accounting overflowed");
  if (!containers.has_value())
    return common::make_unexpected(containers.error());
  common::Result<std::size_t> charge =
      add(*total, *containers, "head scan retained accounting overflowed");
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  const std::optional<std::size_t> column_allocations =
      common::checked_multiply(destination_column_ordinals.size(), std::size_t{3U});
  const std::optional<std::size_t> allocation_count =
      column_allocations.has_value() ? common::checked_add(*column_allocations, std::size_t{2U})
                                     : std::nullopt;
  if (!allocation_count.has_value())
    return common::make_unexpected(exhausted("head scan allocation accounting overflowed"));
  common::Result<std::size_t> overhead =
      bytes_for(*allocation_count, kConservativeAllocationOverheadBytes,
                "head scan allocation accounting overflowed");
  if (!overhead.has_value())
    return common::make_unexpected(overhead.error());
  charge = add(*charge, *overhead, "head scan retained accounting overflowed");
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  if (*charge > limits.chunk.maximum_retained_buffer_bytes) {
    return common::make_unexpected(exhausted("head scan output exceeds its retained-byte limit"));
  }
  return HeadChunkPlan{.first_row = first_row,
                       .row_count = row_count,
                       .logical_buffer_bytes = *total,
                       .retained_charge = *charge};
}

void set_bit(std::vector<std::byte>& bitmap, const std::uint32_t row) {
  bitmap[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

[[nodiscard]] common::Result<columnar::OwnedPhysicalColumn>
materialize_column(const head::HeadSnapshot& snapshot, const schema::ColumnDefinition& destination,
                   const std::optional<std::size_t> source_ordinal, const HeadChunkPlan plan) {
  columnar::ColumnVectorBuffers buffers;
  if (destination.nullable())
    buffers.validity.resize(columnar::bitmap_size(plan.row_count));
  std::uint32_t null_count = 0U;

  if (!source_ordinal.has_value()) {
    null_count = plan.row_count;
    if (destination.type().kind() == schema::LogicalTypeKind::kBool) {
      buffers.values.resize(columnar::bitmap_size(plan.row_count));
    } else if (destination.type().is_variable_width()) {
      common::Result<std::size_t> offset_bytes =
          offset_bytes_for_rows(plan.row_count, "head scan materialized offset size overflowed");
      if (!offset_bytes.has_value())
        return common::make_unexpected(offset_bytes.error());
      buffers.offsets.resize(*offset_bytes);
    } else {
      common::Result<std::size_t> value_bytes =
          bytes_for(plan.row_count, fixed_width(destination.type().kind()),
                    "head scan materialized value size overflowed");
      if (!value_bytes.has_value())
        return common::make_unexpected(value_bytes.error());
      buffers.values.resize(*value_bytes);
    }
  } else {
    common::Result<head::HeadColumnView> source = snapshot.column(*source_ordinal);
    if (!source.has_value())
      return common::make_unexpected(internal("head scan lost a materialized source column"));
    for (std::uint32_t local = 0U; local < plan.row_count; ++local) {
      const std::uint32_t source_row = plan.first_row + local;
      const std::uint8_t validity = destination.nullable() ? source->validity()[source_row] : 1U;
      if (validity > 1U)
        return common::make_unexpected(internal("head scan source validity byte is not Boolean"));
      if (validity == 0U) {
        ++null_count;
      } else if (destination.nullable()) {
        set_bit(buffers.validity, local);
      }
    }

    if (destination.type().kind() == schema::LogicalTypeKind::kBool) {
      buffers.values.resize(columnar::bitmap_size(plan.row_count));
      for (std::uint32_t local = 0U; local < plan.row_count; ++local) {
        const std::uint32_t source_row = plan.first_row + local;
        const std::uint8_t value = source->boolean_values()[source_row];
        if (value > 1U)
          return common::make_unexpected(internal("head scan source Boolean byte is invalid"));
        const bool valid = !destination.nullable() || source->validity()[source_row] != 0U;
        if (!valid && value != 0U) {
          return common::make_unexpected(
              internal("head scan source has a nonzero null Boolean value"));
        }
        if (valid && value != 0U)
          set_bit(buffers.values, local);
      }
    } else if (destination.type().is_variable_width()) {
      common::Result<std::size_t> offset_bytes =
          offset_bytes_for_rows(plan.row_count, "head scan materialized offset size overflowed");
      if (!offset_bytes.has_value())
        return common::make_unexpected(offset_bytes.error());
      buffers.offsets.resize(*offset_bytes);
      const std::uint32_t base = source->variable_offsets()[plan.first_row];
      std::uint32_t previous = base;
      for (std::uint32_t local = 0U; local <= plan.row_count; ++local) {
        const std::uint32_t value = source->variable_offsets()[plan.first_row + local];
        if (value < previous || value < base ||
            static_cast<std::size_t>(value) > source->variable_values().size()) {
          return common::make_unexpected(
              internal("head scan source variable offsets are outside their frontier"));
        }
        if (local != 0U && destination.nullable() &&
            source->validity()[plan.first_row + local - 1U] == 0U && value != previous) {
          return common::make_unexpected(
              internal("head scan source null variable row owns value bytes"));
        }
        store_u32_le(buffers.offsets, static_cast<std::size_t>(local) * sizeof(std::uint32_t),
                     value - base);
        previous = value;
      }
      const common::ByteView values = source->variable_values().subspan(base, previous - base);
      buffers.values.assign(values.begin(), values.end());
    } else {
      const std::size_t width = fixed_width(destination.type().kind());
      const std::size_t begin = static_cast<std::size_t>(plan.first_row) * width;
      const std::size_t length = static_cast<std::size_t>(plan.row_count) * width;
      const common::ByteView values = source->fixed_values().subspan(begin, length);
      buffers.values.assign(values.begin(), values.end());
    }
  }

  return columnar::OwnedPhysicalColumn::create({.type = destination.type(),
                                                .nullable = destination.nullable(),
                                                .row_count = plan.row_count,
                                                .null_count = null_count},
                                               std::move(buffers));
}

[[nodiscard]] common::Result<std::vector<columnar::OwnedPhysicalColumn>> materialize_columns(
    const head::HeadSnapshot& snapshot, const schema::TableSchema& destination_schema,
    const schema::SchemaProjection& projection,
    const std::vector<std::uint32_t>& destination_column_ordinals, const HeadChunkPlan plan) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.reserve(destination_column_ordinals.size());
  for (const std::uint32_t destination_ordinal : destination_column_ordinals) {
    const schema::ProjectionEntry& entry = projection.entries()[destination_ordinal];
    common::Result<columnar::OwnedPhysicalColumn> column =
        materialize_column(snapshot, destination_schema.columns()[destination_ordinal],
                           entry.ancestor_ordinal(), plan);
    if (!column.has_value())
      return common::make_unexpected(column.error());
    columns.push_back(std::move(*column));
  }
  return columns;
}

} // namespace

class HeadScanOperator::State {
public:
  State(head::HeadSnapshot snapshot_value,
        std::shared_ptr<const schema::TableSchema> destination_schema_value,
        schema::SchemaProjection projection_value,
        std::vector<std::uint32_t> destination_column_ordinals_value,
        const HeadScanLimits limits_value, QueryMemoryReservation reservation_value) noexcept
      : snapshot(std::move(snapshot_value)),
        destination_schema(std::move(destination_schema_value)),
        projection(std::move(projection_value)),
        destination_column_ordinals(std::move(destination_column_ordinals_value)),
        limits(limits_value), reservation(std::move(reservation_value)) {}

  head::HeadSnapshot snapshot;
  std::shared_ptr<const schema::TableSchema> destination_schema;
  schema::SchemaProjection projection;
  std::vector<std::uint32_t> destination_column_ordinals;
  HeadScanLimits limits;
  QueryMemoryReservation reservation;
  std::uint32_t next_row{};
};

HeadScanOperator::~HeadScanOperator() = default;

HeadScanOperator::HeadScanOperator(std::unique_ptr<State> state) noexcept
    : state_(std::move(state)) {}

common::Result<std::unique_ptr<PhysicalOperator>> HeadScanOperator::create(
    const QueryResourceContext& resources, head::HeadSnapshot snapshot,
    const schema::SchemaLineage& lineage, const schema::SchemaId destination_schema_id,
    const schema::TabletId& target_tablet, std::vector<std::uint32_t> destination_column_ordinals,
    const HeadScanLimits limits) {
  common::Result<std::shared_ptr<const schema::TableSchema>> destination =
      validate_head_scan_request(snapshot, lineage, destination_schema_id, target_tablet,
                                 destination_column_ordinals, limits);
  if (!destination.has_value())
    return common::make_unexpected(destination.error());
  std::shared_ptr<const schema::TableSchema> destination_schema = std::move(*destination);

  common::Result<std::size_t> charge =
      source_charge(snapshot, *destination_schema, destination_column_ordinals);
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  common::Result<QueryMemoryReservation> reservation = resources.reserve(*charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());

  try {
    common::Result<schema::SchemaProjection> projection =
        lineage.projection({.ancestor_schema_id = snapshot.schema_ptr()->schema_id(),
                            .descendant_schema_id = destination_schema_id});
    if (!projection.has_value())
      return common::make_unexpected(projection.error());
    common::Status valid = validate_projection(snapshot, *destination_schema, *projection,
                                               destination_column_ordinals);
    if (!valid.is_ok())
      return common::make_unexpected(std::move(valid));
    auto state = std::make_unique<State>(
        std::move(snapshot), std::move(destination_schema), std::move(*projection),
        std::move(destination_column_ordinals), limits, std::move(*reservation));
    return std::unique_ptr<PhysicalOperator>{new HeadScanOperator{std::move(state)}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("head scan source allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("head scan source exceeds container limits"));
  }
}

common::Result<std::unique_ptr<PhysicalOperator>> HeadScanOperator::create_event_time_filtered(
    const QueryResourceContext& resources, head::HeadSnapshot snapshot,
    const schema::SchemaLineage& lineage, const schema::SchemaId destination_schema_id,
    const schema::TabletId& target_tablet, std::vector<std::uint32_t> destination_column_ordinals,
    TimestampRangePredicate predicate, const HeadScanLimits limits) {
  common::Result<std::shared_ptr<const schema::TableSchema>> destination =
      validate_head_scan_request(snapshot, lineage, destination_schema_id, target_tablet,
                                 destination_column_ordinals, limits);
  if (!destination.has_value())
    return common::make_unexpected(destination.error());
  const std::optional<std::size_t> event_time_destination_ordinal =
      (*destination)->column_ordinal((*destination)->event_time_column());
  if (!event_time_destination_ordinal.has_value())
    return common::make_unexpected(
        invalid("head scan destination schema has no event-time column"));

  const std::uint32_t event_time_ordinal =
      static_cast<std::uint32_t>(*event_time_destination_ordinal);
  const auto requested = std::ranges::find(destination_column_ordinals, event_time_ordinal);
  const bool append_event_time_helper = requested == destination_column_ordinals.end();
  const std::size_t event_time_output_ordinal =
      append_event_time_helper
          ? destination_column_ordinals.size()
          : static_cast<std::size_t>(requested - destination_column_ordinals.begin());
  if (append_event_time_helper &&
      destination_column_ordinals.size() >= limits.chunk.maximum_columns) {
    return common::make_unexpected(
        exhausted("head scan exact predicate helper exceeds its column limit"));
  }

  try {
    if (append_event_time_helper)
      destination_column_ordinals.push_back(event_time_ordinal);
    common::Result<std::unique_ptr<PhysicalOperator>> source =
        create(resources, std::move(snapshot), lineage, destination_schema_id, target_tablet,
               std::move(destination_column_ordinals), limits);
    if (!source.has_value())
      return common::make_unexpected(source.error());
    source = TimestampRangeFilterOperator::create(std::move(*source), event_time_output_ordinal,
                                                  predicate);
    if (!source.has_value())
      return common::make_unexpected(source.error());
    if (!append_event_time_helper)
      return source;

    std::vector<std::size_t> visible_columns(event_time_output_ordinal);
    for (std::size_t ordinal = 0U; ordinal < visible_columns.size(); ++ordinal)
      visible_columns[ordinal] = ordinal;
    return ColumnSubsetOperator::create(std::move(*source), std::move(visible_columns));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("head scan exact source allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("head scan exact source exceeds container limits"));
  }
}

common::Result<PhysicalOperatorStep> HeadScanOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());
  if (!resources.owns(state_->reservation)) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(invalid("head scan source belongs to another query"));
  }
  if (state_->next_row >= state_->snapshot.row_count()) {
    ended_ = true;
    state_.reset();
    return PhysicalOperatorStep::end();
  }
  const std::uint32_t remaining = state_->snapshot.row_count() - state_->next_row;
  const std::uint32_t row_count = std::min(remaining, state_->limits.chunk.maximum_rows);
  common::Result<HeadChunkPlan> plan =
      plan_chunk(state_->snapshot, *state_->destination_schema, state_->projection,
                 state_->destination_column_ordinals, state_->next_row, row_count, state_->limits);
  if (!plan.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(plan.error());
  }
  common::Result<QueryMemoryReservation> reservation = resources.reserve(plan->retained_charge);
  if (!reservation.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(reservation.error());
  }

  try {
    common::Result<std::vector<columnar::OwnedPhysicalColumn>> columns =
        materialize_columns(state_->snapshot, *state_->destination_schema, state_->projection,
                            state_->destination_column_ordinals, *plan);
    if (!columns.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(columns.error());
    }
    common::Result<void> still_active = resources.check_cancelled();
    if (!still_active.has_value())
      return common::make_unexpected(still_active.error());
    common::Result<VectorSelection> selection = VectorSelection::all(plan->row_count);
    if (!selection.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(selection.error());
    }
    common::Result<VectorChunk> chunk =
        VectorChunk::create(std::move(*columns), std::move(*selection), state_->limits.chunk);
    if (!chunk.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(chunk.error());
    }
    if (chunk->buffer_bytes() != plan->logical_buffer_bytes) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(
          internal("head scan materialization disagrees with its logical-byte plan"));
    }
    common::Result<AccountedVectorChunk> accounted =
        AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation), resources);
    if (!accounted.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(accounted.error());
    }

    state_->next_row += plan->row_count;
    if (state_->next_row == state_->snapshot.row_count()) {
      ended_ = true;
      state_.reset();
    }
    return PhysicalOperatorStep::chunk(std::move(*accounted));
  } catch (const std::bad_alloc&) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(exhausted("head scan output allocation failed"));
  } catch (const std::length_error&) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(exhausted("head scan output exceeds container limits"));
  }
}

} // namespace chronos::query
