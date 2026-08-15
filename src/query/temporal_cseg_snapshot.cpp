#include "chronos/query/temporal_cseg_snapshot.hpp"

#include "chronos/query/value.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <numeric>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return common::Status{code, message};
}

[[nodiscard]] common::Status invalid(const char* message) {
  return status(common::StatusCode::kInvalidArgument, message);
}

[[nodiscard]] common::Status corruption(const char* message) {
  return status(common::StatusCode::kCorruption, message);
}

[[nodiscard]] common::Result<common::ByteView>
cell_bytes(const columnar::PhysicalColumnView& column, const std::uint32_t row) {
  const common::Result<columnar::ColumnCellView> cell = column.cell(row);
  if (!cell.has_value() || cell->is_null()) {
    return common::make_unexpected(corruption("temporal CSEG system cell is inaccessible or null"));
  }
  const common::Result<common::ByteView> bytes = cell->bytes();
  return bytes.has_value()
             ? bytes
             : common::make_unexpected(corruption("temporal CSEG system cell is not byte-valued"));
}

template <typename Unsigned>
[[nodiscard]] common::Result<Unsigned> load_unsigned(const columnar::PhysicalColumnView& column,
                                                     const std::uint32_t row) {
  const common::Result<common::ByteView> bytes = cell_bytes(column, row);
  if (!bytes.has_value() || bytes->size() != sizeof(Unsigned)) {
    return common::make_unexpected(corruption("temporal CSEG system cell has invalid width"));
  }
  Unsigned value = 0U;
  for (std::size_t index = 0U; index < bytes->size(); ++index) {
    value = static_cast<Unsigned>(
        value |
        static_cast<Unsigned>(static_cast<Unsigned>(std::to_integer<std::uint8_t>((*bytes)[index]))
                              << (index * 8U)));
  }
  return value;
}

[[nodiscard]] common::Result<std::int64_t>
load_timestamp(const columnar::PhysicalColumnView& column, const std::uint32_t row) {
  const common::Result<std::uint64_t> bits = load_unsigned<std::uint64_t>(column, row);
  return bits.has_value() ? common::Result<std::int64_t>{std::bit_cast<std::int64_t>(*bits)}
                          : common::make_unexpected(bits.error());
}

struct Winner {
  const cseg::ProjectedCsegGranule* granule;
  std::uint32_t row;
  std::vector<std::byte> identity;
  std::uint64_t commit_position;
  std::uint32_t row_ordinal;
  std::int64_t commit_time;
  cseg::temporal_format::Operation operation;
};

[[nodiscard]] common::Result<TemporalMutationKind> mutation_kind(const std::uint8_t operation) {
  switch (static_cast<cseg::temporal_format::Operation>(operation)) {
  case cseg::temporal_format::Operation::kOriginal:
    return TemporalMutationKind::kOriginal;
  case cseg::temporal_format::Operation::kCorrection:
    return TemporalMutationKind::kCorrection;
  case cseg::temporal_format::Operation::kReplacement:
    return TemporalMutationKind::kReplacement;
  case cseg::temporal_format::Operation::kTombstone:
    return TemporalMutationKind::kTombstone;
  }
  return common::make_unexpected(corruption("temporal CSEG operation is unsupported"));
}

[[nodiscard]] common::Status retained_restore_error(common::Status error) {
  if (error.code() == common::StatusCode::kInvalidArgument ||
      error.code() == common::StatusCode::kAlreadyExists) {
    return status(common::StatusCode::kCorruption,
                  "retained temporal CSEG history violates mutation ordering");
  }
  return error;
}

[[nodiscard]] common::Status
validate_manifest_temporal_tablet_view(const schema::TableSchema& schema_value,
                                       const manifest::TemporalTabletDescriptor& tablet,
                                       const std::span<const TemporalManifestCsegPartView> parts,
                                       const TemporalCsegSourceLineage source) {
  if (tablet.table_id != schema_value.table_id() || tablet.tablet_id.uuid().is_nil() ||
      tablet.commit_source != source.source || tablet.source_id != source.source_id ||
      tablet.part_count != parts.size() || parts.empty()) {
    return invalid("Manifest temporal tablet view is incomplete or belongs to another owner");
  }
  std::uint64_t version_count = 0U;
  for (std::size_t index = 0U; index < parts.size(); ++index) {
    const manifest::TemporalPartDescriptor* descriptor = parts[index].descriptor;
    if (descriptor == nullptr || descriptor->table_id != tablet.table_id ||
        descriptor->tablet_id != tablet.tablet_id ||
        descriptor->commit_source != tablet.commit_source ||
        descriptor->source_id != tablet.source_id ||
        descriptor->maximum_commit_position > tablet.durable_position ||
        descriptor->row_count > tablet.durable_version_count - version_count ||
        (index != 0U && !(parts[index - 1U].descriptor->part_id < descriptor->part_id))) {
      return invalid("Manifest temporal tablet part coverage is noncanonical or inconsistent");
    }
    version_count += descriptor->row_count;
  }
  return version_count == tablet.durable_version_count
             ? common::Status::ok()
             : invalid("Manifest temporal tablet version count is not completely covered");
}

[[nodiscard]] bool same_identity(const std::vector<std::byte>& left,
                                 const common::ByteView right) noexcept {
  return std::ranges::equal(left, right);
}

[[nodiscard]] common::Result<std::vector<cseg::ProjectedCsegGranule>>
decode_manifest_temporal_parts(const std::shared_ptr<const schema::TableSchema>& schema_value,
                               const schema::SchemaLineage& lineage,
                               const schema::TabletId& tablet_id,
                               const std::span<const TemporalManifestCsegPartView> parts,
                               const TemporalCsegSourceLineage source,
                               const std::optional<std::int64_t> prune_after_system_time_ns,
                               const TemporalManifestCsegResolutionLimits limits) {
  std::vector<std::uint32_t> projection(schema_value->columns().size());
  std::iota(projection.begin(), projection.end(), std::uint32_t{0U});
  std::vector<cseg::ProjectedCsegGranule> decoded_granules;
  decoded_granules.reserve(std::min(parts.size(), limits.maximum_granules));
  std::uint64_t decoded_buffer_bytes = 0U;

  for (const TemporalManifestCsegPartView& image : parts) {
    if (image.descriptor == nullptr || image.bytes.empty()) {
      return common::make_unexpected(
          invalid("Manifest temporal CSEG part image is missing its descriptor or bytes"));
    }
    const manifest::TemporalPartDescriptor& descriptor = *image.descriptor;
    if (descriptor.table_id != schema_value->table_id() || descriptor.tablet_id != tablet_id ||
        descriptor.commit_source != source.source || descriptor.source_id != source.source_id) {
      return common::make_unexpected(
          invalid("Manifest temporal CSEG part belongs to another table, tablet, or source"));
    }
    if (prune_after_system_time_ns.has_value() &&
        descriptor.minimum_system_time > *prune_after_system_time_ns) {
      continue;
    }
    cseg::CsegProjectedReaderOpenResult reader = cseg::open_cseg_v2_temporal_projected_reader_exact(
        image.bytes, lineage, schema_value->schema_id(), tablet_id, limits.reader);
    if (!reader.has_value()) {
      return common::make_unexpected(reader.error().status());
    }
    for (std::size_t granule = 0U; granule < reader->metadata().granules().size(); ++granule) {
      if (decoded_granules.size() >= limits.maximum_granules) {
        return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                              "Manifest temporal CSEG granule limit is exhausted"));
      }
      const common::Result<cseg::CsegProjectedGranuleReadPlan> plan =
          reader->plan_granule(granule, projection);
      if (!plan.has_value()) {
        return common::make_unexpected(plan.error());
      }
      if (plan->decoded_buffer_bytes() >
          limits.maximum_decoded_buffer_bytes - decoded_buffer_bytes) {
        return common::make_unexpected(
            status(common::StatusCode::kResourceExhausted,
                   "Manifest temporal CSEG decoded-byte limit is exhausted"));
      }
      common::Result<cseg::ProjectedCsegGranule> decoded = reader->read_granule(*plan);
      if (!decoded.has_value()) {
        return common::make_unexpected(decoded.error());
      }
      decoded_buffer_bytes += plan->decoded_buffer_bytes();
      decoded_granules.push_back(std::move(*decoded));
    }
  }
  return decoded_granules;
}

} // namespace

common::Result<std::shared_ptr<const ScalarTableSnapshot>>
resolve_cseg_v2_temporal_snapshot(const std::shared_ptr<const schema::TableSchema>& schema_value,
                                  const std::span<const cseg::ProjectedCsegGranule* const> granules,
                                  const TemporalCsegSourceLineage lineage,
                                  const std::optional<std::int64_t> as_of_system_time_ns,
                                  const TemporalCsegResolutionLimits limits) {
  if (schema_value == nullptr || lineage.source_id.is_nil() || limits.maximum_versions == 0U ||
      limits.maximum_output_rows == 0U || limits.maximum_identity_bytes == 0U ||
      limits.maximum_identity_bytes > cseg::temporal_format::kMaximumLogicalIdentityBytes ||
      (lineage.source != cseg::temporal_format::CommitSource::kWal &&
       lineage.source != cseg::temporal_format::CommitSource::kRaft)) {
    return common::make_unexpected(invalid("temporal CSEG resolution input or limits are invalid"));
  }

  try {
    std::vector<Winner> winners;
    winners.reserve(std::min(granules.size(), limits.maximum_output_rows));
    std::size_t version_count = 0U;
    std::uint64_t visible_position = 0U;
    for (const cseg::ProjectedCsegGranule* granule : granules) {
      if (granule == nullptr || granule->schema_ptr()->schema_id() != schema_value->schema_id() ||
          granule->schema_ptr()->version() != schema_value->version() ||
          granule->columns().size() != schema_value->columns().size()) {
        return common::make_unexpected(
            invalid("temporal CSEG granule schema or projection is invalid"));
      }
      for (std::size_t column = 0U; column < granule->columns().size(); ++column) {
        if (granule->columns()[column].column_id() != schema_value->columns()[column].id()) {
          return common::make_unexpected(
              invalid("temporal CSEG projection is not in complete schema order"));
        }
      }
      if (granule->row_count() > limits.maximum_versions - version_count) {
        return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                              "temporal CSEG version limit is exhausted"));
      }
      version_count += granule->row_count();

      for (std::uint32_t row = 0U; row < granule->row_count(); ++row) {
        const common::Result<std::uint8_t> source =
            load_unsigned<std::uint8_t>(granule->commit_source(), row);
        const common::Result<common::ByteView> source_id = cell_bytes(granule->source_id(), row);
        const common::Result<std::uint64_t> position =
            load_unsigned<std::uint64_t>(granule->commit_position(), row);
        const common::Result<std::uint32_t> row_ordinal =
            load_unsigned<std::uint32_t>(granule->temporal_row_ordinal(), row);
        const common::Result<std::uint8_t> operation =
            load_unsigned<std::uint8_t>(granule->temporal_operation(), row);
        const common::Result<common::ByteView> identity =
            cell_bytes(granule->logical_identity(), row);
        const common::Result<std::int64_t> commit_time =
            load_timestamp(granule->system_commit_time(), row);
        if (!source.has_value() || !source_id.has_value() || !position.has_value() ||
            !row_ordinal.has_value() || !operation.has_value() || !identity.has_value() ||
            !commit_time.has_value()) {
          return common::make_unexpected(
              corruption("validated temporal CSEG row became inaccessible"));
        }
        if (*source != static_cast<std::uint8_t>(lineage.source) ||
            source_id->size() != common::Uuid::kSize ||
            !std::ranges::equal(*source_id, lineage.source_id.bytes())) {
          return common::make_unexpected(
              invalid("temporal CSEG row belongs to another source lineage"));
        }
        if (identity->empty() || identity->size() > limits.maximum_identity_bytes) {
          return common::make_unexpected(
              corruption("temporal CSEG identity exceeds resolution limits"));
        }
        if (as_of_system_time_ns.has_value() && *commit_time > *as_of_system_time_ns) {
          continue;
        }
        visible_position = std::max(visible_position, *position);
        const auto existing = std::ranges::find_if(winners, [&](const Winner& candidate) {
          return same_identity(candidate.identity, *identity);
        });
        const auto typed_operation = static_cast<cseg::temporal_format::Operation>(*operation);
        if (existing == winners.end()) {
          if (winners.size() >= limits.maximum_output_rows) {
            return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                                  "temporal CSEG output-row limit is exhausted"));
          }
          winners.push_back(Winner{granule,
                                   row,
                                   {identity->begin(), identity->end()},
                                   *position,
                                   *row_ordinal,
                                   *commit_time,
                                   typed_operation});
          continue;
        }
        if (existing->commit_time == *commit_time && existing->commit_position == *position) {
          return common::make_unexpected(
              corruption("temporal CSEG contains duplicate physical versions for one identity"));
        }
        if (*commit_time > existing->commit_time ||
            (*commit_time == existing->commit_time && *position > existing->commit_position)) {
          existing->granule = granule;
          existing->row = row;
          existing->commit_position = *position;
          existing->row_ordinal = *row_ordinal;
          existing->commit_time = *commit_time;
          existing->operation = typed_operation;
        }
      }
    }

    std::vector<ScalarInputRow> rows;
    if (visible_position == 0U && as_of_system_time_ns.has_value()) {
      return common::make_unexpected(
          status(common::StatusCode::kNotFound, "requested system history is not retained"));
    }
    rows.reserve(winners.size());
    for (const Winner& winner : winners) {
      if (winner.operation == cseg::temporal_format::Operation::kTombstone) {
        continue;
      }
      std::vector<ScalarValue> columns;
      columns.reserve(winner.granule->columns().size());
      for (const cseg::CsegProjectedColumnView& projected : winner.granule->columns()) {
        const common::Result<columnar::ColumnCellView> cell = projected.physical().cell(winner.row);
        if (!cell.has_value()) {
          return common::make_unexpected(cell.error());
        }
        common::Result<ScalarValue> scalar =
            ScalarValue::from_column_cell(projected.physical().type(), *cell);
        if (!scalar.has_value()) {
          return common::make_unexpected(scalar.error());
        }
        columns.push_back(std::move(*scalar));
      }
      rows.push_back(
          ScalarInputRow{.columns = std::move(columns),
                         .generated_logical_identity = schema_value->deduplication_key().empty()
                                                           ? winner.identity
                                                           : std::vector<std::byte>{},
                         .wal_id = lineage.source_id,
                         .record_sequence = winner.commit_position,
                         .system_commit_position = winner.commit_position,
                         .row_ordinal = winner.row_ordinal});
    }
    common::Result<ScalarTableSnapshot> snapshot =
        ScalarTableSnapshot::create(schema_value, visible_position, std::move(rows));
    if (!snapshot.has_value()) {
      return common::make_unexpected(snapshot.error());
    }
    return std::make_shared<const ScalarTableSnapshot>(std::move(*snapshot));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "temporal CSEG resolution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "temporal CSEG resolution exceeded container limits"));
  }
}

common::Result<std::shared_ptr<const ScalarTableSnapshot>>
resolve_manifest_v2_temporal_tablet_snapshot(
    const std::shared_ptr<const schema::TableSchema>& schema_value,
    const schema::SchemaLineage& lineage, const manifest::TemporalTabletDescriptor& tablet,
    const std::span<const TemporalManifestCsegPartView> parts,
    const TemporalCsegSourceLineage source, const std::optional<std::int64_t> as_of_system_time_ns,
    const TemporalManifestCsegResolutionLimits limits) {
  if (schema_value == nullptr || schema_value->table_id() != lineage.table_id() ||
      parts.size() > limits.maximum_parts || limits.maximum_parts == 0U ||
      limits.maximum_granules == 0U || limits.maximum_decoded_buffer_bytes == 0U) {
    return common::make_unexpected(
        invalid("Manifest temporal CSEG resolution input or limits are invalid"));
  }
  common::Status tablet_status =
      validate_manifest_temporal_tablet_view(*schema_value, tablet, parts, source);
  if (!tablet_status.is_ok()) {
    return common::make_unexpected(std::move(tablet_status));
  }
  try {
    common::Result<std::vector<cseg::ProjectedCsegGranule>> decoded_granules =
        decode_manifest_temporal_parts(schema_value, lineage, tablet.tablet_id, parts, source,
                                       as_of_system_time_ns, limits);
    if (!decoded_granules.has_value()) {
      return common::make_unexpected(decoded_granules.error());
    }

    std::vector<const cseg::ProjectedCsegGranule*> granule_views;
    granule_views.reserve(decoded_granules->size());
    for (const cseg::ProjectedCsegGranule& granule : *decoded_granules) {
      granule_views.push_back(&granule);
    }
    return resolve_cseg_v2_temporal_snapshot(schema_value, granule_views, source,
                                             as_of_system_time_ns, limits.resolution);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Manifest temporal CSEG allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Manifest temporal CSEG container limit was exceeded"));
  }
}

common::Result<std::unique_ptr<TemporalSnapshotProvider>>
restore_manifest_v2_temporal_tablet_history(
    const std::shared_ptr<const schema::TableSchema>& schema_value,
    const schema::SchemaLineage& lineage, const manifest::TemporalTabletDescriptor& tablet,
    const std::span<const TemporalManifestCsegPartView> parts,
    const TemporalCsegSourceLineage source, const std::int64_t retained_system_time_ns,
    const TemporalStoreLimits store_limits, const TemporalManifestCsegResolutionLimits limits) {
  if (schema_value == nullptr || schema_value->table_id() != lineage.table_id() || parts.empty() ||
      parts.size() > limits.maximum_parts || limits.maximum_parts == 0U ||
      limits.maximum_granules == 0U || limits.maximum_decoded_buffer_bytes == 0U ||
      limits.resolution.maximum_versions == 0U || source.source_id.is_nil() ||
      (source.source != cseg::temporal_format::CommitSource::kWal &&
       source.source != cseg::temporal_format::CommitSource::kRaft)) {
    return common::make_unexpected(
        invalid("Manifest temporal CSEG restore input or limits are invalid"));
  }
  common::Status tablet_status =
      validate_manifest_temporal_tablet_view(*schema_value, tablet, parts, source);
  if (!tablet_status.is_ok()) {
    return common::make_unexpected(std::move(tablet_status));
  }
  try {
    common::Result<std::unique_ptr<TemporalSnapshotProvider>> provider =
        TemporalSnapshotProvider::create(schema_value, store_limits);
    if (!provider.has_value()) {
      return common::make_unexpected(provider.error());
    }
    common::Result<std::vector<cseg::ProjectedCsegGranule>> decoded_granules =
        decode_manifest_temporal_parts(schema_value, lineage, tablet.tablet_id, parts, source,
                                       std::nullopt, limits);
    if (!decoded_granules.has_value()) {
      return common::make_unexpected(decoded_granules.error());
    }
    const std::optional<std::size_t> event_time_ordinal =
        schema_value->column_ordinal(schema_value->event_time_column());
    if (!event_time_ordinal.has_value()) {
      return common::make_unexpected(
          corruption("restored temporal schema lost its event-time column"));
    }

    std::vector<RetainedTemporalVersion> versions;
    for (const cseg::ProjectedCsegGranule& granule : *decoded_granules) {
      if (granule.row_count() > limits.resolution.maximum_versions - versions.size()) {
        return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                              "temporal CSEG restore version limit is exhausted"));
      }
      versions.reserve(versions.size() + granule.row_count());
      for (std::uint32_t row = 0U; row < granule.row_count(); ++row) {
        const common::Result<std::uint8_t> row_source =
            load_unsigned<std::uint8_t>(granule.commit_source(), row);
        const common::Result<common::ByteView> source_id = cell_bytes(granule.source_id(), row);
        const common::Result<std::uint64_t> position =
            load_unsigned<std::uint64_t>(granule.commit_position(), row);
        const common::Result<std::uint32_t> row_ordinal =
            load_unsigned<std::uint32_t>(granule.temporal_row_ordinal(), row);
        const common::Result<std::uint8_t> operation =
            load_unsigned<std::uint8_t>(granule.temporal_operation(), row);
        const common::Result<common::ByteView> identity =
            cell_bytes(granule.logical_identity(), row);
        const common::Result<std::int64_t> receive_time =
            load_timestamp(granule.receive_time(), row);
        const common::Result<std::int64_t> commit_time =
            load_timestamp(granule.system_commit_time(), row);
        if (!row_source.has_value() || !source_id.has_value() || !position.has_value() ||
            !row_ordinal.has_value() || !operation.has_value() || !identity.has_value() ||
            !receive_time.has_value() || !commit_time.has_value()) {
          return common::make_unexpected(
              corruption("validated temporal CSEG restore row became inaccessible"));
        }
        if (*row_source != static_cast<std::uint8_t>(source.source) ||
            source_id->size() != common::Uuid::kSize ||
            !std::ranges::equal(*source_id, source.source_id.bytes())) {
          return common::make_unexpected(
              corruption("temporal CSEG restore row changed source lineage"));
        }
        if (identity->size() > store_limits.maximum_identity_bytes) {
          return common::make_unexpected(
              status(common::StatusCode::kResourceExhausted,
                     "temporal CSEG restore identity limit is exhausted"));
        }
        const common::Result<TemporalMutationKind> kind = mutation_kind(*operation);
        if (!kind.has_value()) {
          return common::make_unexpected(kind.error());
        }

        std::vector<ScalarValue> columns;
        columns.reserve(granule.columns().size());
        for (const cseg::CsegProjectedColumnView& projected : granule.columns()) {
          const common::Result<columnar::ColumnCellView> cell = projected.physical().cell(row);
          if (!cell.has_value()) {
            return common::make_unexpected(cell.error());
          }
          common::Result<ScalarValue> scalar =
              ScalarValue::from_column_cell(projected.physical().type(), *cell);
          if (!scalar.has_value()) {
            return common::make_unexpected(scalar.error());
          }
          columns.push_back(std::move(*scalar));
        }
        const auto* event_time = std::get_if<std::int64_t>(&columns[*event_time_ordinal].storage());
        if (event_time == nullptr) {
          return common::make_unexpected(
              corruption("temporal CSEG event-time value is inaccessible"));
        }
        versions.push_back(RetainedTemporalVersion{
            .system_commit_position = *position,
            .system_commit_time_ns = *commit_time,
            .mutation = TemporalMutation{.logical_identity = {identity->begin(), identity->end()},
                                         .columns = std::move(columns),
                                         .event_time_ns = *event_time,
                                         .receive_time_ns = *receive_time,
                                         .wal_id = source.source_id,
                                         .record_sequence = *position,
                                         .row_ordinal = *row_ordinal,
                                         .kind = *kind}});
      }
    }
    std::ranges::sort(
        versions, [](const RetainedTemporalVersion& left, const RetainedTemporalVersion& right) {
          if (left.system_commit_position != right.system_commit_position) {
            return left.system_commit_position < right.system_commit_position;
          }
          if (left.mutation.row_ordinal != right.mutation.row_ordinal) {
            return left.mutation.row_ordinal < right.mutation.row_ordinal;
          }
          return std::lexicographical_compare(
              left.mutation.logical_identity.begin(), left.mutation.logical_identity.end(),
              right.mutation.logical_identity.begin(), right.mutation.logical_identity.end());
        });
    if (versions.empty() || retained_system_time_ns > versions.back().system_commit_time_ns) {
      return common::make_unexpected(
          invalid("Manifest temporal retained boundary is outside restored history"));
    }
    common::Status restored =
        (*provider)->restore_retained_history(retained_system_time_ns, std::move(versions));
    if (!restored.is_ok()) {
      return common::make_unexpected(retained_restore_error(std::move(restored)));
    }
    return std::move(*provider);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Manifest temporal CSEG restore allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted,
               "Manifest temporal CSEG restore exceeded container limits"));
  }
}

} // namespace chronos::query
