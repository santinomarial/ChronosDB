#include "chronos/manifest/checkpoint_builder.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/wal/wal_recovery.hpp"
#include "chronos/wal/wal_replay_sink.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
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

[[nodiscard]] common::Status decode_failure(const cseg::CsegPartDecodeError& error) {
  return error.kind() == cseg::CsegPartDecodeErrorKind::kIncomplete
             ? corruption("Manifest checkpoint referenced an incomplete CSEG image")
             : error.status();
}

[[nodiscard]] common::Result<common::ByteView>
cell_bytes(const columnar::PhysicalColumnView& column, const std::uint32_t row) {
  const common::Result<columnar::ColumnCellView> cell = column.cell(row);
  if (!cell.has_value() || cell->is_null()) {
    return common::make_unexpected(corruption("Checkpoint system cell is inaccessible or null"));
  }
  const common::Result<common::ByteView> bytes = cell->bytes();
  if (!bytes.has_value()) {
    return common::make_unexpected(corruption("Checkpoint system cell is not byte-valued"));
  }
  return *bytes;
}

template <typename Unsigned>
[[nodiscard]] common::Result<Unsigned> load_le(const common::ByteView bytes) {
  if (bytes.size() != sizeof(Unsigned)) {
    return common::make_unexpected(corruption("Checkpoint system cell has an invalid width"));
  }
  Unsigned value = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    const Unsigned byte = static_cast<Unsigned>(std::to_integer<std::uint8_t>(bytes[index]));
    const Unsigned shifted = static_cast<Unsigned>(byte << (index * 8U));
    value = static_cast<Unsigned>(value | shifted);
  }
  return value;
}

struct RowLocation {
  std::uint64_t sequence{};
  std::uint32_t ordinal{};
  std::size_t part_index{};
  std::size_t granule_index{};
  std::uint32_t local_row{};
};

struct PartIndex {
  cseg::DecodedCsegPartView decoded;
  std::reference_wrapper<const PartDescriptor> descriptor;

  PartIndex(cseg::DecodedCsegPartView decoded_value,
            const PartDescriptor& descriptor_value) noexcept
      : decoded(std::move(decoded_value)), descriptor(std::cref(descriptor_value)) {}
  PartIndex(PartIndex&&) noexcept = default;
  PartIndex& operator=(PartIndex&&) noexcept = delete;
  PartIndex(const PartIndex&) = delete;
  PartIndex& operator=(const PartIndex&) = delete;
};

struct CoverageIndex {
  std::vector<PartIndex> parts;
  std::vector<RowLocation> rows;
};

[[nodiscard]] common::Result<CoverageIndex>
build_index(const DecodedManifestView& candidate, const std::span<const ReferencedPartImage> images,
            const cseg::CsegMetadataDecodeLimits limits) {
  CoverageIndex index;
  index.parts.reserve(images.size());
  std::uint64_t total_rows = 0U;
  for (const PartDescriptor& part : candidate.parts()) {
    const std::optional<std::uint64_t> next = common::checked_add(total_rows, part.row_count);
    if (!next.has_value()) {
      return common::make_unexpected(exhausted("Checkpoint CSEG row count overflowed"));
    }
    total_rows = *next;
  }
  if (total_rows > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(exhausted("Checkpoint CSEG rows do not fit this platform"));
  }
  index.rows.reserve(static_cast<std::size_t>(total_rows));
  for (std::size_t part_index = 0U; part_index < images.size(); ++part_index) {
    cseg::CsegPartDecodeResult decoded =
        cseg::decode_cseg_v1_part_exact(images[part_index].bytes, limits);
    if (!decoded.has_value()) {
      return common::make_unexpected(decode_failure(decoded.error()));
    }
    index.parts.emplace_back(std::move(*decoded), candidate.parts()[part_index]);
    const cseg::DecodedCsegMetadataView& metadata = index.parts.back().decoded.metadata();
    const std::size_t user_count = metadata.columns().size() - cseg::format::kSystemColumnCount;
    for (std::size_t granule_index = 0U; granule_index < metadata.granules().size();
         ++granule_index) {
      const cseg::CsegGranuleDescriptor& granule = metadata.granules()[granule_index];
      const std::size_t first_page = static_cast<std::size_t>(granule.first_page_index);
      common::Result<cseg::DecodedCsegPage> sequences =
          index.parts.back().decoded.decode_page(first_page + user_count + 1U);
      common::Result<cseg::DecodedCsegPage> ordinals =
          index.parts.back().decoded.decode_page(first_page + user_count + 2U);
      if (!sequences.has_value() || !ordinals.has_value()) {
        return common::make_unexpected(
            corruption("Validated checkpoint CSEG system pages no longer decode"));
      }
      for (std::uint32_t row = 0U; row < granule.row_count; ++row) {
        const common::Result<common::ByteView> sequence_bytes =
            cell_bytes(sequences->physical(), row);
        const common::Result<common::ByteView> ordinal_bytes =
            cell_bytes(ordinals->physical(), row);
        if (!sequence_bytes.has_value() || !ordinal_bytes.has_value()) {
          return common::make_unexpected(
              corruption("Validated checkpoint CSEG system cells no longer decode"));
        }
        const common::Result<std::uint64_t> sequence = load_le<std::uint64_t>(*sequence_bytes);
        const common::Result<std::uint32_t> ordinal = load_le<std::uint32_t>(*ordinal_bytes);
        if (!sequence.has_value() || !ordinal.has_value()) {
          return common::make_unexpected(
              corruption("Validated checkpoint CSEG system values no longer decode"));
        }
        index.rows.push_back({.sequence = *sequence,
                              .ordinal = *ordinal,
                              .part_index = part_index,
                              .granule_index = granule_index,
                              .local_row = row});
      }
    }
  }
  std::ranges::sort(index.rows, [](const RowLocation& left, const RowLocation& right) {
    return std::pair{left.sequence, left.ordinal} < std::pair{right.sequence, right.ordinal};
  });
  for (std::size_t row = 1U; row < index.rows.size(); ++row) {
    if (index.rows[row - 1U].sequence == index.rows[row].sequence &&
        index.rows[row - 1U].ordinal == index.rows[row].ordinal) {
      return common::make_unexpected(
          corruption("Manifest checkpoint contains a duplicate WAL row identity"));
    }
  }
  return index;
}

[[nodiscard]] bool cell_equal(const columnar::ColumnCellView& left,
                              const columnar::ColumnCellView& right,
                              const schema::LogicalTypeKind kind) {
  if (left.is_null() || right.is_null()) {
    return left.is_null() == right.is_null();
  }
  if (kind == schema::LogicalTypeKind::kBool) {
    const common::Result<bool> lhs = left.boolean();
    const common::Result<bool> rhs = right.boolean();
    return lhs.has_value() && rhs.has_value() && *lhs == *rhs;
  }
  const common::Result<common::ByteView> lhs = left.bytes();
  const common::Result<common::ByteView> rhs = right.bytes();
  return lhs.has_value() && rhs.has_value() && std::ranges::equal(*lhs, *rhs);
}

[[nodiscard]] const TabletDescriptor* find_tablet(const DecodedManifestView& manifest,
                                                  const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::lower_bound(manifest.tablets(), tablet_id, {},
                               [](const TabletDescriptor& tablet) { return tablet.tablet_id; });
  return found != manifest.tablets().end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

[[nodiscard]] const RetryDescriptor* find_retry(const DecodedManifestView& manifest,
                                                const ingest::ClientId& client_id,
                                                const ingest::ClientBatchId& batch_id) noexcept {
  const auto key = std::pair{client_id, batch_id};
  const auto found =
      std::ranges::lower_bound(manifest.retries(), key, {}, [](const RetryDescriptor& retry) {
        return std::pair{retry.client_id, retry.client_batch_id};
      });
  return found != manifest.retries().end() && found->client_id == client_id &&
                 found->client_batch_id == batch_id
             ? &*found
             : nullptr;
}

[[nodiscard]] const PartDescriptor* find_part(const DecodedManifestView& manifest,
                                              const TabletDescriptor& tablet,
                                              const cseg::PartId& part_id) noexcept {
  const std::span<const PartDescriptor> parts =
      manifest.parts().subspan(static_cast<std::size_t>(tablet.first_part_index),
                               static_cast<std::size_t>(tablet.part_count));
  const auto found = std::ranges::lower_bound(
      parts, part_id, {}, [](const PartDescriptor& part) { return part.part_id; });
  return found != parts.end() && found->part_id == part_id ? &*found : nullptr;
}

[[nodiscard]] common::Status
validate_incremental_coverage(const ManifestCheckpointBuildInput& input) {
  const DecodedManifestView& predecessor = input.predecessor.get();
  const DecodedManifestView& candidate = input.candidate.get();
  for (const TabletDescriptor& tablet : candidate.tablets()) {
    const TabletDescriptor* old_tablet = find_tablet(predecessor, tablet.tablet_id);
    if (old_tablet == nullptr) {
      continue;
    }
    const std::span<const PartDescriptor> parts =
        candidate.parts().subspan(static_cast<std::size_t>(tablet.first_part_index),
                                  static_cast<std::size_t>(tablet.part_count));
    for (const PartDescriptor& part : parts) {
      if (find_part(predecessor, *old_tablet, part.part_id) == nullptr &&
          part.minimum_record_sequence <= old_tablet->durable_record_sequence) {
        return invalid("Checkpoint candidate adds a part at an already durable tablet boundary");
      }
    }
  }
  for (const RetryDescriptor& retry : candidate.retries()) {
    const TabletDescriptor* old_tablet = find_tablet(predecessor, retry.tablet_id);
    if (old_tablet == nullptr || retry.record_sequence > old_tablet->durable_record_sequence) {
      continue;
    }
    const RetryDescriptor* old_retry =
        find_retry(predecessor, retry.client_id, retry.client_batch_id);
    if (old_retry == nullptr || *old_retry != retry) {
      return invalid(
          "Checkpoint candidate adds a retry outcome at an already durable tablet boundary");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] const TabletSchemaBinding*
find_binding(const std::span<const TabletSchemaBinding> bindings,
             const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::lower_bound(bindings, tablet_id, {}, [](const TabletSchemaBinding& binding) {
        return binding.tablet_id;
      });
  return found != bindings.end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

class CoverageSink final : public wal::WalReplaySink {
public:
  CoverageSink(const ManifestCheckpointBuildInput& input, const CoverageIndex& index)
      : predecessor_(input.predecessor.get()), candidate_(input.candidate.get()),
        bindings_(input.schema_bindings), index_(index), limits_(input.command_decode_limits),
        checkpoint_(predecessor_.reclaim_checkpoint()),
        boundary_seen_(candidate_.tablets().size(), false) {}

  [[nodiscard]] common::Status preflight(const wal::WalReplayRecord& record) override {
    const ingest::ColumnarAppendDecodeResult decoded = decode(record);
    return decoded.has_value() ? common::Status::ok() : decoded.error().status();
  }

  [[nodiscard]] common::Status replay(const wal::WalReplayRecord& record) override {
    ingest::ColumnarAppendDecodeResult decoded = decode(record);
    if (!decoded.has_value()) {
      return decoded.error().status();
    }
    return cover(record, *decoded);
  }

  [[nodiscard]] const WalCheckpoint& checkpoint() const noexcept {
    return checkpoint_;
  }
  [[nodiscard]] std::uint64_t checkpointed_records() const noexcept {
    return checkpointed_records_;
  }
  [[nodiscard]] std::uint64_t validated_applied_rows() const noexcept {
    return validated_applied_rows_;
  }

  [[nodiscard]] common::Status finish() const {
    for (std::size_t index = 0U; index < candidate_.tablets().size(); ++index) {
      const TabletDescriptor& tablet = candidate_.tablets()[index];
      const TabletDescriptor* old = find_tablet(predecessor_, tablet.tablet_id);
      const std::uint64_t old_boundary = old == nullptr ? 0U : old->durable_record_sequence;
      if (tablet.durable_record_sequence > old_boundary && !boundary_seen_[index]) {
        return corruption("Manifest tablet boundary is absent from the verified WAL suffix");
      }
    }
    return common::Status::ok();
  }

private:
  [[nodiscard]] ingest::ColumnarAppendDecodeResult
  decode(const wal::WalReplayRecord& record) const {
    return ingest::decode_columnar_append_v1_record(
        wal::DecodedRecord{.header = record.header, .payload = record.payload}, limits_);
  }

  [[nodiscard]] std::span<const RowLocation> rows_for(const std::uint64_t sequence) const noexcept {
    const auto begin = std::ranges::lower_bound(index_.rows, sequence, {}, &RowLocation::sequence);
    const auto end = std::ranges::upper_bound(index_.rows, sequence, {}, &RowLocation::sequence);
    return {begin, end};
  }

  [[nodiscard]] common::Status compare_rows(const ingest::DecodedColumnarAppendView& command,
                                            const std::span<const RowLocation> rows) const {
    if (rows.size() != command.row_count()) {
      return corruption("Manifest CSEG rows do not exactly cover the applied WAL command");
    }
    for (std::size_t index = 0U; index < rows.size(); ++index) {
      if (rows[index].ordinal != index) {
        return corruption("Manifest CSEG row ordinals do not exactly cover the WAL batch");
      }
    }
    std::vector<const RowLocation*> grouped;
    grouped.reserve(rows.size());
    for (const RowLocation& row : rows) {
      grouped.push_back(&row);
    }
    std::ranges::sort(grouped, [](const RowLocation* left, const RowLocation* right) {
      return std::pair{left->part_index, left->granule_index} <
             std::pair{right->part_index, right->granule_index};
    });
    std::size_t begin = 0U;
    while (begin < grouped.size()) {
      std::size_t end = begin + 1U;
      while (end < grouped.size() && grouped[end]->part_index == grouped[begin]->part_index &&
             grouped[end]->granule_index == grouped[begin]->granule_index) {
        ++end;
      }
      const PartIndex& part = index_.parts[grouped[begin]->part_index];
      if (part.descriptor.get().tablet_id != command.tablet_id() ||
          part.descriptor.get().schema_id != command.schema_id() ||
          part.descriptor.get().schema_version != command.schema_version()) {
        return corruption("Manifest CSEG row target or schema disagrees with its WAL command");
      }
      const cseg::CsegGranuleDescriptor& granule =
          part.decoded.metadata().granules()[grouped[begin]->granule_index];
      for (std::size_t column = 0U; column < command.batch().columns().size(); ++column) {
        common::Result<cseg::DecodedCsegPage> page =
            part.decoded.decode_page(static_cast<std::size_t>(granule.first_page_index) + column);
        if (!page.has_value()) {
          return corruption("Validated checkpoint CSEG user page no longer decodes");
        }
        for (std::size_t row = begin; row < end; ++row) {
          const RowLocation& location = *grouped[row];
          const common::Result<columnar::ColumnCellView> stored =
              page->physical().cell(location.local_row);
          const common::Result<columnar::ColumnCellView> original =
              command.batch().columns()[column].cell(location.ordinal);
          if (!stored.has_value() || !original.has_value() ||
              !cell_equal(*stored, *original, command.batch().columns()[column].type().kind())) {
            return corruption("Manifest CSEG user cell disagrees with its WAL command");
          }
        }
      }
      begin = end;
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status cover(const wal::WalReplayRecord& record,
                                     const ingest::DecodedColumnarAppendView& command) {
    const TabletDescriptor* tablet = find_tablet(candidate_, command.tablet_id());
    const bool claimed =
        tablet != nullptr && tablet->durable_record_sequence >= record.header.record_sequence;
    const std::span<const RowLocation> rows = rows_for(record.header.record_sequence);
    if (!claimed) {
      if (!rows.empty()) {
        return corruption("Unclaimed WAL command unexpectedly has manifest CSEG rows");
      }
      prefix_open_ = false;
      return common::Status::ok();
    }
    const TabletSchemaBinding* binding = find_binding(bindings_, command.tablet_id());
    if (binding == nullptr) {
      return corruption("Claimed WAL command is missing its manifest schema binding");
    }
    const std::shared_ptr<const schema::TableSchema> command_schema =
        binding->lineage.get().find(command.schema_id());
    if (!command_schema || command_schema->version() != command.schema_version()) {
      return corruption("Claimed WAL command schema is absent from its retained lineage");
    }
    common::Status command_schema_status =
        ingest::validate_columnar_append_schema(command, *command_schema);
    if (!command_schema_status.is_ok()) {
      return corruption("Claimed WAL command does not bind to its retained schema");
    }
    const RetryDescriptor* retry =
        find_retry(candidate_, command.client_id(), command.client_batch_id());
    if (retry == nullptr || retry->table_id != command.table_id() ||
        retry->tablet_id != command.tablet_id() ||
        retry->request_digest != command.request_digest() || retry->wal_id != candidate_.wal_id() ||
        retry->applied_row_count != command.row_count() ||
        retry->record_sequence > record.header.record_sequence) {
      return corruption("Claimed WAL command disagrees with its protected retry outcome");
    }
    if (retry->record_sequence == record.header.record_sequence) {
      common::Status row_status = compare_rows(command, rows);
      if (!row_status.is_ok()) {
        return row_status;
      }
      const std::optional<std::uint64_t> next_rows = common::checked_add(
          validated_applied_rows_, static_cast<std::uint64_t>(command.row_count()));
      if (!next_rows.has_value()) {
        return exhausted("Checkpoint covered row metric overflowed");
      }
      validated_applied_rows_ = *next_rows;
    } else if (!rows.empty()) {
      return corruption("Matching duplicate WAL command unexpectedly has CSEG rows");
    }
    const auto tablet_index = static_cast<std::size_t>(tablet - candidate_.tablets().data());
    if (record.header.record_sequence == tablet->durable_record_sequence) {
      boundary_seen_[tablet_index] = true;
    }
    if (prefix_open_) {
      checkpoint_ = {.record_sequence = record.header.record_sequence,
                     .segment_number = record.record_end.segment_number,
                     .byte_offset = record.record_end.byte_offset};
      if (checkpointed_records_ == std::numeric_limits<std::uint64_t>::max()) {
        return exhausted("Checkpoint covered record metric overflowed");
      }
      ++checkpointed_records_;
    }
    return common::Status::ok();
  }

  const DecodedManifestView& predecessor_;
  const DecodedManifestView& candidate_;
  std::span<const TabletSchemaBinding> bindings_;
  const CoverageIndex& index_;
  ingest::ColumnarAppendDecodeLimits limits_;
  WalCheckpoint checkpoint_;
  std::vector<bool> boundary_seen_;
  bool prefix_open_{true};
  std::uint64_t checkpointed_records_{};
  std::uint64_t validated_applied_rows_{};
};

struct CheckpointBuildArtifacts {
  EncodedManifest encoded_manifest;
  WalCheckpoint previous_checkpoint;
  WalCheckpoint reclaim_checkpoint;
  std::uint64_t newly_checkpointed_records{};
  std::uint64_t validated_applied_rows{};
  wal::WalRecoveryReport wal_report;
};

[[nodiscard]] common::Result<CheckpointBuildArtifacts>
build(const ManifestCheckpointBuildInput& input) {
  const DecodedManifestView& predecessor = input.predecessor.get();
  const DecodedManifestView& candidate = input.candidate.get();
  if (candidate.reclaim_checkpoint() != predecessor.reclaim_checkpoint()) {
    return common::make_unexpected(
        invalid("Checkpoint candidate must preserve the predecessor reclaim coordinate"));
  }
  common::Status transition =
      validate_manifest_v1_transition(predecessor, candidate, input.schema_bindings);
  if (!transition.is_ok()) {
    return common::make_unexpected(transition);
  }
  const common::Status incremental = validate_incremental_coverage(input);
  if (!incremental.is_ok()) {
    return common::make_unexpected(incremental);
  }
  common::Status parts = validate_manifest_v1_referenced_parts(
      candidate, input.schema_bindings, input.referenced_parts, input.part_validation_limits);
  if (!parts.is_ok()) {
    return common::make_unexpected(parts);
  }
  common::Result<CoverageIndex> index =
      build_index(candidate, input.referenced_parts, input.part_validation_limits.decode);
  if (!index.has_value()) {
    return common::make_unexpected(index.error());
  }
  CoverageSink sink{input, *index};
  const wal::WalReplayCheckpoint checkpoint{
      .wal_id = predecessor.wal_id(),
      .record_sequence = predecessor.reclaim_checkpoint().record_sequence,
      .segment_number = predecessor.reclaim_checkpoint().segment_number,
      .byte_offset = predecessor.reclaim_checkpoint().byte_offset,
  };
  common::Result<wal::WalRecoveryReport> report =
      wal::inspect_wal_suffix(input.wal_directory, checkpoint, sink);
  if (!report.has_value()) {
    return common::make_unexpected(report.error());
  }
  if (report->classification != wal::WalScanClassification::kClean) {
    return common::make_unexpected(status(
        common::StatusCode::kOutOfRange,
        "WAL checkpoint proof requires a clean suffix; incomplete final tail needs recovery"));
  }
  const common::Status finished = sink.finish();
  if (!finished.is_ok()) {
    return common::make_unexpected(finished);
  }
  common::Result<EncodedManifest> encoded = encode_manifest_v1({
      .generation = candidate.generation(),
      .database_id = candidate.database_id(),
      .wal_id = candidate.wal_id(),
      .reclaim_checkpoint = sink.checkpoint(),
      .tablets = candidate.tablets(),
      .parts = candidate.parts(),
      .retries = candidate.retries(),
  });
  if (!encoded.has_value()) {
    return common::make_unexpected(encoded.error());
  }
  ManifestDecodeResult decoded = decode_manifest_v1_exact(encoded->bytes());
  if (!decoded.has_value()) {
    return common::make_unexpected(
        status(common::StatusCode::kInternal, "Checkpointed Manifest v1 did not decode"));
  }
  transition = validate_manifest_v1_transition(predecessor, *decoded, input.schema_bindings);
  if (!transition.is_ok()) {
    return common::make_unexpected(transition);
  }
  return CheckpointBuildArtifacts{.encoded_manifest = std::move(*encoded),
                                  .previous_checkpoint = predecessor.reclaim_checkpoint(),
                                  .reclaim_checkpoint = sink.checkpoint(),
                                  .newly_checkpointed_records = sink.checkpointed_records(),
                                  .validated_applied_rows = sink.validated_applied_rows(),
                                  .wal_report = *report};
}

} // namespace

struct CheckpointedManifestGeneration::State {
  CheckpointBuildArtifacts artifacts;
};

CheckpointedManifestGeneration::CheckpointedManifestGeneration(State state) noexcept
    : encoded_manifest(std::move(state.artifacts.encoded_manifest)),
      previous_checkpoint(state.artifacts.previous_checkpoint),
      reclaim_checkpoint(state.artifacts.reclaim_checkpoint),
      newly_checkpointed_records(state.artifacts.newly_checkpointed_records),
      validated_applied_rows(state.artifacts.validated_applied_rows),
      wal_report(state.artifacts.wal_report) {}

common::Result<CheckpointedManifestGeneration>
build_manifest_v1_checkpointed_generation(const ManifestCheckpointBuildInput& input) {
  try {
    common::Result<CheckpointBuildArtifacts> artifacts = build(input);
    if (!artifacts.has_value()) {
      return common::make_unexpected(artifacts.error());
    }
    return CheckpointedManifestGeneration{
        CheckpointedManifestGeneration::State{.artifacts = std::move(*artifacts)}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Manifest checkpoint proof allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("Manifest checkpoint proof allocation exceeded limits"));
  }
}

} // namespace chronos::manifest
