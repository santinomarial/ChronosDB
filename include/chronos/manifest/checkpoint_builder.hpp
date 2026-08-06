#ifndef CHRONOS_MANIFEST_CHECKPOINT_BUILDER_HPP_
#define CHRONOS_MANIFEST_CHECKPOINT_BUILDER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/part_validation.hpp"
#include "chronos/manifest/validation.hpp"
#include "chronos/wal/wal_recovery_report.hpp"

#include <cstdint>
#include <functional>
#include <span>
#include <string_view>

namespace chronos::manifest {

// Every string/view/reference is borrowed only for build_manifest_v1_checkpointed_generation().
// Candidate and predecessor encoded storage, schema lineages, part images, and the WAL directory
// name must remain alive and immutable for that call. The caller must serialize the call against
// WAL append, rotation, repair, and reclamation plus out-of-band directory mutation. No input view
// is retained in the result.
struct ManifestCheckpointBuildInput {
  std::string_view wal_directory;
  std::reference_wrapper<const DecodedManifestView> predecessor;
  std::reference_wrapper<const DecodedManifestView> candidate;
  std::span<const TabletSchemaBinding> schema_bindings;
  std::span<const ReferencedPartImage> referenced_parts;
  ingest::ColumnarAppendDecodeLimits command_decode_limits;
  ReferencedPartValidationLimits part_validation_limits;
};

struct CheckpointedManifestGeneration {
  EncodedManifest encoded_manifest;
  WalCheckpoint previous_checkpoint;
  WalCheckpoint reclaim_checkpoint;
  // Counts records added to the global consecutive coordinate. Applied rows validated after an
  // earlier cross-tablet gap remain excluded here but are included in validated_applied_rows.
  std::uint64_t newly_checkpointed_records{};
  std::uint64_t validated_applied_rows{};
  wal::WalRecoveryReport wal_report;

  CheckpointedManifestGeneration(const CheckpointedManifestGeneration&) = delete;
  CheckpointedManifestGeneration& operator=(const CheckpointedManifestGeneration&) = delete;
  CheckpointedManifestGeneration(CheckpointedManifestGeneration&&) noexcept = default;
  CheckpointedManifestGeneration& operator=(CheckpointedManifestGeneration&&) noexcept = default;

private:
  struct State;
  explicit CheckpointedManifestGeneration(State state) noexcept;

  friend common::Result<CheckpointedManifestGeneration>
  build_manifest_v1_checkpointed_generation(const ManifestCheckpointBuildInput& input);
};

// Revalidates a next-generation candidate whose reclaim checkpoint still equals its predecessor,
// then read-only inspects the exact WAL suffix. Every record claimed by a tablet boundary must be a
// supported COLUMNAR_APPEND with matching schema, retry outcome, and exact CSEG user/system rows.
// The returned generation advances only through the longest globally consecutive covered prefix.
// No WAL, part, manifest, or in-memory publication state is mutated.
[[nodiscard]] common::Result<CheckpointedManifestGeneration>
build_manifest_v1_checkpointed_generation(const ManifestCheckpointBuildInput& input);

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_CHECKPOINT_BUILDER_HPP_
