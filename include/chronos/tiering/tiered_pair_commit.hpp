#ifndef CHRONOS_TIERING_TIERED_PAIR_COMMIT_HPP_
#define CHRONOS_TIERING_TIERED_PAIR_COMMIT_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/tiering/cold_manifest_storage.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace chronos::tiering {

class ObjectStore;

inline constexpr std::size_t kTieredPairCommitV1Size = 256U;
inline constexpr std::string_view kTieredPairCommitLockFileName = "LOCK";

struct TieredPairCommitRecord {
  std::uint64_t generation{};
  std::uint64_t previous_generation{};
  manifest::DatabaseId database_id;
  common::Uuid object_store_id;
  std::uint64_t manifest_generation{};
  std::uint64_t cold_generation{};
  std::uint64_t manifest_length{};
  std::uint64_t cold_length{};
  ingest::Sha256Digest manifest_sha256;
  ingest::Sha256Digest cold_sha256;

  friend bool operator==(const TieredPairCommitRecord&, const TieredPairCommitRecord&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_tiered_pair_commit_v1(const TieredPairCommitRecord& record);
[[nodiscard]] common::Result<TieredPairCommitRecord>
decode_tiered_pair_commit_v1_exact(common::ByteView bytes);
[[nodiscard]] common::Result<std::string> tiered_pair_commit_file_name(std::uint64_t generation);

struct TieredPairCommitStorageConfig {
  std::string directory_path;
  manifest::DatabaseId expected_database_id;
  common::Uuid expected_object_store_id;
  std::uint16_t file_permissions{0600U};
};

struct InstalledTieredPairCommit {
  std::string file_name;
  TieredPairCommitRecord record;
  bool already_present{false};
};

struct TieredPairRecoveryRequest {
  manifest::TemporalManifestLoadRequest manifest_request;
  // Required only when a committed Manifest part has no local final. Recovery never uses this
  // store without an exact committed cold route.
  const ObjectStore* remote_store{};
};

struct RecoveredTieredPair {
  TieredPairCommitRecord record;
  manifest::TemporalDatabaseStorageSnapshot manifest_snapshot;
  std::shared_ptr<const LoadedColdLocationManifest> cold_manifest;
};

// The pair directory is the aggregate crash authority after tiering is enabled. Base/cold finals
// installed after the latest pair marker are uncommitted orphans. A synchronized next pair marker
// atomically makes both already-durable exact hashes selectable after restart.
class TieredPairCommitStorage {
public:
  TieredPairCommitStorage() = delete;
  ~TieredPairCommitStorage();
  TieredPairCommitStorage(const TieredPairCommitStorage&) = delete;
  TieredPairCommitStorage& operator=(const TieredPairCommitStorage&) = delete;
  TieredPairCommitStorage(TieredPairCommitStorage&&) noexcept;
  TieredPairCommitStorage& operator=(TieredPairCommitStorage&&) noexcept;

  [[nodiscard]] static common::Result<TieredPairCommitStorage>
  create(TieredPairCommitStorageConfig config);
  [[nodiscard]] static common::Result<TieredPairCommitStorage>
  open_existing(TieredPairCommitStorageConfig config);

  [[nodiscard]] common::Result<InstalledTieredPairCommit>
  commit(const manifest::TemporalDatabaseStorageSnapshot& manifest_snapshot,
         const std::shared_ptr<const LoadedColdLocationManifest>& cold_manifest);

  [[nodiscard]] common::Result<std::optional<RecoveredTieredPair>>
  recover(manifest::ManifestStorage& manifest_storage,
          const ColdLocationManifestStorage& cold_storage,
          const TieredPairRecoveryRequest& request) const;

  // Returns the highest consecutive, exact-decoded durable pair marker without loading its
  // components. Reclamation uses this to revalidate aggregate commit authority immediately before
  // irreversible mutation.
  [[nodiscard]] common::Result<std::optional<TieredPairCommitRecord>> load_selected_record() const;

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  class Impl;
  explicit TieredPairCommitStorage(std::unique_ptr<Impl> impl) noexcept;
  [[nodiscard]] static common::Result<TieredPairCommitStorage>
  open(TieredPairCommitStorageConfig config, bool create_lock);
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::tiering

#endif // CHRONOS_TIERING_TIERED_PAIR_COMMIT_HPP_
