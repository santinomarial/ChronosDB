#ifndef CHRONOS_RUNTIME_DATABASE_BOOTSTRAP_HPP_
#define CHRONOS_RUNTIME_DATABASE_BOOTSTRAP_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace chronos::runtime {

inline constexpr std::size_t kDatabaseBootstrapV1Size = 128U;
inline constexpr const char* kDatabaseBootstrapFileName = "BOOTSTRAP";
inline constexpr const char* kDatabaseBootstrapTemporaryFileName = "BOOTSTRAP.tmp";
inline constexpr const char* kDatabaseRootLockFileName = "LOCK";
inline constexpr const char* kDatabaseWalDirectoryName = "wal";
inline constexpr const char* kDatabaseRaftDirectoryName = "raft";

// Durable operational limits needed to reproduce the same in-memory admission shape after a
// restart. UUIDs are uninterpreted durable identities; callers convert them to subsystem-specific
// strong identifier types only after this image has passed checksum and relationship validation.
struct DatabaseBootstrapDescriptor {
  common::Uuid database_id;
  common::Uuid metadata_group_id;
  std::uint64_t local_node_id{};
  std::uint32_t mutable_head_rows{};
  std::uint32_t maximum_sealed_generations{};
  std::uint64_t variable_column_bytes{};
  std::uint64_t maximum_retry_entries{};
  std::uint64_t wal_segment_target_bytes{};
  std::uint64_t raft_segment_target_bytes{};

  friend bool operator==(const DatabaseBootstrapDescriptor&,
                         const DatabaseBootstrapDescriptor&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_database_bootstrap_v1(const DatabaseBootstrapDescriptor& descriptor);

[[nodiscard]] common::Result<DatabaseBootstrapDescriptor>
decode_database_bootstrap_v1(common::ByteView bytes);

struct DatabaseBootstrapConfig {
  // Existing dedicated database root. Creation of this directory and durability of its name are
  // deployment responsibilities; this owner installs and synchronizes children within it.
  std::string database_root;
  DatabaseBootstrapDescriptor new_database;
  std::uint16_t file_permissions{0600U};
  std::uint16_t directory_permissions{0700U};
};

// Owns the database-root advisory lock and a validated durable bootstrap descriptor. open_or_create
// is restartable: BOOTSTRAP.tmp is a synchronized intent carrying the authoritative identities, so
// an interrupted first installation resumes those exact bytes instead of manufacturing new state.
// Once BOOTSTRAP exists, new_database is ignored and the durable descriptor wins.
class DatabaseBootstrap {
public:
  DatabaseBootstrap() = delete;
  ~DatabaseBootstrap();

  DatabaseBootstrap(const DatabaseBootstrap&) = delete;
  DatabaseBootstrap& operator=(const DatabaseBootstrap&) = delete;
  DatabaseBootstrap(DatabaseBootstrap&&) noexcept;
  DatabaseBootstrap& operator=(DatabaseBootstrap&&) noexcept;

  [[nodiscard]] static common::Result<DatabaseBootstrap>
  open_or_create(const DatabaseBootstrapConfig& config);

  [[nodiscard]] const DatabaseBootstrapDescriptor& descriptor() const noexcept;
  [[nodiscard]] const std::string& database_root() const noexcept;
  [[nodiscard]] std::string wal_directory_path() const;
  [[nodiscard]] std::string raft_directory_path() const;
  [[nodiscard]] common::Status close();

private:
  class Impl;
  explicit DatabaseBootstrap(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::runtime

#endif // CHRONOS_RUNTIME_DATABASE_BOOTSTRAP_HPP_
