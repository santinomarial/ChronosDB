#ifndef CHRONOS_RAFT_METADATA_SNAPSHOT_STORAGE_INTERNAL_HPP_
#define CHRONOS_RAFT_METADATA_SNAPSHOT_STORAGE_INTERNAL_HPP_

#include "chronos/raft/metadata_snapshot_storage.hpp"
#include "io/posix_syscalls.hpp"

#include <utility>

namespace chronos::raft::detail {

class MetadataSnapshotStorageTestAccess {
public:
  [[nodiscard]] static common::Result<MetadataSnapshotStorage>
  create(MetadataSnapshotStorageConfig config, io::detail::PosixSyscalls& syscalls) {
    return MetadataSnapshotStorage::open_with(std::move(config), true, syscalls);
  }

  [[nodiscard]] static common::Result<MetadataSnapshotStorage>
  open_existing(MetadataSnapshotStorageConfig config, io::detail::PosixSyscalls& syscalls) {
    return MetadataSnapshotStorage::open_with(std::move(config), false, syscalls);
  }
};

} // namespace chronos::raft::detail

#endif // CHRONOS_RAFT_METADATA_SNAPSHOT_STORAGE_INTERNAL_HPP_
