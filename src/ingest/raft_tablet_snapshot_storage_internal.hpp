#ifndef CHRONOS_INGEST_RAFT_TABLET_SNAPSHOT_STORAGE_INTERNAL_HPP_
#define CHRONOS_INGEST_RAFT_TABLET_SNAPSHOT_STORAGE_INTERNAL_HPP_

#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "io/posix_syscalls.hpp"

#include <utility>

namespace chronos::ingest::detail {

class RaftTabletSnapshotStorageTestAccess {
public:
  [[nodiscard]] static common::Result<RaftTabletSnapshotStorage>
  create(RaftTabletSnapshotStorageConfig config, io::detail::PosixSyscalls& syscalls) {
    return RaftTabletSnapshotStorage::open_with(std::move(config), true, syscalls);
  }

  [[nodiscard]] static common::Result<RaftTabletSnapshotStorage>
  open_existing(RaftTabletSnapshotStorageConfig config, io::detail::PosixSyscalls& syscalls) {
    return RaftTabletSnapshotStorage::open_with(std::move(config), false, syscalls);
  }
};

} // namespace chronos::ingest::detail

#endif // CHRONOS_INGEST_RAFT_TABLET_SNAPSHOT_STORAGE_INTERNAL_HPP_
