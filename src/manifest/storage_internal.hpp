#ifndef CHRONOS_MANIFEST_STORAGE_INTERNAL_HPP_
#define CHRONOS_MANIFEST_STORAGE_INTERNAL_HPP_

#include "chronos/manifest/storage.hpp"
#include "io/posix_syscalls.hpp"

namespace chronos::manifest::detail {

class ManifestStorageTestAccess {
public:
  [[nodiscard]] static common::Result<ManifestStorage>
  open_existing(const ManifestStorageConfig& config, io::detail::PosixSyscalls& syscalls) {
    return ManifestStorage::open_existing_with(config, syscalls);
  }
};

} // namespace chronos::manifest::detail

#endif // CHRONOS_MANIFEST_STORAGE_INTERNAL_HPP_
