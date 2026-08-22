#ifndef CHRONOS_TESTS_RAFT_METADATA_SNAPSHOT_INSTALL_CRASH_FIXTURE_HPP_
#define CHRONOS_TESTS_RAFT_METADATA_SNAPSHOT_INSTALL_CRASH_FIXTURE_HPP_

#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_snapshot_storage.hpp"

#include <cstddef>
#include <filesystem>
#include <utility>

namespace chronos::raft::test {

[[nodiscard]] inline GroupId metadata_crash_group_id() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{8U});
  return GroupId{bytes};
}

[[nodiscard]] inline MetadataApplicationSnapshot
metadata_crash_snapshot(const LogIndex included = 7U) {
  SnapshotMetadata metadata{.last_included_index = included,
                            .last_included_term = included == 7U ? 2U : 3U,
                            .manifest_generation = included,
                            .part_set_checksum = {},
                            .configuration_index = 4U,
                            .voters = {1U, 2U}};
  metadata.part_set_checksum.front() = std::byte{0x5AU};
  return {.group_id = metadata_crash_group_id(),
          .raft_snapshot = std::move(metadata),
          .entries = {{.index = 3U,
                       .term = 1U,
                       .type = kRaftMetadataCommandEntryType,
                       .payload = {std::byte{1U}, std::byte{2U}}},
                      {.index = 6U,
                       .term = 2U,
                       .type = kRaftMetadataCommandEntryType,
                       .payload = {std::byte{3U}}}}};
}

[[nodiscard]] inline MetadataSnapshotStorageConfig
metadata_crash_storage_config(const std::filesystem::path& directory) {
  return {.directory_path = directory.string(), .group_id = metadata_crash_group_id()};
}

} // namespace chronos::raft::test

#endif // CHRONOS_TESTS_RAFT_METADATA_SNAPSHOT_INSTALL_CRASH_FIXTURE_HPP_
