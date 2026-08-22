#ifndef CHRONOS_TESTS_RAFT_METADATA_SNAPSHOT_INSTALL_CRASH_PROTOCOL_HPP_
#define CHRONOS_TESTS_RAFT_METADATA_SNAPSHOT_INSTALL_CRASH_PROTOCOL_HPP_

#include <string_view>

namespace chronos::raft::test {

inline constexpr std::string_view kAfterMetadataTemporaryCreate = "after_metadata_temporary_create";
inline constexpr std::string_view kAfterMetadataWrite = "after_metadata_write";
inline constexpr std::string_view kAfterMetadataReadback = "after_metadata_readback";
inline constexpr std::string_view kAfterMetadataFileSync = "after_metadata_file_sync";
inline constexpr std::string_view kAfterMetadataTemporaryClose = "after_metadata_temporary_close";
inline constexpr std::string_view kAfterMetadataRename = "after_metadata_rename";
inline constexpr std::string_view kAfterMetadataDirectorySync = "after_metadata_directory_sync";
inline constexpr std::string_view kAfterMetadataSuccessRelease = "after_metadata_success_release";

} // namespace chronos::raft::test

#endif // CHRONOS_TESTS_RAFT_METADATA_SNAPSHOT_INSTALL_CRASH_PROTOCOL_HPP_
