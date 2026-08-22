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
inline constexpr std::string_view kAfterMetadataAuthoritativeReclamationList =
    "after_metadata_authoritative_reclamation_list";
inline constexpr std::string_view kAfterMetadataAuthoritativeReclamationUnlink =
    "after_metadata_authoritative_reclamation_unlink";
inline constexpr std::string_view kAfterMetadataAuthoritativeReclamationDirectorySync =
    "after_metadata_authoritative_reclamation_directory_sync";
inline constexpr std::string_view kAfterMetadataAuthoritativeReclamationSuccess =
    "after_metadata_authoritative_reclamation_success";
inline constexpr std::string_view kAfterMetadataOrphanReclamationList =
    "after_metadata_orphan_reclamation_list";
inline constexpr std::string_view kAfterMetadataOrphanReclamationUnlink =
    "after_metadata_orphan_reclamation_unlink";
inline constexpr std::string_view kAfterMetadataOrphanReclamationDirectorySync =
    "after_metadata_orphan_reclamation_directory_sync";
inline constexpr std::string_view kAfterMetadataOrphanReclamationSuccess =
    "after_metadata_orphan_reclamation_success";
inline constexpr std::string_view kAfterMetadataCompactionTemporaryCreate =
    "after_metadata_compaction_temporary_create";
inline constexpr std::string_view kAfterMetadataCompactionWrite = "after_metadata_compaction_write";
inline constexpr std::string_view kAfterMetadataCompactionReadback =
    "after_metadata_compaction_readback";
inline constexpr std::string_view kAfterMetadataCompactionFileSync =
    "after_metadata_compaction_file_sync";
inline constexpr std::string_view kAfterMetadataCompactionTemporaryClose =
    "after_metadata_compaction_temporary_close";
inline constexpr std::string_view kAfterMetadataCompactionRename =
    "after_metadata_compaction_rename";
inline constexpr std::string_view kAfterMetadataCompactionDirectorySync =
    "after_metadata_compaction_directory_sync";
inline constexpr std::string_view kAfterMetadataCompactionRaftWrite =
    "after_metadata_compaction_raft_write";
inline constexpr std::string_view kAfterMetadataCompactionRaftSync =
    "after_metadata_compaction_raft_sync";
inline constexpr std::string_view kAfterMetadataCompactionSuccess =
    "after_metadata_compaction_success";

} // namespace chronos::raft::test

#endif // CHRONOS_TESTS_RAFT_METADATA_SNAPSHOT_INSTALL_CRASH_PROTOCOL_HPP_
