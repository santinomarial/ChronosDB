#ifndef CHRONOS_TESTS_INGEST_TABLET_SNAPSHOT_INSTALL_CRASH_PROTOCOL_HPP_
#define CHRONOS_TESTS_INGEST_TABLET_SNAPSHOT_INSTALL_CRASH_PROTOCOL_HPP_

#include <string_view>

namespace chronos::ingest::test {

inline constexpr std::string_view kAfterApplicationTemporaryCreate =
    "after_application_temporary_create";
inline constexpr std::string_view kAfterApplicationWrite = "after_application_write";
inline constexpr std::string_view kAfterApplicationReadback = "after_application_readback";
inline constexpr std::string_view kAfterApplicationFileSync = "after_application_file_sync";
inline constexpr std::string_view kAfterApplicationTemporaryClose =
    "after_application_temporary_close";
inline constexpr std::string_view kAfterApplicationRename = "after_application_rename";
inline constexpr std::string_view kAfterApplicationDirectorySync =
    "after_application_directory_sync";
inline constexpr std::string_view kAfterRaftStateWrite = "after_raft_state_write";
inline constexpr std::string_view kAfterRaftStateSync = "after_raft_state_sync";
inline constexpr std::string_view kAfterSuccessRelease = "after_success_release";
inline constexpr std::string_view kAfterApplicationAuthoritativeReclamationList =
    "after_application_authoritative_reclamation_list";
inline constexpr std::string_view kAfterApplicationAuthoritativeReclamationUnlink =
    "after_application_authoritative_reclamation_unlink";
inline constexpr std::string_view kAfterApplicationAuthoritativeReclamationDirectorySync =
    "after_application_authoritative_reclamation_directory_sync";
inline constexpr std::string_view kAfterApplicationAuthoritativeReclamationSuccess =
    "after_application_authoritative_reclamation_success";
inline constexpr std::string_view kAfterApplicationOrphanReclamationList =
    "after_application_orphan_reclamation_list";
inline constexpr std::string_view kAfterApplicationOrphanReclamationUnlink =
    "after_application_orphan_reclamation_unlink";
inline constexpr std::string_view kAfterApplicationOrphanReclamationDirectorySync =
    "after_application_orphan_reclamation_directory_sync";
inline constexpr std::string_view kAfterApplicationOrphanReclamationSuccess =
    "after_application_orphan_reclamation_success";
inline constexpr std::string_view kAfterApplicationCompactionTemporaryCreate =
    "after_application_compaction_temporary_create";
inline constexpr std::string_view kAfterApplicationCompactionWrite =
    "after_application_compaction_write";
inline constexpr std::string_view kAfterApplicationCompactionReadback =
    "after_application_compaction_readback";
inline constexpr std::string_view kAfterApplicationCompactionFileSync =
    "after_application_compaction_file_sync";
inline constexpr std::string_view kAfterApplicationCompactionTemporaryClose =
    "after_application_compaction_temporary_close";
inline constexpr std::string_view kAfterApplicationCompactionRename =
    "after_application_compaction_rename";
inline constexpr std::string_view kAfterApplicationCompactionDirectorySync =
    "after_application_compaction_directory_sync";
inline constexpr std::string_view kAfterApplicationCompactionRaftWrite =
    "after_application_compaction_raft_write";
inline constexpr std::string_view kAfterApplicationCompactionRaftSync =
    "after_application_compaction_raft_sync";
inline constexpr std::string_view kAfterApplicationCompactionSuccess =
    "after_application_compaction_success";

} // namespace chronos::ingest::test

#endif // CHRONOS_TESTS_INGEST_TABLET_SNAPSHOT_INSTALL_CRASH_PROTOCOL_HPP_
