#ifndef CHRONOS_TESTS_RAFT_PERSISTENT_LOG_CRASH_PROTOCOL_HPP_
#define CHRONOS_TESTS_RAFT_PERSISTENT_LOG_CRASH_PROTOCOL_HPP_

#include <string_view>

namespace chronos::raft::test {

inline constexpr std::string_view kAfterInitialLockCreate = "after_initial_lock_create";
inline constexpr std::string_view kAfterInitialLockDirectorySync =
    "after_initial_lock_directory_sync";
inline constexpr std::string_view kAfterInitialHeaderWrite = "after_initial_header_write";
inline constexpr std::string_view kAfterInitialFileSync = "after_initial_file_sync";
inline constexpr std::string_view kAfterInitialRename = "after_initial_rename";
inline constexpr std::string_view kAfterInitialDirectorySync = "after_initial_directory_sync";
inline constexpr std::string_view kAfterPredecessorDataSync = "after_predecessor_data_sync";
inline constexpr std::string_view kAfterPredecessorClose = "after_predecessor_close";
inline constexpr std::string_view kAfterSuccessorHeaderWrite = "after_successor_header_write";
inline constexpr std::string_view kAfterSuccessorFileSync = "after_successor_file_sync";
inline constexpr std::string_view kAfterSuccessorRename = "after_successor_rename";
inline constexpr std::string_view kAfterSuccessorDirectorySync = "after_successor_directory_sync";
inline constexpr std::string_view kAfterRotatedRecordWrite = "after_rotated_record_write";
inline constexpr std::string_view kAfterRotatedRecordDataSync = "after_rotated_record_data_sync";
inline constexpr std::string_view kAfterCheckpointRecordWrite = "after_checkpoint_record_write";
inline constexpr std::string_view kAfterCheckpointDataSync = "after_checkpoint_data_sync";
inline constexpr std::string_view kAfterAnchorWrite = "after_anchor_write";
inline constexpr std::string_view kAfterAnchorFileSync = "after_anchor_file_sync";
inline constexpr std::string_view kAfterAnchorRename = "after_anchor_rename";
inline constexpr std::string_view kAfterAnchorDirectorySync = "after_anchor_directory_sync";
inline constexpr std::string_view kAfterAnchorClose = "after_anchor_close";
inline constexpr std::string_view kAfterObsoleteSegmentUnlink = "after_obsolete_segment_unlink";
inline constexpr std::string_view kAfterObsoleteSegmentDirectorySync =
    "after_obsolete_segment_directory_sync";
inline constexpr std::string_view kAfterObsoleteAnchorUnlink = "after_obsolete_anchor_unlink";
inline constexpr std::string_view kAfterObsoleteAnchorDirectorySync =
    "after_obsolete_anchor_directory_sync";

} // namespace chronos::raft::test

#endif // CHRONOS_TESTS_RAFT_PERSISTENT_LOG_CRASH_PROTOCOL_HPP_
