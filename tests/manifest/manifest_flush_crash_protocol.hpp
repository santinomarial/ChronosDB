#ifndef CHRONOS_TESTS_MANIFEST_MANIFEST_FLUSH_CRASH_PROTOCOL_HPP_
#define CHRONOS_TESTS_MANIFEST_MANIFEST_FLUSH_CRASH_PROTOCOL_HPP_

#include <string_view>

namespace chronos::manifest::test {

inline constexpr std::string_view kAfterPartWrite = "after_part_write";
inline constexpr std::string_view kAfterPartReadback = "after_part_readback";
inline constexpr std::string_view kAfterPartFileSync = "after_part_file_sync";
inline constexpr std::string_view kAfterPartRename = "after_part_rename";
inline constexpr std::string_view kAfterPartsDirectorySync = "after_parts_directory_sync";
inline constexpr std::string_view kAfterManifestWrite = "after_manifest_write";
inline constexpr std::string_view kAfterManifestReadback = "after_manifest_readback";
inline constexpr std::string_view kAfterManifestFileSync = "after_manifest_file_sync";
inline constexpr std::string_view kAfterManifestRename = "after_manifest_rename";
inline constexpr std::string_view kAfterManifestDirectorySync = "after_manifest_directory_sync";
inline constexpr std::string_view kAfterPublication = "after_publication";

} // namespace chronos::manifest::test

#endif // CHRONOS_TESTS_MANIFEST_MANIFEST_FLUSH_CRASH_PROTOCOL_HPP_
