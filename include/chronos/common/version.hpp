#pragma once

#include <string>
#include <string_view>

namespace chronos::common {

struct VersionInfo {
  std::string_view semantic_version;
  std::string_view git_commit;
  bool git_metadata_available;
  bool git_dirty;
  std::string_view build_type;
  std::string_view compiler;
  std::string_view target_architecture;
  std::string_view operating_system;
};

[[nodiscard]] VersionInfo version_info() noexcept;
[[nodiscard]] std::string_view semantic_version() noexcept;
[[nodiscard]] std::string version_text(const VersionInfo& info);
[[nodiscard]] std::string version_text();
[[nodiscard]] std::string version_json(const VersionInfo& info);
[[nodiscard]] std::string version_json();

} // namespace chronos::common
