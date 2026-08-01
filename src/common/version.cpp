#include "chronos/common/version.hpp"

#include "chronos/common/version_config.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

namespace chronos::common {
namespace {

[[nodiscard]] unsigned char byte_at(const std::string_view value, const std::size_t offset) {
  return static_cast<unsigned char>(value[offset]);
}

[[nodiscard]] bool is_continuation_byte(const std::string_view value, const std::size_t offset) {
  return offset < value.size() && (byte_at(value, offset) & 0xc0U) == 0x80U;
}

[[nodiscard]] std::size_t valid_utf8_sequence_length(const std::string_view value,
                                                     const std::size_t offset) {
  const auto lead = byte_at(value, offset);
  const std::size_t remaining = value.size() - offset;
  if (lead >= 0xc2U && lead <= 0xdfU) {
    return remaining >= 2U && is_continuation_byte(value, offset + 1U) ? 2U : 0U;
  }
  if (lead == 0xe0U) {
    if (remaining < 3U) {
      return 0U;
    }
    const auto second = byte_at(value, offset + 1U);
    return second >= 0xa0U && second <= 0xbfU && is_continuation_byte(value, offset + 2U) ? 3U : 0U;
  }
  if ((lead >= 0xe1U && lead <= 0xecU) || (lead >= 0xeeU && lead <= 0xefU)) {
    return remaining >= 3U && is_continuation_byte(value, offset + 1U) &&
                   is_continuation_byte(value, offset + 2U)
               ? 3U
               : 0U;
  }
  if (lead == 0xedU) {
    if (remaining < 3U) {
      return 0U;
    }
    const auto second = byte_at(value, offset + 1U);
    return second >= 0x80U && second <= 0x9fU && is_continuation_byte(value, offset + 2U) ? 3U : 0U;
  }
  if (lead >= 0xf0U && lead <= 0xf4U) {
    if (remaining < 4U) {
      return 0U;
    }
    const auto second = byte_at(value, offset + 1U);
    const bool second_is_valid =
        (lead == 0xf0U && second >= 0x90U && second <= 0xbfU) ||
        (lead >= 0xf1U && lead <= 0xf3U && second >= 0x80U && second <= 0xbfU) ||
        (lead == 0xf4U && second >= 0x80U && second <= 0x8fU);
    return second_is_valid && is_continuation_byte(value, offset + 2U) &&
                   is_continuation_byte(value, offset + 3U)
               ? 4U
               : 0U;
  }
  return 0U;
}

void append_json_string(std::string& output, const std::string_view value) {
  constexpr std::array<char, 16> kHexDigits{'0', '1', '2', '3', '4', '5', '6', '7',
                                            '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  output.push_back('"');
  for (std::size_t offset = 0; offset < value.size();) {
    const auto byte = byte_at(value, offset);
    if (byte >= 0x80U) {
      const std::size_t sequence_length = valid_utf8_sequence_length(value, offset);
      if (sequence_length == 0U) {
        output.append("\\ufffd");
        ++offset;
      } else {
        output.append(value.substr(offset, sequence_length));
        offset += sequence_length;
      }
      continue;
    }

    switch (byte) {
    case '"':
      output.append("\\\"");
      break;
    case '\\':
      output.append("\\\\");
      break;
    case '\b':
      output.append("\\b");
      break;
    case '\f':
      output.append("\\f");
      break;
    case '\n':
      output.append("\\n");
      break;
    case '\r':
      output.append("\\r");
      break;
    case '\t':
      output.append("\\t");
      break;
    default:
      if (byte < 0x20U) {
        output.append("\\u00");
        output.push_back(kHexDigits[(byte >> 4U) & 0x0fU]);
        output.push_back(kHexDigits[byte & 0x0fU]);
      } else {
        output.push_back(static_cast<char>(byte));
      }
      break;
    }
    ++offset;
  }
  output.push_back('"');
}

struct JsonStringField {
  std::string_view name;
  std::string_view value;
};

void append_json_field(std::string& output, const JsonStringField field) {
  append_json_string(output, field.name);
  output.push_back(':');
  append_json_string(output, field.value);
}

} // namespace

VersionInfo version_info() noexcept {
  return VersionInfo{
      .semantic_version = build::kSemanticVersion,
      .git_commit = build::kGitCommit,
      .git_metadata_available = build::kGitMetadataAvailable,
      .git_dirty = build::kGitDirty,
      .build_type = build::kBuildType,
      .compiler = build::kCompiler,
      .target_architecture = build::kTargetArchitecture,
      .operating_system = build::kOperatingSystem,
  };
}

std::string_view semantic_version() noexcept {
  return build::kSemanticVersion;
}

std::string version_text(const VersionInfo& info) {
  std::string output;
  output.reserve(192);
  output.append("ChronosDB ");
  output.append(info.semantic_version);
  output.append("\ngit commit: ");
  if (info.git_metadata_available) {
    output.append(info.git_commit);
    output.append(info.git_dirty ? " (dirty)" : " (clean)");
  } else {
    output.append("unavailable");
  }
  output.append("\nbuild type: ");
  output.append(info.build_type);
  output.append("\ncompiler: ");
  output.append(info.compiler);
  output.append("\ntarget architecture: ");
  output.append(info.target_architecture);
  output.append("\noperating system: ");
  output.append(info.operating_system);
  return output;
}

std::string version_text() {
  return version_text(version_info());
}

std::string version_json(const VersionInfo& info) {
  std::string output;
  output.reserve(256);
  output.push_back('{');
  append_json_field(output, {.name = "version", .value = info.semantic_version});
  output.push_back(',');
  append_json_string(output, "git_commit");
  output.push_back(':');
  if (info.git_metadata_available) {
    append_json_string(output, info.git_commit);
  } else {
    output.append("null");
  }
  output.push_back(',');
  append_json_string(output, "git_dirty");
  output.push_back(':');
  if (info.git_metadata_available) {
    output.append(info.git_dirty ? "true" : "false");
  } else {
    output.append("null");
  }
  output.push_back(',');
  append_json_field(output, {.name = "build_type", .value = info.build_type});
  output.push_back(',');
  append_json_field(output, {.name = "compiler", .value = info.compiler});
  output.push_back(',');
  append_json_field(output, {.name = "target_architecture", .value = info.target_architecture});
  output.push_back(',');
  append_json_field(output, {.name = "operating_system", .value = info.operating_system});
  output.push_back('}');
  return output;
}

std::string version_json() {
  return version_json(version_info());
}

} // namespace chronos::common
