#include "chronos/manifest/naming.hpp"

#include "chronos/common/status.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace chronos::manifest {
namespace {

constexpr std::string_view kPartPrefix = "part-";
constexpr std::string_view kPartSuffix = ".cseg";
constexpr std::string_view kManifestPrefix = "manifest-";
constexpr std::string_view kManifestSuffix = ".cman";
constexpr std::string_view kTemporarySuffix = ".tmp-";
constexpr std::size_t kHexLength = common::Uuid::kSize * 2U;
constexpr std::size_t kGenerationLength = 20U;
constexpr std::array<char, 16> kHexDigits{'0', '1', '2', '3', '4', '5', '6', '7',
                                          '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

void append_hex(std::string& output, const common::Uuid::Bytes& bytes) {
  output.reserve(output.size() + kHexLength);
  for (const std::byte byte : bytes) {
    const std::uint8_t value = std::to_integer<std::uint8_t>(byte);
    output.push_back(kHexDigits[value >> 4U]);
    output.push_back(kHexDigits[value & 0x0fU]);
  }
}

[[nodiscard]] common::Result<common::Uuid> parse_uuid_hex(const std::string_view text) {
  if (text.size() != kHexLength) {
    return common::make_unexpected(invalid("Manifest basename UUID must contain 32 hex digits"));
  }
  common::Uuid::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    const auto nibble = [](const char value) -> int {
      if (value >= '0' && value <= '9') {
        return value - '0';
      }
      if (value >= 'a' && value <= 'f') {
        return value - 'a' + 10;
      }
      return -1;
    };
    const int high = nibble(text[index * 2U]);
    const int low = nibble(text[index * 2U + 1U]);
    if (high < 0 || low < 0) {
      return common::make_unexpected(
          invalid("Manifest basename UUID is not canonical lowercase hexadecimal"));
    }
    bytes[index] = std::byte{static_cast<std::uint8_t>((high << 4U) | low)};
  }
  return common::Uuid{bytes};
}

[[nodiscard]] common::Result<std::uint64_t> parse_generation(const std::string_view text) {
  if (text.size() != kGenerationLength) {
    return common::make_unexpected(
        invalid("Manifest generation must contain exactly 20 decimal digits"));
  }
  std::uint64_t generation = 0U;
  for (const char digit_character : text) {
    if (digit_character < '0' || digit_character > '9') {
      return common::make_unexpected(invalid("Manifest generation is not decimal"));
    }
    const std::uint64_t digit = static_cast<std::uint64_t>(digit_character - '0');
    if (generation > (std::numeric_limits<std::uint64_t>::max() - digit) / 10U) {
      return common::make_unexpected(invalid("Manifest generation exceeds uint64"));
    }
    generation = generation * 10U + digit;
  }
  if (generation == 0U) {
    return common::make_unexpected(invalid("Manifest generation must be nonzero"));
  }
  return generation;
}

[[nodiscard]] common::Result<std::array<char, kGenerationLength>>
generation_digits(const std::uint64_t generation) {
  if (generation == 0U) {
    return common::make_unexpected(invalid("Manifest generation must be nonzero"));
  }
  std::array<char, kGenerationLength> digits{};
  std::uint64_t remaining = generation;
  for (std::size_t index = digits.size(); index > 0U; --index) {
    digits[index - 1U] = static_cast<char>('0' + (remaining % 10U));
    remaining /= 10U;
  }
  return digits;
}

} // namespace

std::string part_file_name(const cseg::PartId& part_id) {
  std::string name{kPartPrefix};
  append_hex(name, part_id.bytes());
  name.append(kPartSuffix);
  return name;
}

std::string temporary_part_file_name(const cseg::PartId& part_id, const common::Uuid& nonce) {
  std::string name{"."};
  name.append(part_file_name(part_id));
  name.append(kTemporarySuffix);
  append_hex(name, nonce.bytes());
  return name;
}

common::Result<cseg::PartId> parse_part_file_name(const std::string_view name) {
  constexpr std::size_t kLength = kPartPrefix.size() + kHexLength + kPartSuffix.size();
  if (name.size() != kLength || !name.starts_with(kPartPrefix) || !name.ends_with(kPartSuffix)) {
    return common::make_unexpected(invalid("Installed part basename does not match v1 grammar"));
  }
  const common::Result<common::Uuid> uuid =
      parse_uuid_hex(name.substr(kPartPrefix.size(), kHexLength));
  if (!uuid.has_value()) {
    return common::make_unexpected(uuid.error());
  }
  return cseg::PartId::from_uuid(*uuid);
}

common::Result<TemporaryPartName> parse_temporary_part_file_name(const std::string_view name) {
  constexpr std::size_t kFinalLength = kPartPrefix.size() + kHexLength + kPartSuffix.size();
  constexpr std::size_t kLength = 1U + kFinalLength + kTemporarySuffix.size() + kHexLength;
  if (name.size() != kLength || !name.starts_with('.') ||
      name.substr(1U + kFinalLength, kTemporarySuffix.size()) != kTemporarySuffix) {
    return common::make_unexpected(invalid("Temporary part basename does not match v1 grammar"));
  }
  const common::Result<cseg::PartId> part_id = parse_part_file_name(name.substr(1U, kFinalLength));
  if (!part_id.has_value()) {
    return common::make_unexpected(part_id.error());
  }
  const common::Result<common::Uuid> nonce =
      parse_uuid_hex(name.substr(1U + kFinalLength + kTemporarySuffix.size()));
  if (!nonce.has_value()) {
    return common::make_unexpected(nonce.error());
  }
  return TemporaryPartName{.part_id = *part_id, .nonce = *nonce};
}

common::Result<std::string> manifest_file_name(const std::uint64_t generation) {
  const common::Result<std::array<char, kGenerationLength>> digits = generation_digits(generation);
  if (!digits.has_value()) {
    return common::make_unexpected(digits.error());
  }
  std::string name{kManifestPrefix};
  name.append(digits->data(), digits->size());
  name.append(kManifestSuffix);
  return name;
}

common::Result<std::string> temporary_manifest_file_name(const std::uint64_t generation,
                                                         const common::Uuid& nonce) {
  const common::Result<std::string> final_name = manifest_file_name(generation);
  if (!final_name.has_value()) {
    return common::make_unexpected(final_name.error());
  }
  std::string name{"."};
  name.append(*final_name);
  name.append(kTemporarySuffix);
  append_hex(name, nonce.bytes());
  return name;
}

common::Result<std::uint64_t> parse_manifest_file_name(const std::string_view name) {
  constexpr std::size_t kLength =
      kManifestPrefix.size() + kGenerationLength + kManifestSuffix.size();
  if (name.size() != kLength || !name.starts_with(kManifestPrefix) ||
      !name.ends_with(kManifestSuffix)) {
    return common::make_unexpected(
        invalid("Installed manifest basename does not match v1 grammar"));
  }
  return parse_generation(name.substr(kManifestPrefix.size(), kGenerationLength));
}

common::Result<TemporaryManifestName>
parse_temporary_manifest_file_name(const std::string_view name) {
  constexpr std::size_t kFinalLength =
      kManifestPrefix.size() + kGenerationLength + kManifestSuffix.size();
  constexpr std::size_t kLength = 1U + kFinalLength + kTemporarySuffix.size() + kHexLength;
  if (name.size() != kLength || !name.starts_with('.') ||
      name.substr(1U + kFinalLength, kTemporarySuffix.size()) != kTemporarySuffix) {
    return common::make_unexpected(
        invalid("Temporary manifest basename does not match v1 grammar"));
  }
  const common::Result<std::uint64_t> generation =
      parse_manifest_file_name(name.substr(1U, kFinalLength));
  if (!generation.has_value()) {
    return common::make_unexpected(generation.error());
  }
  const common::Result<common::Uuid> nonce =
      parse_uuid_hex(name.substr(1U + kFinalLength + kTemporarySuffix.size()));
  if (!nonce.has_value()) {
    return common::make_unexpected(nonce.error());
  }
  return TemporaryManifestName{.generation = *generation, .nonce = *nonce};
}

} // namespace chronos::manifest
