#include "chronos/service/native_server_principal_config.hpp"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <new>
#include <ranges>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool parse_principal_id(const std::string_view text, std::uint64_t& output) noexcept {
  if (text.empty() || (text.size() > 1U && text.front() == '0'))
    return false;
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), output);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size() && output != 0U;
}

[[nodiscard]] common::Result<network::PeerCertificateSha256>
parse_fingerprint(const std::string_view text) {
  if (text.size() != 64U)
    return common::make_unexpected(
        invalid("native server principal certificate fingerprint length is invalid"));
  network::PeerCertificateSha256 fingerprint{};
  const auto nibble = [](const char value) noexcept -> int {
    if (value >= '0' && value <= '9')
      return value - '0';
    if (value >= 'a' && value <= 'f')
      return value - 'a' + 10;
    return -1;
  };
  for (std::size_t index = 0U; index < fingerprint.size(); ++index) {
    const int high = nibble(text[index * 2U]);
    const int low = nibble(text[index * 2U + 1U]);
    if (high < 0 || low < 0) {
      return common::make_unexpected(
          invalid("native server principal fingerprint is not lowercase hexadecimal"));
    }
    fingerprint[index] = static_cast<std::uint8_t>((high << 4) | low);
  }
  return fingerprint;
}

} // namespace

common::Result<std::vector<NativeServerPrincipal>>
parse_native_server_principal_config(const std::string_view text,
                                     const NativeServerPrincipalConfigLimits limits) {
  if (limits.maximum_bytes == 0U || limits.maximum_principals == 0U ||
      limits.maximum_principals > 65'536U) {
    return common::make_unexpected(invalid("native server principal config limits are invalid"));
  }
  if (text.empty())
    return common::make_unexpected(invalid("native server principal config is empty"));
  if (text.size() > limits.maximum_bytes) {
    return common::make_unexpected(exhausted("native server principal config size limit exceeded"));
  }
  if (text.find('\r') != std::string_view::npos) {
    return common::make_unexpected(
        invalid("native server principal config contains a carriage return"));
  }
  const std::size_t first_lf = text.find('\n');
  if (first_lf == std::string_view::npos ||
      text.substr(0U, first_lf) != kNativeServerPrincipalConfigV1Magic) {
    return common::make_unexpected(invalid("native server principal config magic is invalid"));
  }

  try {
    std::vector<NativeServerPrincipal> principals;
    principals.reserve(std::min(limits.maximum_principals, text.size() - first_lf - 1U));
    std::size_t offset = first_lf + 1U;
    while (offset < text.size()) {
      const std::size_t separator = text.find('\n', offset);
      const std::size_t end = separator == std::string_view::npos ? text.size() : separator;
      const std::string_view line = text.substr(offset, end - offset);
      if (line.empty()) {
        return common::make_unexpected(
            invalid("native server principal config contains a blank line"));
      }
      const std::size_t equals = line.find('=');
      if (equals == std::string_view::npos ||
          line.find('=', equals + 1U) != std::string_view::npos) {
        return common::make_unexpected(invalid("native server principal config line is malformed"));
      }
      std::uint64_t principal_id{};
      if (!parse_principal_id(line.substr(0U, equals), principal_id)) {
        return common::make_unexpected(invalid("native server principal ID is invalid"));
      }
      if (!principals.empty() && principals.back().principal_id >= principal_id) {
        return common::make_unexpected(
            invalid("native server principal IDs are not strictly increasing"));
      }
      auto fingerprint = parse_fingerprint(line.substr(equals + 1U));
      if (!fingerprint.has_value())
        return common::make_unexpected(fingerprint.error());
      if (principals.size() >= limits.maximum_principals) {
        return common::make_unexpected(exhausted("native server principal count limit exceeded"));
      }
      if (std::ranges::any_of(principals, [&](const NativeServerPrincipal& principal) {
            return principal.certificate_sha256 == *fingerprint;
          })) {
        return common::make_unexpected(
            invalid("native server principal certificate fingerprint is duplicated"));
      }
      principals.push_back({principal_id, *fingerprint});
      if (separator == std::string_view::npos)
        break;
      offset = separator + 1U;
    }
    if (principals.empty())
      return common::make_unexpected(invalid("native server principal config has no principals"));
    return principals;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("native server principal config allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("native server principal config exceeds container limits"));
  }
}

} // namespace chronos::service
