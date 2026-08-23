#include "chronos/service/native_client_route_config.hpp"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <new>
#include <ranges>
#include <stdexcept>
#include <string>
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

template <typename Integer>
[[nodiscard]] bool parse_canonical_decimal(const std::string_view text, Integer& value) {
  if (text.empty() || (text.size() > 1U && text.front() == '0'))
    return false;
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

[[nodiscard]] bool is_lowercase_dns_identity(const std::string_view text) {
  if (text.empty() || text.size() > 253U || text.front() == '.' || text.back() == '.')
    return false;
  std::size_t label_start = 0U;
  for (std::size_t index = 0U; index <= text.size(); ++index) {
    if (index != text.size() && text[index] != '.')
      continue;
    const std::size_t label_size = index - label_start;
    if (label_size == 0U || label_size > 63U || text[label_start] == '-' || text[index - 1U] == '-')
      return false;
    for (std::size_t character = label_start; character < index; ++character) {
      const char value = text[character];
      if (!((value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '-'))
        return false;
    }
    label_start = index + 1U;
  }
  return true;
}

[[nodiscard]] bool is_canonical_ipv4_identity(const std::string_view text) {
  const auto endpoint = network::parse_ipv4_endpoint(std::string{text} + ":1");
  return endpoint.has_value();
}

[[nodiscard]] bool is_canonical_tls_identity(const std::string_view text) {
  const bool numeric = std::ranges::all_of(
      text, [](const char value) { return (value >= '0' && value <= '9') || value == '.'; });
  return numeric ? is_canonical_ipv4_identity(text) : is_lowercase_dns_identity(text);
}

[[nodiscard]] bool valid_endpoint(const network::Ipv4Endpoint& endpoint) noexcept {
  return endpoint.port != 0U && std::ranges::any_of(endpoint.address, [](const std::uint8_t octet) {
           return octet != 0U;
         });
}

[[nodiscard]] common::Result<network::PeerCertificateSha256>
parse_fingerprint(const std::string_view text) {
  if (text.size() != 64U)
    return common::make_unexpected(
        invalid("native client certificate fingerprint length is invalid"));
  network::PeerCertificateSha256 fingerprint{};
  const auto nibble = [](const char value) -> int {
    if (value >= '0' && value <= '9')
      return value - '0';
    if (value >= 'a' && value <= 'f')
      return value - 'a' + 10;
    return -1;
  };
  for (std::size_t index = 0U; index < fingerprint.size(); ++index) {
    const int high = nibble(text[index * 2U]);
    const int low = nibble(text[index * 2U + 1U]);
    if (high < 0 || low < 0)
      return common::make_unexpected(
          invalid("native client certificate fingerprint is not lowercase hexadecimal"));
    fingerprint[index] = static_cast<std::uint8_t>((high << 4) | low);
  }
  return fingerprint;
}

} // namespace

common::Result<std::vector<NativeClientRoute>>
parse_native_client_route_config(const std::string_view text,
                                 const NativeClientRouteConfigLimits limits) {
  if (limits.maximum_bytes == 0U || limits.maximum_nodes == 0U || limits.maximum_nodes > 65535U ||
      limits.maximum_tls_identity_bytes == 0U || limits.maximum_tls_identity_bytes > 253U)
    return common::make_unexpected(invalid("native client route config limits are invalid"));
  if (text.empty() || text.size() > limits.maximum_bytes)
    return common::make_unexpected(exhausted("native client route config size is invalid"));
  if (text.find('\r') != std::string_view::npos)
    return common::make_unexpected(
        invalid("native client route config contains a carriage return"));
  const std::size_t first_lf = text.find('\n');
  if (first_lf == std::string_view::npos ||
      text.substr(0U, first_lf) != kNativeClientRouteConfigV1Magic)
    return common::make_unexpected(invalid("native client route config magic is invalid"));

  try {
    std::vector<NativeClientRoute> routes;
    routes.reserve(std::min(limits.maximum_nodes, text.size() - first_lf - 1U));
    std::size_t offset = first_lf + 1U;
    while (offset < text.size()) {
      const std::size_t separator = text.find('\n', offset);
      const std::size_t end = separator == std::string_view::npos ? text.size() : separator;
      const std::string_view line = text.substr(offset, end - offset);
      if (line.empty())
        return common::make_unexpected(invalid("native client route config contains a blank line"));
      const std::size_t equals = line.find('=');
      if (equals == std::string_view::npos || line.find('=', equals + 1U) != std::string_view::npos)
        return common::make_unexpected(invalid("native client route config line is malformed"));
      const std::size_t first_comma = line.find(',', equals + 1U);
      if (first_comma == std::string_view::npos)
        return common::make_unexpected(invalid("native client route config line is malformed"));
      const std::size_t second_comma = line.find(',', first_comma + 1U);
      if (second_comma == std::string_view::npos ||
          line.find(',', second_comma + 1U) != std::string_view::npos)
        return common::make_unexpected(invalid("native client route config line is malformed"));

      std::uint64_t node_id{};
      if (!parse_canonical_decimal(line.substr(0U, equals), node_id) || node_id == 0U)
        return common::make_unexpected(invalid("native client route node ID is invalid"));
      if (!routes.empty() && routes.back().node_id >= node_id)
        return common::make_unexpected(
            invalid("native client route node IDs are not strictly increasing"));
      auto endpoint =
          network::parse_ipv4_endpoint(line.substr(equals + 1U, first_comma - equals - 1U));
      if (!endpoint.has_value())
        return common::make_unexpected(endpoint.error());
      if (!valid_endpoint(*endpoint))
        return common::make_unexpected(invalid("native client route endpoint is not usable"));
      const std::string_view identity =
          line.substr(first_comma + 1U, second_comma - first_comma - 1U);
      if (identity.size() > limits.maximum_tls_identity_bytes ||
          !is_canonical_tls_identity(identity))
        return common::make_unexpected(invalid("native client TLS identity is not canonical"));
      auto fingerprint = parse_fingerprint(line.substr(second_comma + 1U));
      if (!fingerprint.has_value())
        return common::make_unexpected(fingerprint.error());
      if (routes.size() >= limits.maximum_nodes)
        return common::make_unexpected(exhausted("native client route count limit exceeded"));
      if (std::ranges::any_of(routes, [&](const NativeClientRoute& route) {
            return route.endpoint == *endpoint || route.certificate_sha256 == *fingerprint;
          }))
        return common::make_unexpected(
            invalid("native client endpoint or certificate fingerprint is duplicated"));
      routes.push_back({node_id, *endpoint, std::string{identity}, *fingerprint});
      if (separator == std::string_view::npos)
        break;
      offset = separator + 1U;
    }
    if (routes.empty())
      return common::make_unexpected(invalid("native client route config has no nodes"));
    return routes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("native client route config allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("native client route config exceeds container limits"));
  }
}

} // namespace chronos::service
