#include "chronos/service/native_client_route_authority.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status unauthenticated(const char* message) {
  return {common::StatusCode::kUnauthenticated, message};
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
  return network::parse_ipv4_endpoint(std::string{text} + ":1").has_value();
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

} // namespace

class NativeClientRouteAuthority::Impl {
public:
  explicit Impl(std::vector<NativeClientRoute> configured) noexcept
      : configured_routes(std::move(configured)) {}

  [[nodiscard]] const NativeClientRoute* find(const std::uint64_t node_id) const noexcept {
    const auto found =
        std::ranges::lower_bound(configured_routes, node_id, {}, &NativeClientRoute::node_id);
    return found != configured_routes.end() && found->node_id == node_id ? &*found : nullptr;
  }

  std::vector<NativeClientRoute> configured_routes;
};

NativeClientRouteAuthority::NativeClientRouteAuthority(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
NativeClientRouteAuthority::~NativeClientRouteAuthority() = default;
NativeClientRouteAuthority::NativeClientRouteAuthority(NativeClientRouteAuthority&&) noexcept =
    default;
NativeClientRouteAuthority&
NativeClientRouteAuthority::operator=(NativeClientRouteAuthority&&) noexcept = default;

common::Result<NativeClientRouteAuthority>
NativeClientRouteAuthority::create(std::vector<NativeClientRoute> routes) {
  if (routes.empty())
    return common::make_unexpected(invalid("native client route authority is empty"));
  try {
    for (std::size_t index = 0U; index < routes.size(); ++index) {
      const NativeClientRoute& route = routes[index];
      if (route.node_id == 0U || !valid_endpoint(route.endpoint) ||
          !is_canonical_tls_identity(route.tls_server_identity))
        return common::make_unexpected(invalid("native client route authority entry is invalid"));
      if (index != 0U && routes[index - 1U].node_id >= route.node_id)
        return common::make_unexpected(
            invalid("native client route authority node IDs are not strictly increasing"));
      if (std::ranges::any_of(std::span{routes}.first(index),
                              [&](const NativeClientRoute& previous) {
                                return previous.endpoint == route.endpoint ||
                                       previous.certificate_sha256 == route.certificate_sha256;
                              }))
        return common::make_unexpected(
            invalid("native client route authority contains duplicate route authority"));
    }
    return NativeClientRouteAuthority{std::make_unique<Impl>(std::move(routes))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("native client route authority allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("native client route authority exceeds limits"));
  }
}

common::Result<network::PeerAuthenticationResult>
NativeClientRouteAuthority::authenticate(const network::PeerAuthenticationRequest& request) {
  if (!request.transport_authenticated || !request.peer_certificate_sha256.has_value())
    return common::make_unexpected(
        unauthenticated("native client route authority requires a verified TLS certificate"));
  const auto found = std::ranges::find(impl_->configured_routes, *request.peer_certificate_sha256,
                                       &NativeClientRoute::certificate_sha256);
  if (found == impl_->configured_routes.end() || found->endpoint.address != request.ipv4_address)
    return network::PeerAuthenticationResult{};
  return network::PeerAuthenticationResult{.authorized = true, .principal_id = found->node_id};
}

common::Result<bool>
NativeClientRouteAuthority::authorize_node(const std::uint64_t principal_id,
                                           const std::uint64_t claimed_node_id) const {
  return principal_id != 0U && principal_id == claimed_node_id &&
         impl_->find(claimed_node_id) != nullptr;
}

const NativeClientRoute*
NativeClientRouteAuthority::find_route(const std::uint64_t node_id) const noexcept {
  return impl_->find(node_id);
}

std::span<const NativeClientRoute> NativeClientRouteAuthority::routes() const noexcept {
  return impl_->configured_routes;
}

} // namespace chronos::service
