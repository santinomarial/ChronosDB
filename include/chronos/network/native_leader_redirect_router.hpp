#ifndef CHRONOS_NETWORK_NATIVE_LEADER_REDIRECT_ROUTER_HPP_
#define CHRONOS_NETWORK_NATIVE_LEADER_REDIRECT_ROUTER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace chronos::network {

struct NativeLeaderRoute {
  std::uint64_t node_id{};
  Ipv4Endpoint endpoint;
  const TlsClientContext* tls_context{};

  friend bool operator==(const NativeLeaderRoute&, const NativeLeaderRoute&) = default;
};

struct NativeLeaderRedirectRouterLimits {
  std::size_t maximum_routes{1024U};
  std::size_t maximum_redirects{8U};
};

struct NativeLeaderRedirectRouterConfig {
  common::Uuid group_id;
  std::uint64_t initial_node_id{};
  std::uint64_t minimum_placement_epoch{};
  std::vector<NativeLeaderRoute> routes;
  NativeLeaderRedirectRouterLimits limits;
};

struct NativeLeaderRedirectTarget {
  NativeLeaderRoute route;
  LeaderRedirect authority;
  std::size_t redirect_number{};

  friend bool operator==(const NativeLeaderRedirectTarget&,
                         const NativeLeaderRedirectTarget&) = default;
};

// Single-threaded policy owner for one finite native request. Routes are an immutable,
// deployment-authenticated node map: each borrowed TLS context must outlive any carrier attempt
// that uses a returned target. The router does not treat redirects as leases and does not open
// sockets; every destination must repeat normal protocol and consistency checks.
class NativeLeaderRedirectRouter {
public:
  NativeLeaderRedirectRouter() = delete;
  NativeLeaderRedirectRouter(const NativeLeaderRedirectRouter&) = delete;
  NativeLeaderRedirectRouter& operator=(const NativeLeaderRedirectRouter&) = delete;
  NativeLeaderRedirectRouter(NativeLeaderRedirectRouter&&) noexcept = default;
  NativeLeaderRedirectRouter& operator=(NativeLeaderRedirectRouter&&) noexcept = default;

  [[nodiscard]] static common::Result<NativeLeaderRedirectRouter>
  create(NativeLeaderRedirectRouterConfig config);

  [[nodiscard]] common::Result<NativeLeaderRedirectTarget> accept(const LeaderRedirect& redirect);

  [[nodiscard]] std::uint64_t current_node_id() const noexcept;
  [[nodiscard]] std::size_t accepted_redirects() const noexcept;
  [[nodiscard]] std::optional<LeaderRedirect> last_authority() const noexcept;
  [[nodiscard]] std::span<const NativeLeaderRoute> routes() const noexcept;

private:
  explicit NativeLeaderRedirectRouter(NativeLeaderRedirectRouterConfig config) noexcept;

  NativeLeaderRedirectRouterConfig config_;
  std::uint64_t current_node_id_{};
  std::size_t accepted_redirects_{};
  std::optional<LeaderRedirect> last_authority_;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_NATIVE_LEADER_REDIRECT_ROUTER_HPP_
