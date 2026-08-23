#include "chronos/network/native_leader_redirect_router.hpp"

#include <algorithm>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool valid_endpoint(const Ipv4Endpoint& endpoint) noexcept {
  return endpoint.port != 0U && std::ranges::any_of(endpoint.address, [](const std::uint8_t octet) {
           return octet != 0U;
         });
}

} // namespace

NativeLeaderRedirectRouter::NativeLeaderRedirectRouter(
    NativeLeaderRedirectRouterConfig config) noexcept
    : config_(std::move(config)), current_node_id_(config_.initial_node_id) {}

common::Result<NativeLeaderRedirectRouter>
NativeLeaderRedirectRouter::create(NativeLeaderRedirectRouterConfig config) {
  if (config.group_id.is_nil() || config.initial_node_id == 0U ||
      config.minimum_placement_epoch == 0U || config.limits.maximum_routes == 0U ||
      config.limits.maximum_routes > 65'536U || config.limits.maximum_redirects == 0U ||
      config.limits.maximum_redirects > 65'536U || config.routes.empty()) {
    return common::make_unexpected(
        invalid("native leader redirect router configuration is invalid"));
  }
  if (config.routes.size() > config.limits.maximum_routes)
    return common::make_unexpected(exhausted("native leader route count exceeds limit"));
  try {
    std::vector<Ipv4Endpoint> endpoints;
    endpoints.reserve(config.routes.size());
    for (std::size_t index = 0U; index < config.routes.size(); ++index) {
      const NativeLeaderRoute& route = config.routes[index];
      if (route.node_id == 0U || !valid_endpoint(route.endpoint) || route.tls_context == nullptr ||
          (index != 0U && config.routes[index - 1U].node_id >= route.node_id)) {
        return common::make_unexpected(invalid("native leader routes are not canonical"));
      }
      endpoints.push_back(route.endpoint);
    }
    std::ranges::sort(endpoints, [](const Ipv4Endpoint& left, const Ipv4Endpoint& right) {
      return left.address != right.address ? left.address < right.address : left.port < right.port;
    });
    if (std::ranges::adjacent_find(endpoints) != endpoints.end())
      return common::make_unexpected(invalid("native leader route endpoint is duplicated"));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("native leader route validation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("native leader routes exceed container limits"));
  }
  if (!std::ranges::binary_search(config.routes, config.initial_node_id, {},
                                  &NativeLeaderRoute::node_id)) {
    return common::make_unexpected(
        invalid("native leader initial node has no authenticated route"));
  }
  return NativeLeaderRedirectRouter{std::move(config)};
}

common::Result<NativeLeaderRedirectTarget>
NativeLeaderRedirectRouter::accept(const LeaderRedirect& redirect) {
  if (redirect.group_id.is_nil() || redirect.leader_node_id == 0U || redirect.leader_term == 0U ||
      redirect.placement_epoch == 0U) {
    return common::make_unexpected(invalid("native leader redirect is malformed"));
  }
  if (redirect.group_id != config_.group_id)
    return common::make_unexpected(invalid("native leader redirect names a different group"));
  if (redirect.placement_epoch < config_.minimum_placement_epoch ||
      (last_authority_.has_value() &&
       (redirect.placement_epoch < last_authority_->placement_epoch ||
        redirect.leader_term < last_authority_->leader_term))) {
    return common::make_unexpected(unavailable("native leader redirect authority is stale"));
  }
  if (last_authority_.has_value() && redirect.leader_term == last_authority_->leader_term &&
      redirect.leader_node_id != last_authority_->leader_node_id) {
    return common::make_unexpected(
        unavailable("native leader redirect conflicts within one Raft term"));
  }
  if (redirect.leader_node_id == current_node_id_)
    return common::make_unexpected(
        unavailable("native leader redirect points to the current node"));
  const auto route = std::ranges::lower_bound(config_.routes, redirect.leader_node_id, {},
                                              &NativeLeaderRoute::node_id);
  if (route == config_.routes.end() || route->node_id != redirect.leader_node_id)
    return common::make_unexpected(unavailable("native leader has no authenticated route"));
  if (accepted_redirects_ == config_.limits.maximum_redirects)
    return common::make_unexpected(exhausted("native leader redirect retry limit exceeded"));

  ++accepted_redirects_;
  current_node_id_ = redirect.leader_node_id;
  last_authority_ = redirect;
  return NativeLeaderRedirectTarget{*route, redirect, accepted_redirects_};
}

std::uint64_t NativeLeaderRedirectRouter::current_node_id() const noexcept {
  return current_node_id_;
}

std::size_t NativeLeaderRedirectRouter::accepted_redirects() const noexcept {
  return accepted_redirects_;
}

std::optional<LeaderRedirect> NativeLeaderRedirectRouter::last_authority() const noexcept {
  return last_authority_;
}

std::span<const NativeLeaderRoute> NativeLeaderRedirectRouter::routes() const noexcept {
  return config_.routes;
}

} // namespace chronos::network
