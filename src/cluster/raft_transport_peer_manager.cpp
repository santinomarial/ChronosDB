#include "chronos/cluster/raft_transport_peer_manager.hpp"

#include <new>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {
[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}
} // namespace

class RaftTransportPeerManager::Impl {
public:
  struct Route {
    raft::NodeId peer{};
    RaftTransportPeerReconnect reconnect;
  };
  Impl(RaftTransportPeerPool owned_pool, std::vector<std::optional<Route>> owned_routes,
       const std::size_t count) noexcept
      : pool(std::move(owned_pool)), routes(std::move(owned_routes)), route_count(count) {}
  [[nodiscard]] Route* find(const raft::NodeId peer) noexcept {
    for (std::optional<Route>& route : routes)
      if (route.has_value() && route->peer == peer)
        return &*route;
    return nullptr;
  }
  [[nodiscard]] const Route* find(const raft::NodeId peer) const noexcept {
    for (const std::optional<Route>& route : routes)
      if (route.has_value() && route->peer == peer)
        return &*route;
    return nullptr;
  }
  [[nodiscard]] common::Status install_ready(Route& route) {
    if (route.reconnect.state() != RaftTransportPeerReconnectState::kCarrierReady)
      return common::Status::ok();
    if (pool.find_peer(route.peer) != nullptr)
      return status(common::StatusCode::kCorruption,
                    "Raft peer manager has duplicate connected ownership");
    auto connected = route.reconnect.take_connected_peer();
    if (!connected.has_value())
      return connected.error();
    const common::Status installed = pool.add_connected_peer(std::move(*connected));
    return installed.is_ok() ? installed
                             : status(common::StatusCode::kCorruption,
                                      "Raft peer manager could not install its reserved peer slot");
  }
  RaftTransportPeerPool pool;
  std::vector<std::optional<Route>> routes;
  std::size_t route_count{};
};

RaftTransportPeerManager::RaftTransportPeerManager(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftTransportPeerManager::~RaftTransportPeerManager() = default;
RaftTransportPeerManager::RaftTransportPeerManager(RaftTransportPeerManager&&) noexcept = default;
RaftTransportPeerManager&
RaftTransportPeerManager::operator=(RaftTransportPeerManager&&) noexcept = default;

common::Result<RaftTransportPeerManager>
RaftTransportPeerManager::create(RaftTransportPeerManagerConfig config) {
  if (config.local_node_id == 0U || config.peers.empty() ||
      config.peers.size() > config.pool.maximum_peers)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft peer manager configuration is invalid"));
  auto pool = RaftTransportPeerPool::create(config.local_node_id, config.pool);
  if (!pool.has_value())
    return common::make_unexpected(pool.error());
  try {
    std::vector<std::optional<Impl::Route>> routes(config.peers.size());
    std::size_t count{};
    for (const RaftTransportPeerReconnectConfig& peer : config.peers) {
      const raft::NodeId id = peer.connector.carrier.peer_node_id;
      if (peer.connector.carrier.local_node_id != config.local_node_id || id == 0U)
        return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                              "Raft peer manager route ownership is invalid"));
      for (std::size_t index = 0U; index < count; ++index)
        if (routes[index]->peer == id)
          return common::make_unexpected(
              status(common::StatusCode::kAlreadyExists, "Raft peer manager route is duplicated"));
      auto reconnect = RaftTransportPeerReconnect::create(peer);
      if (!reconnect.has_value())
        return common::make_unexpected(reconnect.error());
      routes[count++].emplace(Impl::Route{id, std::move(*reconnect)});
    }
    return RaftTransportPeerManager{
        std::make_unique<Impl>(std::move(*pool), std::move(routes), count)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Raft peer manager allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft peer manager exceeds container limits"));
  }
}

common::Status RaftTransportPeerManager::drive(const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft peer manager is empty");
  for (std::optional<Impl::Route>& route : implementation_->routes) {
    if (!route.has_value())
      continue;
    common::Status progress = route->reconnect.drive(now);
    if (!progress.is_ok())
      continue;
    progress = implementation_->install_ready(*route);
    if (!progress.is_ok())
      return progress;
  }
  return common::Status::ok();
}

common::Status RaftTransportPeerManager::on_ready(const raft::NodeId peer, const bool readable,
                                                  const bool writable, const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft peer manager is empty");
  Impl::Route* route = implementation_->find(peer);
  if (route == nullptr)
    return status(common::StatusCode::kNotFound, "Raft peer manager route does not exist");
  if (route->reconnect.state() == RaftTransportPeerReconnectState::kConnecting) {
    const common::Status progress = route->reconnect.on_ready(writable, now);
    if (!progress.is_ok())
      return common::Status::ok();
    return implementation_->install_ready(*route);
  }
  if (route->reconnect.state() != RaftTransportPeerReconnectState::kConnected)
    return common::Status::ok();
  const common::Status progress = implementation_->pool.on_ready(peer, readable, writable, now);
  if (progress.is_ok())
    return progress;
  auto failed = implementation_->pool.take_failed_peer(peer);
  if (!failed.has_value())
    return failed.error();
  const common::Status retained = route->reconnect.accept_failed_peer(std::move(*failed), now);
  return retained.is_ok() ? common::Status::ok() : retained;
}

common::Status RaftTransportPeerManager::on_transport_closed(const raft::NodeId peer,
                                                             const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft peer manager is empty");
  Impl::Route* route = implementation_->find(peer);
  if (route == nullptr)
    return status(common::StatusCode::kNotFound, "Raft peer manager route does not exist");
  if (route->reconnect.state() == RaftTransportPeerReconnectState::kConnecting)
    return on_ready(peer, false, true, now);
  if (route->reconnect.state() != RaftTransportPeerReconnectState::kConnected)
    return common::Status::ok();
  const common::Status closed = implementation_->pool.on_transport_closed(peer);
  if (!closed.is_ok())
    return closed;
  auto failed = implementation_->pool.take_failed_peer(peer);
  if (!failed.has_value())
    return failed.error();
  return route->reconnect.accept_failed_peer(std::move(*failed), now);
}

common::Status RaftTransportPeerManager::route_result(const raft::GroupId& group,
                                                      const raft::DurableRaftResult& result,
                                                      const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft peer manager is empty");
  return implementation_->pool.route_result(group, result, now);
}

common::Result<std::vector<RaftTransportPeerInterest>> RaftTransportPeerManager::interests() const {
  if (!implementation_)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft peer manager is empty"));
  try {
    std::vector<RaftTransportPeerInterest> interests;
    interests.reserve(implementation_->route_count);
    for (const std::optional<Impl::Route>& route : implementation_->routes) {
      if (!route.has_value())
        continue;
      if (route->reconnect.state() == RaftTransportPeerReconnectState::kConnecting) {
        interests.push_back(
            {route->peer, route->reconnect.descriptor(), false, route->reconnect.wants_write()});
      } else if (route->reconnect.state() == RaftTransportPeerReconnectState::kConnected) {
        const RaftTransportTlsClient* carrier = implementation_->pool.find_peer(route->peer);
        if (carrier == nullptr)
          return common::make_unexpected(status(common::StatusCode::kCorruption,
                                                "Raft peer manager connected carrier is absent"));
        const RaftTransportTlsClientInterest interest = carrier->interest();
        interests.push_back({route->peer, implementation_->pool.peer_descriptor(route->peer),
                             interest.want_read, interest.want_write});
      }
    }
    return interests;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft peer manager interest allocation failed"));
  }
}

std::optional<RaftTransportPeerManager::TimePoint>
RaftTransportPeerManager::next_deadline() const noexcept {
  std::optional<TimePoint> next;
  if (!implementation_)
    return next;
  for (const std::optional<Impl::Route>& route : implementation_->routes) {
    if (!route.has_value())
      continue;
    const auto deadline = route->reconnect.next_deadline();
    if (deadline.has_value() && (!next.has_value() || *deadline < *next))
      next = deadline;
  }
  const auto carrier_deadline = implementation_->pool.next_deadline();
  if (carrier_deadline.has_value() && (!next.has_value() || *carrier_deadline < *next))
    next = carrier_deadline;
  return next;
}

std::size_t RaftTransportPeerManager::configured_peer_count() const noexcept {
  return implementation_ ? implementation_->route_count : 0U;
}
std::size_t RaftTransportPeerManager::connected_peer_count() const noexcept {
  return implementation_ ? implementation_->pool.peer_count() : 0U;
}

} // namespace chronos::cluster
