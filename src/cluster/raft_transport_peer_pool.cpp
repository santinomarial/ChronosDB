#include "chronos/cluster/raft_transport_peer_pool.hpp"

#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {
[[nodiscard]] common::Status status(common::StatusCode code, const char* message) {
  return {code, message};
}
} // namespace

class RaftTransportPeerPool::Impl {
public:
  struct Peer {
    raft::NodeId node_id{};
    RaftTransportTlsClient carrier;
  };
  Impl(raft::NodeId local, RaftTransportPeerPoolLimits limits,
       std::vector<std::optional<Peer>> peers) noexcept
      : local_(local), limits_(limits), peers_(std::move(peers)) {}
  [[nodiscard]] Peer* find(raft::NodeId node) noexcept {
    for (std::optional<Peer>& peer : peers_)
      if (peer.has_value() && peer->node_id == node)
        return &*peer;
    return nullptr;
  }
  [[nodiscard]] const Peer* find(raft::NodeId node) const noexcept {
    for (const std::optional<Peer>& peer : peers_)
      if (peer.has_value() && peer->node_id == node)
        return &*peer;
    return nullptr;
  }
  raft::NodeId local_{};
  RaftTransportPeerPoolLimits limits_;
  std::vector<std::optional<Peer>> peers_;
  std::size_t count_{};
};

RaftTransportPeerPool::RaftTransportPeerPool(std::unique_ptr<Impl> impl) noexcept
    : implementation_(std::move(impl)) {}
RaftTransportPeerPool::~RaftTransportPeerPool() = default;
RaftTransportPeerPool::RaftTransportPeerPool(RaftTransportPeerPool&&) noexcept = default;
RaftTransportPeerPool& RaftTransportPeerPool::operator=(RaftTransportPeerPool&&) noexcept = default;

common::Result<RaftTransportPeerPool>
RaftTransportPeerPool::create(const raft::NodeId local, const RaftTransportPeerPoolLimits limits) {
  if (local == 0U || limits.maximum_peers == 0U)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft peer pool configuration is invalid"));
  auto valid = raft::RaftTransportFrameReader::create(limits.codec);
  if (!valid.has_value())
    return common::make_unexpected(valid.error());
  try {
    std::vector<std::optional<Impl::Peer>> peers(limits.maximum_peers);
    return RaftTransportPeerPool{std::make_unique<Impl>(local, limits, std::move(peers))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Raft peer pool allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft peer pool bound exceeds container limits"));
  }
}

common::Status RaftTransportPeerPool::add_peer(RaftTransportTlsClient&& carrier) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft peer pool is empty");
  const raft::NodeId peer = carrier.peer_node_id();
  if (carrier.local_node_id() != implementation_->local_ || peer == 0U)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft peer carrier route differs from pool ownership");
  if (carrier.state() == RaftTransportTlsClientState::kFailed)
    return status(common::StatusCode::kUnavailable, "Raft peer carrier has already failed");
  if (implementation_->find(peer) != nullptr)
    return status(common::StatusCode::kAlreadyExists, "Raft peer already exists");
  if (implementation_->count_ == implementation_->peers_.size())
    return status(common::StatusCode::kResourceExhausted, "Raft peer pool is full");
  for (std::optional<Impl::Peer>& slot : implementation_->peers_) {
    if (!slot.has_value()) {
      slot.emplace(Impl::Peer{peer, std::move(carrier)});
      ++implementation_->count_;
      return common::Status::ok();
    }
  }
  return status(common::StatusCode::kCorruption, "Raft peer pool accounting is inconsistent");
}

common::Status RaftTransportPeerPool::route_result(const raft::GroupId& group,
                                                   const raft::DurableRaftResult& result,
                                                   const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft peer pool is empty");
  if (!result.status.is_ok())
    return result.status;
  if (!result.transition.has_value())
    return status(common::StatusCode::kInvalidArgument,
                  "Raft routed result does not contain a transition");
  struct Demand {
    raft::NodeId peer{};
    std::size_t frames{};
    std::size_t bytes{};
  };
  try {
    std::vector<Demand> demands;
    demands.reserve(result.transition->outbound.size());
    for (const raft::GroupOutboundMessage& outbound : result.transition->outbound) {
      if (implementation_->find(outbound.outbound.destination) == nullptr)
        return status(common::StatusCode::kNotFound, "Raft outbound peer is not connected");
      auto length =
          raft::raft_transport_encoded_length_v1({.group_id = outbound.group_id,
                                                  .source = outbound.source,
                                                  .destination = outbound.outbound.destination,
                                                  .message = outbound.outbound.message},
                                                 implementation_->limits_.codec);
      if (!length.has_value())
        return length.error();
      Demand* demand = nullptr;
      for (Demand& candidate : demands)
        if (candidate.peer == outbound.outbound.destination)
          demand = &candidate;
      if (demand == nullptr) {
        demands.push_back({outbound.outbound.destination, 0U, 0U});
        demand = &demands.back();
      }
      if (demand->frames == std::numeric_limits<std::size_t>::max() ||
          *length > std::numeric_limits<std::size_t>::max() - demand->bytes)
        return status(common::StatusCode::kResourceExhausted, "Raft peer route demand overflows");
      ++demand->frames;
      demand->bytes += *length;
    }
    for (const Demand& demand : demands) {
      const Impl::Peer* peer = implementation_->find(demand.peer);
      if (peer->carrier.state() == RaftTransportTlsClientState::kFailed)
        return status(common::StatusCode::kUnavailable, "Raft outbound peer carrier has failed");
      if (demand.frames > peer->carrier.available_frames() ||
          demand.bytes > peer->carrier.available_bytes())
        return status(common::StatusCode::kResourceExhausted,
                      "Raft peer output queue lacks aggregate capacity");
    }
    auto frames = encode_durable_raft_outbound_v1(group, implementation_->local_, result,
                                                  implementation_->limits_.codec);
    if (!frames.has_value())
      return frames.error();
    for (std::size_t index = 0U; index < frames->size(); ++index) {
      const raft::NodeId destination = result.transition->outbound[index].outbound.destination;
      common::Status queued = implementation_->find(destination)
                                  ->carrier.try_enqueue_prevalidated((*frames)[index], now);
      if (!queued.is_ok())
        return status(common::StatusCode::kCorruption,
                      "Raft peer queue changed after successful preflight");
    }
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    return status(common::StatusCode::kResourceExhausted, "Raft peer routing allocation failed");
  } catch (const std::length_error&) {
    return status(common::StatusCode::kResourceExhausted,
                  "Raft peer routing exceeds container limits");
  }
}

common::Status RaftTransportPeerPool::on_ready(const raft::NodeId peer, const bool readable,
                                               const bool writable, const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft peer pool is empty");
  Impl::Peer* found = implementation_->find(peer);
  return found == nullptr ? status(common::StatusCode::kNotFound, "Raft peer does not exist")
                          : found->carrier.on_ready(readable, writable, now);
}

common::Result<RaftTransportFailedPeer>
RaftTransportPeerPool::take_failed_peer(const raft::NodeId peer) {
  if (!implementation_)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft peer pool is empty"));
  for (std::optional<Impl::Peer>& slot : implementation_->peers_) {
    if (!slot.has_value() || slot->node_id != peer)
      continue;
    if (slot->carrier.state() != RaftTransportTlsClientState::kFailed)
      return common::make_unexpected(
          status(common::StatusCode::kUnavailable, "Raft peer carrier has not failed"));
    auto retry = slot->carrier.drain_retry_frames();
    if (!retry.has_value())
      return common::make_unexpected(retry.error());
    RaftTransportFailedPeer failed{slot->node_id, std::move(slot->carrier), std::move(*retry)};
    slot.reset();
    --implementation_->count_;
    return failed;
  }
  return common::make_unexpected(status(common::StatusCode::kNotFound, "Raft peer does not exist"));
}

std::size_t RaftTransportPeerPool::peer_count() const noexcept {
  return implementation_ ? implementation_->count_ : 0U;
}
RaftTransportTlsClient* RaftTransportPeerPool::find_peer(const raft::NodeId peer_node_id) noexcept {
  if (!implementation_)
    return nullptr;
  Impl::Peer* peer = implementation_->find(peer_node_id);
  return peer == nullptr ? nullptr : &peer->carrier;
}
} // namespace chronos::cluster
