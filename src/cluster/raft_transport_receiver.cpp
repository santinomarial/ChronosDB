#include "chronos/cluster/raft_transport_receiver.hpp"

#include <new>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status unauthenticated(const char* message) {
  return {common::StatusCode::kUnauthenticated, message};
}

} // namespace

RaftTransportReceiver::RaftTransportReceiver(RaftTransportReceiverConfig config) noexcept
    : config_(config) {}

common::Result<RaftTransportReceiver>
RaftTransportReceiver::create(const RaftTransportReceiverConfig config) {
  if (config.local_node_id == 0U || config.authorizer == nullptr || config.runtime == nullptr)
    return common::make_unexpected(invalid("Raft transport receiver configuration is invalid"));
  auto reader = raft::RaftTransportFrameReader::create(config.codec_limits);
  if (!reader.has_value())
    return common::make_unexpected(std::move(reader).error());
  return RaftTransportReceiver{config};
}

common::Result<RaftTransportAdmission> RaftTransportReceiver::try_receive(
    const common::ByteView frame,
    const network::PeerAuthenticationResult& authenticated_peer) const {
  if (!authenticated_peer.authorized || authenticated_peer.principal_id == 0U) {
    return common::make_unexpected(
        unauthenticated("Raft transport requires an authenticated principal"));
  }
  try {
    auto envelope = raft::decode_raft_transport_envelope_v1(frame, config_.codec_limits);
    if (!envelope.has_value())
      return common::make_unexpected(std::move(envelope).error());
    return try_receive_decoded(std::move(*envelope), authenticated_peer);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft transport receive allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft transport receive exceeded container limits"));
  }
}

common::Result<RaftTransportAdmission> RaftTransportReceiver::try_receive_decoded(
    raft::RaftTransportEnvelope envelope,
    const network::PeerAuthenticationResult& authenticated_peer) const {
  if (!authenticated_peer.authorized || authenticated_peer.principal_id == 0U) {
    return common::make_unexpected(
        unauthenticated("Raft transport requires an authenticated principal"));
  }
  try {
    auto valid = raft::raft_transport_encoded_length_v1(envelope, config_.codec_limits);
    if (!valid.has_value())
      return common::make_unexpected(std::move(valid).error());
    auto authorized =
        config_.authorizer->authorize_node(authenticated_peer.principal_id, envelope.source);
    if (!authorized.has_value())
      return common::make_unexpected(std::move(authorized).error());
    if (!*authorized) {
      return common::make_unexpected(
          unauthenticated("authenticated principal cannot claim the Raft source node"));
    }
    if (envelope.destination != config_.local_node_id) {
      return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                    "Raft transport targets a different node"});
    }
    const raft::GroupId group_id = envelope.group_id;
    const raft::NodeId source_node_id = envelope.source;
    std::vector<raft::DurableRaftRequest> requests;
    requests.reserve(2U);
    requests.emplace_back(group_id,
                          raft::ReceiveOperation{source_node_id, std::move(envelope.message)});
    requests.emplace_back(group_id, raft::ObserveGroupOperation{});
    auto completion = config_.runtime->try_submit(std::move(requests));
    if (!completion.has_value())
      return common::make_unexpected(std::move(completion).error());
    return RaftTransportAdmission{group_id, source_node_id, std::move(*completion)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft transport receive allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft transport receive exceeded container limits"));
  }
}

common::Result<std::vector<std::vector<std::byte>>> encode_durable_raft_outbound_v1(
    const raft::GroupId& expected_group_id, const raft::NodeId local_node_id,
    const raft::DurableRaftResult& durable_result, const raft::RaftTransportCodecLimits limits) {
  if (expected_group_id.is_nil() || local_node_id == 0U)
    return common::make_unexpected(invalid("Raft outbound route is invalid"));
  if (!durable_result.status.is_ok())
    return common::make_unexpected(durable_result.status);
  if (!durable_result.transition.has_value() || durable_result.observation.has_value()) {
    return common::make_unexpected(
        corruption("Raft receive completion does not contain one transition"));
  }
  try {
    std::vector<std::vector<std::byte>> frames;
    frames.reserve(durable_result.transition->outbound.size());
    for (const raft::GroupOutboundMessage& outbound : durable_result.transition->outbound) {
      if (outbound.group_id != expected_group_id || outbound.source != local_node_id) {
        return common::make_unexpected(
            corruption("durable Raft outbound route differs from receiver ownership"));
      }
      auto encoded =
          raft::encode_raft_transport_envelope_v1({.group_id = outbound.group_id,
                                                   .source = outbound.source,
                                                   .destination = outbound.outbound.destination,
                                                   .message = outbound.outbound.message},
                                                  limits);
      if (!encoded.has_value())
        return common::make_unexpected(std::move(encoded).error());
      frames.push_back(std::move(*encoded));
    }
    return frames;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft outbound encoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft outbound encoding exceeded container limits"));
  }
}

} // namespace chronos::cluster
