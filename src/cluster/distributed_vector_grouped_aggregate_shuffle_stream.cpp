#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_stream.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
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

[[nodiscard]] common::Status unauthenticated(const char* message) {
  return {common::StatusCode::kUnauthenticated, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

template <typename Value>
[[nodiscard]] Value* optional_pointer(std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

[[nodiscard]] bool valid_payload_limits(
    const query::DistributedVectorGroupedAggregateExchangeDecodeLimits& limits) noexcept {
  return limits.maximum_frame_length >=
             query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.maximum_frame_length <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength &&
         limits.maximum_key_payload_bytes > 0U &&
         limits.maximum_key_payload_bytes <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes &&
         limits.maximum_groups > 0U &&
         limits.maximum_groups <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_group_keys > 0U &&
         limits.maximum_group_keys <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys &&
         limits.maximum_aggregates <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates &&
         limits.state.maximum_frame_length >=
             query::distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.state.maximum_frame_length <=
             query::distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.state.maximum_variable_extremum_bytes > 0U &&
         limits.state.maximum_variable_extremum_bytes <=
             query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes;
}

} // namespace

bool validate_distributed_vector_grouped_aggregate_shuffle_stream_limits(
    const DistributedVectorGroupedAggregateShuffleStreamLimits& limits) noexcept {
  constexpr std::size_t kMinimumBytes =
      kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize +
      query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength +
      kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize;
  return limits.maximum_frames > 0U &&
         limits.maximum_frames <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_frames <= limits.payload.maximum_groups &&
         limits.maximum_encoded_bytes >= kMinimumBytes &&
         limits.maximum_encoded_bytes <=
             kMaximumDistributedVectorGroupedAggregateShuffleStreamBytes &&
         valid_payload_limits(limits.payload);
}

namespace {

[[nodiscard]] bool same_edge(const DistributedVectorGroupedAggregateShuffleEdge& left,
                             const DistributedVectorGroupedAggregateShuffleEdge& right) noexcept {
  return left.tablet_id == right.tablet_id && left.partition_id == right.partition_id &&
         left.source_node_id == right.source_node_id &&
         left.target_node_id == right.target_node_id && left.hash_version == right.hash_version;
}

[[nodiscard]] bool
valid_position(const query::DistributedVectorGroupedAggregateExchangePosition& position,
               const std::size_t ordinal, const std::uint32_t maximum_frames,
               const std::optional<std::uint32_t> expected_group_count) noexcept {
  const bool empty = position.empty && ordinal == 0U && position.group_count == 0U &&
                     position.group_ordinal == 0U && position.sequence == 1U && position.terminal;
  const bool group = !position.empty && position.group_count > 0U &&
                     position.group_count <= maximum_frames && position.group_ordinal == ordinal &&
                     position.sequence == ordinal + 1U &&
                     position.terminal == (ordinal + 1U == position.group_count);
  return (empty || group) &&
         (!expected_group_count.has_value() || position.group_count == *expected_group_count);
}

} // namespace

DistributedVectorGroupedAggregateShuffleStreamReceiver::
    DistributedVectorGroupedAggregateShuffleStreamReceiver(
        const DistributedVectorGroupedAggregateShuffleAuthority& authority,
        const raft::NodeId local_node_id, const ClusterNodePrincipalAuthorizer& authorizer,
        const network::PeerAuthenticationResult authenticated_peer,
        const DistributedVectorGroupedAggregateShuffleStreamLimits limits,
        std::unique_ptr<DistributedVectorGroupedAggregateShuffleFrameV1Reader> reader) noexcept
    : authority_(authority), local_node_id_(local_node_id), authorizer_(authorizer),
      authenticated_peer_(authenticated_peer), limits_(limits), reader_(std::move(reader)) {}

common::Result<DistributedVectorGroupedAggregateShuffleStreamReceiver>
DistributedVectorGroupedAggregateShuffleStreamReceiver::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const raft::NodeId local_node_id, const ClusterNodePrincipalAuthorizer& authorizer,
    const network::PeerAuthenticationResult authenticated_peer,
    query::QueryResourceContext resources,
    const DistributedVectorGroupedAggregateShuffleStreamLimits limits) {
  if (local_node_id == 0U ||
      !validate_distributed_vector_grouped_aggregate_shuffle_stream_limits(limits)) {
    return common::make_unexpected(
        invalid("grouped shuffle stream receiver configuration is invalid"));
  }
  if (!authenticated_peer.authorized || authenticated_peer.principal_id == 0U) {
    return common::make_unexpected(
        unauthenticated("grouped shuffle stream peer is not authenticated"));
  }
  try {
    auto reader = std::make_unique<DistributedVectorGroupedAggregateShuffleFrameV1Reader>(
        authority, std::move(resources),
        std::min(limits.maximum_encoded_bytes,
                 kMaximumDistributedVectorGroupedAggregateShuffleFrameV1Size),
        limits.payload);
    return DistributedVectorGroupedAggregateShuffleStreamReceiver{
        authority, local_node_id, authorizer, authenticated_peer, limits, std::move(reader)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle stream receiver allocation failed"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleStreamReceiver::fail(common::Status status) {
  if (!failure_.has_value()) {
    failure_ = std::move(status);
    messages_.clear();
    edge_.reset();
    expected_group_count_.reset();
    complete_ = false;
  }
  return *failure_;
}

common::Status DistributedVectorGroupedAggregateShuffleStreamReceiver::accept_frame(
    DistributedVectorGroupedAggregateShuffleFrameV1 frame) {
  if (frame.edge.target_node_id != local_node_id_)
    return fail(corruption("grouped shuffle stream target differs from local node"));
  if (!edge_.has_value()) {
    auto authorized = authorizer_.get().authorize_node(authenticated_peer_.principal_id,
                                                       frame.edge.source_node_id);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized) {
      return fail(
          unauthenticated("authenticated grouped shuffle principal cannot claim source node"));
    }
    edge_ = frame.edge;
  } else if (!same_edge(*edge_, frame.edge)) {
    return fail(corruption("grouped shuffle stream changed immutable edge"));
  }
  if (messages_.size() == limits_.maximum_frames ||
      !valid_position(frame.payload.position(), messages_.size(), limits_.maximum_frames,
                      expected_group_count_)) {
    return fail(corruption("grouped shuffle stream position is invalid"));
  }
  if (!expected_group_count_.has_value())
    expected_group_count_ = frame.payload.position().group_count;
  const bool terminal = frame.payload.position().terminal;
  try {
    messages_.push_back(std::move(frame.payload));
  } catch (const std::bad_alloc&) {
    return fail(exhausted("grouped shuffle stream message allocation failed"));
  } catch (const std::length_error&) {
    return fail(exhausted("grouped shuffle stream message count exceeds container limits"));
  }
  complete_ = terminal;
  return common::Status::ok();
}

common::Result<DistributedVectorGroupedAggregateShuffleStreamReadStep>
DistributedVectorGroupedAggregateShuffleStreamReceiver::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  if (!reader_)
    return common::make_unexpected(fail(invalid("grouped shuffle stream receiver is empty")));
  if (complete_ && !bytes.empty()) {
    return common::make_unexpected(
        fail(corruption("grouped shuffle stream terminal has a coalesced suffix")));
  }
  std::size_t offset{};
  while (offset < bytes.size()) {
    auto step = reader_->consume(bytes.subspan(offset));
    if (!step.has_value())
      return common::make_unexpected(fail(step.error()));
    if (step->consumed_bytes == 0U) {
      return common::make_unexpected(
          fail(corruption("grouped shuffle stream reader made no progress")));
    }
    if (step->consumed_bytes > limits_.maximum_encoded_bytes - accepted_bytes_) {
      return common::make_unexpected(fail(exhausted("grouped shuffle stream byte limit exceeded")));
    }
    accepted_bytes_ += step->consumed_bytes;
    offset += step->consumed_bytes;
    auto* frame = optional_pointer(step->frame);
    if (frame != nullptr) {
      const common::Status accepted = accept_frame(std::move(*frame));
      if (!accepted.is_ok())
        return common::make_unexpected(accepted);
      if (complete_ && offset != bytes.size()) {
        return common::make_unexpected(
            fail(corruption("grouped shuffle stream terminal has a coalesced suffix")));
      }
    }
  }
  return DistributedVectorGroupedAggregateShuffleStreamReadStep{.consumed_bytes = offset,
                                                                .complete = complete_};
}

common::Status DistributedVectorGroupedAggregateShuffleStreamReceiver::finish_input() {
  if (failure_.has_value())
    return *failure_;
  if (!complete_)
    return fail(corruption("grouped shuffle stream ended before terminal"));
  return common::Status::ok();
}

common::Result<DistributedVectorGroupedAggregateShuffleCompleteStream>
DistributedVectorGroupedAggregateShuffleStreamReceiver::take_complete_stream() {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  if (!complete_ || !edge_.has_value() || taken_)
    return common::make_unexpected(invalid("complete grouped shuffle stream is unavailable"));
  taken_ = true;
  return DistributedVectorGroupedAggregateShuffleCompleteStream{
      .edge = *edge_, .messages = std::move(messages_), .encoded_bytes = accepted_bytes_};
}

bool DistributedVectorGroupedAggregateShuffleStreamReceiver::complete() const noexcept {
  return complete_;
}

bool DistributedVectorGroupedAggregateShuffleStreamReceiver::failed() const noexcept {
  return failure_.has_value();
}

std::size_t
DistributedVectorGroupedAggregateShuffleStreamReceiver::buffered_bytes() const noexcept {
  return reader_ ? reader_->buffered_bytes() : 0U;
}

std::size_t
DistributedVectorGroupedAggregateShuffleStreamReceiver::accepted_frames() const noexcept {
  return messages_.size();
}

std::size_t
DistributedVectorGroupedAggregateShuffleStreamReceiver::accepted_bytes() const noexcept {
  return accepted_bytes_;
}

DistributedVectorGroupedAggregateShuffleStreamSender::
    DistributedVectorGroupedAggregateShuffleStreamSender(
        DistributedVectorGroupedAggregateShuffleEdge edge,
        std::vector<DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor> writers,
        const std::size_t encoded_bytes) noexcept
    : edge_(edge), writers_(std::move(writers)), encoded_bytes_(encoded_bytes) {}

common::Result<DistributedVectorGroupedAggregateShuffleStreamSender>
DistributedVectorGroupedAggregateShuffleStreamSender::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const DistributedVectorGroupedAggregateShuffleEdge edge,
    const std::span<const query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages,
    const query::QueryResourceContext& resources,
    const DistributedVectorGroupedAggregateShuffleStreamLimits limits) {
  if (!validate_distributed_vector_grouped_aggregate_shuffle_stream_limits(limits) ||
      messages.empty() || messages.size() > limits.maximum_frames ||
      edge.source_node_id == edge.target_node_id) {
    return common::make_unexpected(invalid("grouped shuffle stream sender input is invalid"));
  }
  const common::Status edge_status = authority.validate_edge(edge);
  if (!edge_status.is_ok())
    return common::make_unexpected(edge_status);
  try {
    std::vector<DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor> writers;
    writers.reserve(messages.size());
    std::optional<std::uint32_t> expected_group_count;
    std::size_t encoded_bytes{};
    for (std::size_t index = 0U; index < messages.size(); ++index) {
      auto decoded = query::decode_distributed_vector_grouped_aggregate_exchange_message_exact(
          messages[index].bytes(), authority.key_definitions(), authority.aggregate_definitions(),
          resources, limits.payload);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      if (!valid_position(decoded->position(), index, limits.maximum_frames,
                          expected_group_count)) {
        return common::make_unexpected(
            invalid("grouped shuffle sender input stream position is invalid"));
      }
      if (!expected_group_count.has_value())
        expected_group_count = decoded->position().group_count;
      if (decoded->position().terminal != (index + 1U == messages.size())) {
        return common::make_unexpected(
            invalid("grouped shuffle sender input stream is not exactly terminal"));
      }
      DistributedVectorGroupedAggregateShuffleFrameV1 frame{.query_id = authority.query_id(),
                                                            .edge = edge,
                                                            .partition_count =
                                                                authority.partition_count(),
                                                            .payload = std::move(*decoded)};
      auto writer =
          DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor::create(frame, authority);
      if (!writer.has_value())
        return common::make_unexpected(writer.error());
      if (writer->pending_write().size() > limits.maximum_encoded_bytes - encoded_bytes) {
        return common::make_unexpected(
            exhausted("grouped shuffle stream sender byte limit exceeded"));
      }
      encoded_bytes += writer->pending_write().size();
      writers.push_back(std::move(*writer));
    }
    return DistributedVectorGroupedAggregateShuffleStreamSender{edge, std::move(writers),
                                                                encoded_bytes};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle stream sender allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle stream sender exceeds limits"));
  }
}

common::ByteView
DistributedVectorGroupedAggregateShuffleStreamSender::pending_write() const noexcept {
  return complete() ? common::ByteView{} : writers_[writer_index_].pending_write();
}

common::Status
DistributedVectorGroupedAggregateShuffleStreamSender::consume_written(const std::size_t bytes) {
  if (complete())
    return bytes == 0U ? common::Status::ok()
                       : invalid("written bytes exceed grouped shuffle stream");
  common::Status consumed = writers_[writer_index_].consume_written(bytes);
  if (!consumed.is_ok())
    return consumed;
  written_bytes_ += bytes;
  if (writers_[writer_index_].complete())
    ++writer_index_;
  return common::Status::ok();
}

bool DistributedVectorGroupedAggregateShuffleStreamSender::complete() const noexcept {
  return writer_index_ == writers_.size();
}

std::size_t DistributedVectorGroupedAggregateShuffleStreamSender::frame_count() const noexcept {
  return writers_.size();
}

std::size_t DistributedVectorGroupedAggregateShuffleStreamSender::encoded_bytes() const noexcept {
  return encoded_bytes_;
}

std::size_t DistributedVectorGroupedAggregateShuffleStreamSender::written_bytes() const noexcept {
  return written_bytes_;
}

const DistributedVectorGroupedAggregateShuffleEdge&
DistributedVectorGroupedAggregateShuffleStreamSender::edge() const noexcept {
  return edge_;
}

} // namespace chronos::cluster
