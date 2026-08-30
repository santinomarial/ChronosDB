#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_stream.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <limits>
#include <memory>
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

[[nodiscard]] common::Status unauthenticated(const char* message) {
  return {common::StatusCode::kUnauthenticated, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool descriptors_match(const network::QueryResultBatchView& batch,
                                     const query::DistributedVectorResultSchema& result_schema) {
  const auto columns = batch.columns();
  if (columns.size() != result_schema.columns.size())
    return false;
  for (std::size_t index = 0U; index < columns.size(); ++index) {
    const auto& expected = result_schema.columns[index];
    if (columns[index].name != expected.name || columns[index].type != expected.type ||
        columns[index].nullable != expected.nullable) {
      return false;
    }
  }
  return true;
}

struct LocalResultExtent {
  std::uint32_t frame_count{};
  std::size_t encoded_bytes{};
};

struct LocalResultIdentity {
  std::uint32_t partition_id{};
  raft::NodeId node_id{};
};

[[nodiscard]] common::Result<LocalResultExtent>
local_result_extent(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
                    const query::DistributedVectorResultSchema& result_schema,
                    const LocalResultIdentity identity,
                    const std::span<const std::vector<std::byte>> encoded_result_batches,
                    const DistributedVectorGroupedAggregateShuffleResultStreamLimits limits) {
  const std::size_t frame_count = std::max<std::size_t>(1U, encoded_result_batches.size());
  const auto destination = authority.destination_node(identity.partition_id);
  if (identity.node_id == 0U || !destination.has_value() || *destination != identity.node_id ||
      !validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(limits) ||
      frame_count > limits.maximum_frames ||
      frame_count > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(invalid("local grouped shuffle result input is invalid"));
  }
  std::size_t encoded_bytes{};
  for (const auto& encoded : encoded_result_batches) {
    if (encoded.empty())
      return common::make_unexpected(invalid("local grouped shuffle result batch is empty"));
    auto decoded = network::decode_query_result_batch(encoded, limits.frame.result_batch);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
    if (!descriptors_match(*decoded, result_schema)) {
      return common::make_unexpected(
          invalid("local grouped shuffle result schema differs from authority"));
    }
    const auto header_and_payload = common::checked_add(
        distributed_vector_grouped_aggregate_shuffle_result_format::kHeaderLength, encoded.size());
    const auto frame_bytes =
        header_and_payload.has_value()
            ? common::checked_add(
                  *header_and_payload,
                  distributed_vector_grouped_aggregate_shuffle_result_format::kTrailerLength)
            : std::optional<std::size_t>{};
    if (!frame_bytes.has_value() || *frame_bytes > limits.maximum_encoded_bytes - encoded_bytes) {
      return common::make_unexpected(exhausted("local grouped shuffle result bytes exhausted"));
    }
    encoded_bytes += *frame_bytes;
  }
  if (encoded_result_batches.empty()) {
    encoded_bytes = distributed_vector_grouped_aggregate_shuffle_result_format::kHeaderLength +
                    distributed_vector_grouped_aggregate_shuffle_result_format::kTrailerLength;
    if (encoded_bytes > limits.maximum_encoded_bytes)
      return common::make_unexpected(exhausted("local grouped shuffle result bytes exhausted"));
  }
  return LocalResultExtent{.frame_count = static_cast<std::uint32_t>(frame_count),
                           .encoded_bytes = encoded_bytes};
}

} // namespace

common::Result<DistributedVectorGroupedAggregateShuffleCompleteResultStream>
create_distributed_vector_grouped_aggregate_shuffle_local_result_stream(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema, const std::uint32_t partition_id,
    const raft::NodeId local_node_id, std::vector<std::vector<std::byte>> encoded_result_batches,
    const DistributedVectorGroupedAggregateShuffleResultStreamLimits limits) {
  try {
    auto extent = local_result_extent(authority, result_schema,
                                      {.partition_id = partition_id, .node_id = local_node_id},
                                      encoded_result_batches, limits);
    if (!extent.has_value())
      return common::make_unexpected(extent.error());
    return DistributedVectorGroupedAggregateShuffleCompleteResultStream{
        .query_id = authority.query_id(),
        .source_node_id = local_node_id,
        .target_node_id = local_node_id,
        .partition_id = partition_id,
        .encoded_result_batches = std::move(encoded_result_batches),
        .frame_count = extent->frame_count,
        .encoded_bytes = extent->encoded_bytes};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("local grouped shuffle result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("local grouped shuffle result exceeds limits"));
  }
}

common::Status validate_distributed_vector_grouped_aggregate_shuffle_local_result_stream(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const DistributedVectorGroupedAggregateShuffleCompleteResultStream& stream,
    const DistributedVectorGroupedAggregateShuffleResultStreamLimits limits) {
  if (stream.query_id != authority.query_id() || stream.source_node_id == 0U ||
      stream.source_node_id != stream.target_node_id) {
    return invalid("local grouped shuffle result identity is invalid");
  }
  auto extent =
      local_result_extent(authority, result_schema,
                          {.partition_id = stream.partition_id, .node_id = stream.source_node_id},
                          stream.encoded_result_batches, limits);
  if (!extent.has_value())
    return extent.error();
  return extent->frame_count == stream.frame_count && extent->encoded_bytes == stream.encoded_bytes
             ? common::Status::ok()
             : invalid("local grouped shuffle result extent is not canonical");
}

bool validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(
    const DistributedVectorGroupedAggregateShuffleResultStreamLimits& limits) noexcept {
  return limits.maximum_frames > 0U &&
         limits.maximum_frames <=
             kMaximumDistributedVectorGroupedAggregateShuffleResultStreamFrames &&
         limits.maximum_encoded_bytes >=
             distributed_vector_grouped_aggregate_shuffle_result_format::kHeaderLength +
                 distributed_vector_grouped_aggregate_shuffle_result_format::kTrailerLength &&
         limits.maximum_encoded_bytes <=
             kMaximumDistributedVectorGroupedAggregateShuffleResultStreamBytes &&
         limits.frame.maximum_frame_length >=
             distributed_vector_grouped_aggregate_shuffle_result_format::kHeaderLength +
                 distributed_vector_grouped_aggregate_shuffle_result_format::kTrailerLength &&
         limits.frame.maximum_frame_length <=
             distributed_vector_grouped_aggregate_shuffle_result_format::kMaximumFrameLength &&
         limits.frame.result_batch.protocol.maximum_payload_size > 0U &&
         limits.frame.result_batch.protocol.maximum_payload_size <=
             network::kDefaultMaximumPayloadSize &&
         limits.frame.result_batch.maximum_rows > 0U &&
         limits.frame.result_batch.maximum_columns > 0U &&
         limits.frame.result_batch.maximum_columns <=
             query::distributed_vector_result_schema_format::kMaximumColumns &&
         limits.frame.result_batch.maximum_column_name_bytes > 0U &&
         limits.frame.result_batch.maximum_column_name_bytes <= 65'536U;
}

DistributedVectorGroupedAggregateShuffleResultStreamReceiver::
    DistributedVectorGroupedAggregateShuffleResultStreamReceiver(
        const DistributedVectorGroupedAggregateShuffleAuthority& authority,
        const query::DistributedVectorResultSchema& result_schema,
        const raft::NodeId coordinator_node_id, const ClusterNodePrincipalAuthorizer& authorizer,
        const network::PeerAuthenticationResult authenticated_peer,
        const DistributedVectorGroupedAggregateShuffleResultStreamLimits limits,
        std::unique_ptr<DistributedVectorGroupedAggregateShuffleResultReader> reader) noexcept
    : authority_(authority), result_schema_(result_schema),
      coordinator_node_id_(coordinator_node_id), authorizer_(authorizer),
      authenticated_peer_(authenticated_peer), limits_(limits), reader_(std::move(reader)) {}

common::Result<DistributedVectorGroupedAggregateShuffleResultStreamReceiver>
DistributedVectorGroupedAggregateShuffleResultStreamReceiver::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const raft::NodeId coordinator_node_id, const ClusterNodePrincipalAuthorizer& authorizer,
    const network::PeerAuthenticationResult authenticated_peer,
    const DistributedVectorGroupedAggregateShuffleResultStreamLimits limits) {
  if (coordinator_node_id == 0U ||
      !validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(limits)) {
    return common::make_unexpected(invalid("grouped shuffle result receiver limits are invalid"));
  }
  if (!authenticated_peer.authorized || authenticated_peer.principal_id == 0U) {
    return common::make_unexpected(
        unauthenticated("grouped shuffle result peer is not authenticated"));
  }
  try {
    auto reader = std::make_unique<DistributedVectorGroupedAggregateShuffleResultReader>(
        authority, result_schema, coordinator_node_id, limits.frame);
    return DistributedVectorGroupedAggregateShuffleResultStreamReceiver{
        authority,          result_schema, coordinator_node_id, authorizer,
        authenticated_peer, limits,        std::move(reader)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle result receiver allocation failed"));
  }
}

common::Status
DistributedVectorGroupedAggregateShuffleResultStreamReceiver::fail(common::Status status) {
  if (!failure_.has_value()) {
    failure_ = std::move(status);
    batches_.clear();
    source_node_id_.reset();
    partition_id_.reset();
    accepted_frames_ = 0U;
    complete_ = false;
  }
  return *failure_;
}

common::Status DistributedVectorGroupedAggregateShuffleResultStreamReceiver::accept_frame(
    DistributedVectorGroupedAggregateShuffleResultFrame frame) {
  if (!source_node_id_.has_value()) {
    auto authorized =
        authorizer_.get().authorize_node(authenticated_peer_.principal_id, frame.source_node_id);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized)
      return fail(unauthenticated("grouped shuffle result principal cannot claim source node"));
    source_node_id_ = frame.source_node_id;
    partition_id_ = frame.partition_id;
  } else if (frame.source_node_id != *source_node_id_ || frame.partition_id != *partition_id_) {
    return fail(corruption("grouped shuffle result stream changed source or partition"));
  }
  if (accepted_frames_ == limits_.maximum_frames || frame.sequence != accepted_frames_ + 1U)
    return fail(corruption("grouped shuffle result stream sequence is invalid"));
  ++accepted_frames_;
  if (!frame.encoded_result_batch.empty()) {
    try {
      batches_.push_back(std::move(frame.encoded_result_batch));
    } catch (const std::bad_alloc&) {
      return fail(exhausted("grouped shuffle result batch retention failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("grouped shuffle result batch count exceeds limits"));
    }
  }
  complete_ = frame.terminal;
  return common::Status::ok();
}

common::Result<DistributedVectorGroupedAggregateShuffleResultStreamReadStep>
DistributedVectorGroupedAggregateShuffleResultStreamReceiver::consume(
    const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  if (!reader_)
    return common::make_unexpected(fail(invalid("grouped shuffle result receiver is empty")));
  if (complete_ && !bytes.empty())
    return common::make_unexpected(fail(corruption("grouped shuffle result terminal has suffix")));
  std::size_t offset{};
  while (offset < bytes.size()) {
    auto step = reader_->consume(bytes.subspan(offset));
    if (!step.has_value())
      return common::make_unexpected(fail(step.error()));
    if (step->consumed_bytes == 0U)
      return common::make_unexpected(fail(corruption("grouped shuffle result reader stalled")));
    if (step->consumed_bytes > limits_.maximum_encoded_bytes - accepted_bytes_)
      return common::make_unexpected(fail(exhausted("grouped shuffle result byte limit exceeded")));
    accepted_bytes_ += step->consumed_bytes;
    offset += step->consumed_bytes;
    if (step->frame.has_value()) {
      common::Status accepted = accept_frame(std::move(*step->frame));
      if (!accepted.is_ok())
        return common::make_unexpected(std::move(accepted));
      if (complete_ && offset != bytes.size())
        return common::make_unexpected(
            fail(corruption("grouped shuffle result terminal has coalesced suffix")));
    }
  }
  return DistributedVectorGroupedAggregateShuffleResultStreamReadStep{.consumed_bytes = offset,
                                                                      .complete = complete_};
}

common::Status DistributedVectorGroupedAggregateShuffleResultStreamReceiver::finish_input() {
  if (failure_.has_value())
    return *failure_;
  return complete_ ? common::Status::ok()
                   : fail(corruption("grouped shuffle result ended before terminal"));
}

common::Result<DistributedVectorGroupedAggregateShuffleCompleteResultStream>
DistributedVectorGroupedAggregateShuffleResultStreamReceiver::take_complete_stream() {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  if (!complete_ || !source_node_id_.has_value() || !partition_id_.has_value() || taken_)
    return common::make_unexpected(invalid("complete grouped shuffle result is unavailable"));
  taken_ = true;
  return DistributedVectorGroupedAggregateShuffleCompleteResultStream{
      .query_id = authority_.get().query_id(),
      .source_node_id = *source_node_id_,
      .target_node_id = coordinator_node_id_,
      .partition_id = *partition_id_,
      .encoded_result_batches = std::move(batches_),
      .frame_count = static_cast<std::uint32_t>(accepted_frames_),
      .encoded_bytes = accepted_bytes_};
}

bool DistributedVectorGroupedAggregateShuffleResultStreamReceiver::complete() const noexcept {
  return complete_;
}

bool DistributedVectorGroupedAggregateShuffleResultStreamReceiver::failed() const noexcept {
  return failure_.has_value();
}

std::size_t
DistributedVectorGroupedAggregateShuffleResultStreamReceiver::buffered_bytes() const noexcept {
  return reader_ ? reader_->buffered_bytes() : 0U;
}

std::size_t
DistributedVectorGroupedAggregateShuffleResultStreamReceiver::accepted_frames() const noexcept {
  return accepted_frames_;
}

std::size_t
DistributedVectorGroupedAggregateShuffleResultStreamReceiver::accepted_bytes() const noexcept {
  return accepted_bytes_;
}

DistributedVectorGroupedAggregateShuffleResultStreamSender::
    DistributedVectorGroupedAggregateShuffleResultStreamSender(
        const std::uint32_t partition_id, const raft::NodeId source_node_id,
        const raft::NodeId coordinator_node_id,
        std::vector<DistributedVectorGroupedAggregateShuffleResultWriteCursor> writers,
        const std::size_t encoded_bytes) noexcept
    : partition_id_(partition_id), source_node_id_(source_node_id),
      coordinator_node_id_(coordinator_node_id), writers_(std::move(writers)),
      encoded_bytes_(encoded_bytes) {}

common::Result<DistributedVectorGroupedAggregateShuffleResultStreamSender>
DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema, const std::uint32_t partition_id,
    const raft::NodeId source_node_id, const raft::NodeId coordinator_node_id,
    const std::span<const std::vector<std::byte>> encoded_result_batches,
    const DistributedVectorGroupedAggregateShuffleResultStreamLimits limits) {
  const std::size_t frame_count = std::max<std::size_t>(1U, encoded_result_batches.size());
  if (!validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(limits) ||
      frame_count > limits.maximum_frames ||
      frame_count > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(invalid("grouped shuffle result sender input is invalid"));
  }
  try {
    std::vector<DistributedVectorGroupedAggregateShuffleResultWriteCursor> writers;
    writers.reserve(frame_count);
    std::size_t encoded_bytes{};
    for (std::size_t index = 0U; index < frame_count; ++index) {
      DistributedVectorGroupedAggregateShuffleResultFrame frame{
          .query_id = authority.query_id(),
          .source_node_id = source_node_id,
          .target_node_id = coordinator_node_id,
          .partition_id = partition_id,
          .partition_count = authority.partition_count(),
          .hash_version = authority.hash_version(),
          .sequence = index + 1U,
          .terminal = index + 1U == frame_count,
          .encoded_result_batch = encoded_result_batches.empty() ? std::vector<std::byte>{}
                                                                 : encoded_result_batches[index]};
      auto writer = DistributedVectorGroupedAggregateShuffleResultWriteCursor::create(
          frame, authority, result_schema, coordinator_node_id);
      if (!writer.has_value())
        return common::make_unexpected(writer.error());
      if (writer->pending_write().size() > limits.maximum_encoded_bytes - encoded_bytes)
        return common::make_unexpected(exhausted("grouped shuffle result sender bytes exhausted"));
      encoded_bytes += writer->pending_write().size();
      writers.push_back(std::move(*writer));
    }
    return DistributedVectorGroupedAggregateShuffleResultStreamSender{
        partition_id, source_node_id, coordinator_node_id, std::move(writers), encoded_bytes};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle result sender allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle result sender exceeds limits"));
  }
}

common::ByteView
DistributedVectorGroupedAggregateShuffleResultStreamSender::pending_write() const noexcept {
  return complete() ? common::ByteView{} : writers_[writer_index_].pending_write();
}

common::Status DistributedVectorGroupedAggregateShuffleResultStreamSender::consume_written(
    const std::size_t bytes) {
  if (complete())
    return bytes == 0U ? common::Status::ok()
                       : invalid("written bytes exceed grouped shuffle result stream");
  common::Status consumed = writers_[writer_index_].consume_written(bytes);
  if (!consumed.is_ok())
    return consumed;
  written_bytes_ += bytes;
  if (writers_[writer_index_].complete())
    ++writer_index_;
  return common::Status::ok();
}

bool DistributedVectorGroupedAggregateShuffleResultStreamSender::complete() const noexcept {
  return writers_.empty() || writer_index_ == writers_.size();
}

std::size_t
DistributedVectorGroupedAggregateShuffleResultStreamSender::frame_count() const noexcept {
  return writers_.size();
}

std::size_t
DistributedVectorGroupedAggregateShuffleResultStreamSender::encoded_bytes() const noexcept {
  return encoded_bytes_;
}

std::size_t
DistributedVectorGroupedAggregateShuffleResultStreamSender::written_bytes() const noexcept {
  return written_bytes_;
}

std::uint32_t
DistributedVectorGroupedAggregateShuffleResultStreamSender::partition_id() const noexcept {
  return partition_id_;
}

raft::NodeId
DistributedVectorGroupedAggregateShuffleResultStreamSender::source_node_id() const noexcept {
  return source_node_id_;
}

raft::NodeId
DistributedVectorGroupedAggregateShuffleResultStreamSender::coordinator_node_id() const noexcept {
  return coordinator_node_id_;
}

} // namespace chronos::cluster
