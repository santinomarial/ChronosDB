#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tls.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <utility>

namespace chronos::cluster {
namespace {

inline constexpr std::size_t kTlsScratchSize = std::size_t{16U} * 1024U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status unauthenticated(const char* message) {
  return {common::StatusCode::kUnauthenticated, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

template <typename Value>
[[nodiscard]] Value* optional_pointer(std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

template <typename TimePoint>
[[nodiscard]] bool
valid_limits(const DistributedVectorGroupedAggregateShuffleResultTlsLimits& limits) noexcept {
  const auto maximum =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  return limits.handshake_timeout.count() > 0 && limits.handshake_timeout <= maximum &&
         limits.exchange_timeout.count() > 0 && limits.exchange_timeout <= maximum &&
         validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(limits.stream);
}

template <typename TimePoint>
[[nodiscard]] TimePoint deadline_after(const TimePoint now,
                                       const std::chrono::milliseconds timeout) noexcept {
  const auto duration = std::chrono::duration_cast<typename TimePoint::duration>(timeout);
  return now > TimePoint::max() - duration ? TimePoint::max() : now + duration;
}

} // namespace

class DistributedVectorGroupedAggregateShuffleResultTlsClient::Impl {
public:
  Impl(network::TlsSocket socket, DistributedVectorGroupedAggregateShuffleResultStreamSender sender,
       const DistributedVectorGroupedAggregateShuffleAuthority& authority,
       const query::DistributedVectorResultSchema& result_schema,
       const raft::NodeId coordinator_node_id,
       const DistributedVectorGroupedAggregateShuffleResultTlsClientConfig config,
       const TimePoint now)
      : socket_(std::move(socket)), sender_(std::move(sender)),
        coordinator_node_id_(coordinator_node_id), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)),
        ack_reader_(authority, result_schema, coordinator_node_id) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedVectorGroupedAggregateShuffleResultTlsState::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateShuffleResultTlsState::kFailed;
      interest_ = {};
    }
    return failure_;
  }

  [[nodiscard]] common::Status authenticate_server(const TimePoint now) {
    auto fingerprint = socket_.peer_certificate_sha256();
    if (!fingerprint.has_value())
      return fail(fingerprint.error());
    const network::NetworkSecurityConfig security{.mode =
                                                      network::TransportSecurityMode::kTlsRequired,
                                                  .authenticator = config_.authenticator};
    auto peer = network::authenticate_peer(security, {.ipv4_address = config_.peer_ipv4_address,
                                                      .transport_authenticated = true,
                                                      .peer_certificate_sha256 = *fingerprint});
    if (!peer.has_value())
      return fail(peer.error());
    if (!peer->authorized || peer->principal_id == 0U)
      return fail(unauthenticated("grouped shuffle result TLS server is not authenticated"));
    auto authorized =
        config_.node_authorizer->authorize_node(peer->principal_id, coordinator_node_id_);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized)
      return fail(
          unauthenticated("grouped shuffle result TLS server cannot claim coordinator node"));
    state_ = DistributedVectorGroupedAggregateShuffleResultTlsState::kWritingStream;
    interest_ = {.want_write = true};
    deadline_ = deadline_after(now, config_.limits.exchange_timeout);
    return common::Status::ok();
  }

  [[nodiscard]] common::Status handshake(const bool readable, const bool writable,
                                         const TimePoint now) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.handshake();
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped shuffle result TLS handshake closed"));
    if (progress->state == network::TlsIoState::kWantRead)
      interest_ = {.want_read = true};
    else if (progress->state == network::TlsIoState::kWantWrite)
      interest_ = {.want_write = true};
    else
      return authenticate_server(now);
    return common::Status::ok();
  }

  [[nodiscard]] common::Status write_stream(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.write(sender_.pending_write());
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped shuffle result TLS stream closed while writing"));
    if (progress->state == network::TlsIoState::kWantRead)
      interest_ = {.want_read = true};
    else if (progress->state == network::TlsIoState::kWantWrite)
      interest_ = {.want_write = true};
    else {
      if (progress->bytes_transferred == 0U)
        return fail(unavailable("grouped shuffle result TLS stream write made no progress"));
      common::Status consumed = sender_.consume_written(progress->bytes_transferred);
      if (!consumed.is_ok())
        return fail(std::move(consumed));
      if (sender_.complete()) {
        state_ = DistributedVectorGroupedAggregateShuffleResultTlsState::kReadingAck;
        interest_ = {.want_read = true};
      } else {
        interest_ = {.want_write = true};
      }
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status read_ack(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    auto progress = socket_.read(scratch_);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped shuffle result TLS closed before acknowledgment"));
    if (progress->state == network::TlsIoState::kWantRead)
      interest_ = {.want_read = true};
    else if (progress->state == network::TlsIoState::kWantWrite)
      interest_ = {.want_write = true};
    else {
      if (progress->bytes_transferred == 0U)
        return fail(unavailable("grouped shuffle result TLS acknowledgment read made no progress"));
      const common::ByteView received =
          common::ByteView{scratch_}.first(progress->bytes_transferred);
      auto step = ack_reader_.consume(received);
      if (!step.has_value())
        return fail(step.error());
      if (step->consumed_bytes != received.size())
        return fail(corruption("grouped shuffle result TLS acknowledgment has a suffix"));
      const auto* ack = optional_pointer(step->ack);
      if (ack != nullptr) {
        if (ack->partition_id != sender_.partition_id() ||
            ack->accepted_frames != sender_.frame_count() ||
            ack->accepted_bytes != sender_.encoded_bytes()) {
          return fail(corruption("grouped shuffle result TLS acknowledgment extent differs"));
        }
        state_ = DistributedVectorGroupedAggregateShuffleResultTlsState::kComplete;
        interest_ = {};
      } else {
        interest_ = {.want_read = true};
      }
    }
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedVectorGroupedAggregateShuffleResultStreamSender sender_;
  raft::NodeId coordinator_node_id_{};
  DistributedVectorGroupedAggregateShuffleResultTlsClientConfig config_;
  TimePoint deadline_;
  DistributedVectorGroupedAggregateShuffleResultTlsState state_{
      DistributedVectorGroupedAggregateShuffleResultTlsState::kHandshaking};
  DistributedVectorGroupedAggregateShuffleResultTlsInterest interest_{.want_write = true};
  DistributedVectorGroupedAggregateShuffleResultAckV1Reader ack_reader_;
  std::array<std::byte, kTlsScratchSize> scratch_{};
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle result TLS client has not failed"};
};

class DistributedVectorGroupedAggregateShuffleResultTlsServer::Impl {
public:
  Impl(network::TlsSocket socket,
       const DistributedVectorGroupedAggregateShuffleResultTlsServerConfig config,
       const TimePoint now)
      : socket_(std::move(socket)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedVectorGroupedAggregateShuffleResultTlsState::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateShuffleResultTlsState::kFailed;
      interest_ = {};
      receiver_.reset();
      stream_.reset();
      ack_writer_.reset();
    }
    return failure_;
  }

  [[nodiscard]] common::Status authenticate_client(const TimePoint now) {
    auto fingerprint = socket_.peer_certificate_sha256();
    if (!fingerprint.has_value())
      return fail(fingerprint.error());
    const network::NetworkSecurityConfig security{.mode =
                                                      network::TransportSecurityMode::kTlsRequired,
                                                  .authenticator = config_.authenticator};
    auto peer = network::authenticate_peer(security, {.ipv4_address = config_.peer_ipv4_address,
                                                      .transport_authenticated = true,
                                                      .peer_certificate_sha256 = *fingerprint});
    if (!peer.has_value())
      return fail(peer.error());
    auto receiver = DistributedVectorGroupedAggregateShuffleResultStreamReceiver::create(
        *config_.authority, *config_.result_schema, config_.coordinator_node_id,
        *config_.node_authorizer, *peer, config_.limits.stream);
    if (!receiver.has_value())
      return fail(receiver.error());
    receiver_.emplace(std::move(*receiver));
    state_ = DistributedVectorGroupedAggregateShuffleResultTlsState::kReadingStream;
    interest_ = {.want_read = true};
    deadline_ = deadline_after(now, config_.limits.exchange_timeout);
    return common::Status::ok();
  }

  [[nodiscard]] common::Status handshake(const bool readable, const bool writable,
                                         const TimePoint now) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.handshake();
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped shuffle result TLS handshake closed"));
    if (progress->state == network::TlsIoState::kWantRead)
      interest_ = {.want_read = true};
    else if (progress->state == network::TlsIoState::kWantWrite)
      interest_ = {.want_write = true};
    else
      return authenticate_client(now);
    return common::Status::ok();
  }

  [[nodiscard]] common::Status read_stream(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    auto progress = socket_.read(scratch_);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped shuffle result TLS closed before stream terminal"));
    if (progress->state == network::TlsIoState::kWantRead)
      interest_ = {.want_read = true};
    else if (progress->state == network::TlsIoState::kWantWrite)
      interest_ = {.want_write = true};
    else {
      if (progress->bytes_transferred == 0U)
        return fail(unavailable("grouped shuffle result TLS stream read made no progress"));
      const common::ByteView received =
          common::ByteView{scratch_}.first(progress->bytes_transferred);
      auto step = receiver_->consume(received);
      if (!step.has_value())
        return fail(step.error());
      if (step->consumed_bytes != received.size())
        return fail(corruption("grouped shuffle result TLS terminal has a suffix"));
      if (step->complete) {
        auto complete = receiver_->take_complete_stream();
        if (!complete.has_value())
          return fail(complete.error());
        DistributedVectorGroupedAggregateShuffleResultAckV1 ack{
            .query_id = config_.authority->query_id(),
            .source_node_id = complete->source_node_id,
            .target_node_id = complete->target_node_id,
            .partition_id = complete->partition_id,
            .partition_count = config_.authority->partition_count(),
            .hash_version = config_.authority->hash_version(),
            .accepted_frames = complete->frame_count,
            .accepted_bytes = complete->encoded_bytes};
        auto writer = DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor::create(
            ack, *config_.authority, *config_.result_schema, config_.coordinator_node_id);
        if (!writer.has_value())
          return fail(writer.error());
        stream_.emplace(std::move(*complete));
        ack_writer_.emplace(std::move(*writer));
        receiver_.reset();
        state_ = DistributedVectorGroupedAggregateShuffleResultTlsState::kWritingAck;
        interest_ = {.want_write = true};
      } else {
        interest_ = {.want_read = true};
      }
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status write_ack(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto* writer = optional_pointer(ack_writer_);
    if (writer == nullptr)
      return fail(corruption("grouped shuffle result acknowledgment writer is unavailable"));
    auto progress = socket_.write(writer->pending_write());
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped shuffle result TLS closed while writing acknowledgment"));
    if (progress->state == network::TlsIoState::kWantRead)
      interest_ = {.want_read = true};
    else if (progress->state == network::TlsIoState::kWantWrite)
      interest_ = {.want_write = true};
    else {
      if (progress->bytes_transferred == 0U)
        return fail(
            unavailable("grouped shuffle result TLS acknowledgment write made no progress"));
      common::Status consumed = writer->consume_written(progress->bytes_transferred);
      if (!consumed.is_ok())
        return fail(std::move(consumed));
      if (writer->complete()) {
        state_ = DistributedVectorGroupedAggregateShuffleResultTlsState::kComplete;
        interest_ = {};
      } else {
        interest_ = {.want_write = true};
      }
    }
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedVectorGroupedAggregateShuffleResultTlsServerConfig config_;
  TimePoint deadline_;
  DistributedVectorGroupedAggregateShuffleResultTlsState state_{
      DistributedVectorGroupedAggregateShuffleResultTlsState::kHandshaking};
  DistributedVectorGroupedAggregateShuffleResultTlsInterest interest_{.want_read = true};
  std::optional<DistributedVectorGroupedAggregateShuffleResultStreamReceiver> receiver_;
  std::optional<DistributedVectorGroupedAggregateShuffleCompleteResultStream> stream_;
  std::optional<DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor> ack_writer_;
  std::array<std::byte, kTlsScratchSize> scratch_{};
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle result TLS server has not failed"};
};

DistributedVectorGroupedAggregateShuffleResultTlsClient::
    DistributedVectorGroupedAggregateShuffleResultTlsClient(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

DistributedVectorGroupedAggregateShuffleResultTlsClient::
    ~DistributedVectorGroupedAggregateShuffleResultTlsClient() = default;

DistributedVectorGroupedAggregateShuffleResultTlsClient::
    DistributedVectorGroupedAggregateShuffleResultTlsClient(
        DistributedVectorGroupedAggregateShuffleResultTlsClient&&) noexcept = default;

DistributedVectorGroupedAggregateShuffleResultTlsClient&
DistributedVectorGroupedAggregateShuffleResultTlsClient::operator=(
    DistributedVectorGroupedAggregateShuffleResultTlsClient&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleResultTlsClient>
DistributedVectorGroupedAggregateShuffleResultTlsClient::create(
    network::TlsSocket socket, DistributedVectorGroupedAggregateShuffleResultStreamSender sender,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const raft::NodeId coordinator_node_id,
    const DistributedVectorGroupedAggregateShuffleResultTlsClientConfig config,
    const TimePoint now) {
  const auto source_node = authority.destination_node(sender.partition_id());
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      !valid_limits<TimePoint>(config.limits) || coordinator_node_id == 0U ||
      !source_node.has_value() || *source_node != sender.source_node_id() ||
      *source_node == coordinator_node_id || sender.coordinator_node_id() != coordinator_node_id ||
      sender.complete() || sender.frame_count() > config.limits.stream.maximum_frames ||
      sender.encoded_bytes() > config.limits.stream.maximum_encoded_bytes ||
      !query::validate_distributed_vector_result_schema_value(result_schema).is_ok()) {
    return common::make_unexpected(
        invalid("grouped shuffle result TLS client configuration is invalid"));
  }
  try {
    return DistributedVectorGroupedAggregateShuffleResultTlsClient{
        std::make_unique<Impl>(std::move(socket), std::move(sender), authority, result_schema,
                               coordinator_node_id, config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle result TLS client allocation failed"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleResultTlsClient::on_ready(
    const bool readable, const bool writable, const TimePoint now) {
  if (!implementation_)
    return invalid("grouped shuffle result TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTlsState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTlsState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(unavailable(
        impl.state_ == DistributedVectorGroupedAggregateShuffleResultTlsState::kHandshaking
            ? "grouped shuffle result TLS handshake timed out"
            : "grouped shuffle result TLS exchange timed out"));
  }
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTlsState::kHandshaking)
    return impl.handshake(readable, writable, now);
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTlsState::kWritingStream)
    return impl.write_stream(readable, writable);
  return impl.read_ack(readable, writable);
}

DistributedVectorGroupedAggregateShuffleResultTlsState
DistributedVectorGroupedAggregateShuffleResultTlsClient::state() const noexcept {
  return implementation_ ? implementation_->state_
                         : DistributedVectorGroupedAggregateShuffleResultTlsState::kFailed;
}

DistributedVectorGroupedAggregateShuffleResultTlsInterest
DistributedVectorGroupedAggregateShuffleResultTlsClient::interest() const noexcept {
  return implementation_ ? implementation_->interest_
                         : DistributedVectorGroupedAggregateShuffleResultTlsInterest{};
}

DistributedVectorGroupedAggregateShuffleResultTlsClient::TimePoint
DistributedVectorGroupedAggregateShuffleResultTlsClient::deadline() const noexcept {
  return implementation_ ? implementation_->deadline_ : TimePoint::min();
}

const common::Status&
DistributedVectorGroupedAggregateShuffleResultTlsClient::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle result TLS client is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

DistributedVectorGroupedAggregateShuffleResultTlsServer::
    DistributedVectorGroupedAggregateShuffleResultTlsServer(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

DistributedVectorGroupedAggregateShuffleResultTlsServer::
    ~DistributedVectorGroupedAggregateShuffleResultTlsServer() = default;

DistributedVectorGroupedAggregateShuffleResultTlsServer::
    DistributedVectorGroupedAggregateShuffleResultTlsServer(
        DistributedVectorGroupedAggregateShuffleResultTlsServer&&) noexcept = default;

DistributedVectorGroupedAggregateShuffleResultTlsServer&
DistributedVectorGroupedAggregateShuffleResultTlsServer::operator=(
    DistributedVectorGroupedAggregateShuffleResultTlsServer&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleResultTlsServer>
DistributedVectorGroupedAggregateShuffleResultTlsServer::create(
    network::TlsSocket socket,
    const DistributedVectorGroupedAggregateShuffleResultTlsServerConfig config,
    const TimePoint now) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      config.authority == nullptr || config.result_schema == nullptr ||
      config.coordinator_node_id == 0U || !valid_limits<TimePoint>(config.limits) ||
      !query::validate_distributed_vector_result_schema_value(*config.result_schema).is_ok()) {
    return common::make_unexpected(
        invalid("grouped shuffle result TLS server configuration is invalid"));
  }
  try {
    return DistributedVectorGroupedAggregateShuffleResultTlsServer{
        std::make_unique<Impl>(std::move(socket), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle result TLS server allocation failed"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleResultTlsServer::on_ready(
    const bool readable, const bool writable, const TimePoint now) {
  if (!implementation_)
    return invalid("grouped shuffle result TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTlsState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTlsState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(unavailable(
        impl.state_ == DistributedVectorGroupedAggregateShuffleResultTlsState::kHandshaking
            ? "grouped shuffle result TLS handshake timed out"
            : "grouped shuffle result TLS exchange timed out"));
  }
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTlsState::kHandshaking)
    return impl.handshake(readable, writable, now);
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleResultTlsState::kReadingStream)
    return impl.read_stream(readable, writable);
  return impl.write_ack(readable, writable);
}

DistributedVectorGroupedAggregateShuffleResultTlsState
DistributedVectorGroupedAggregateShuffleResultTlsServer::state() const noexcept {
  return implementation_ ? implementation_->state_
                         : DistributedVectorGroupedAggregateShuffleResultTlsState::kFailed;
}

DistributedVectorGroupedAggregateShuffleResultTlsInterest
DistributedVectorGroupedAggregateShuffleResultTlsServer::interest() const noexcept {
  return implementation_ ? implementation_->interest_
                         : DistributedVectorGroupedAggregateShuffleResultTlsInterest{};
}

DistributedVectorGroupedAggregateShuffleResultTlsServer::TimePoint
DistributedVectorGroupedAggregateShuffleResultTlsServer::deadline() const noexcept {
  return implementation_ ? implementation_->deadline_ : TimePoint::min();
}

common::Result<DistributedVectorGroupedAggregateShuffleCompleteResultStream>
DistributedVectorGroupedAggregateShuffleResultTlsServer::take_complete_stream() {
  if (!implementation_ || implementation_->state_ !=
                              DistributedVectorGroupedAggregateShuffleResultTlsState::kComplete) {
    return common::make_unexpected(
        invalid("grouped shuffle result TLS complete stream is unavailable"));
  }
  auto* complete_stream = optional_pointer(implementation_->stream_);
  if (complete_stream == nullptr) {
    return common::make_unexpected(
        invalid("grouped shuffle result TLS complete stream is unavailable"));
  }
  auto stream = std::move(*complete_stream);
  implementation_->stream_.reset();
  return stream;
}

const common::Status&
DistributedVectorGroupedAggregateShuffleResultTlsServer::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle result TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
