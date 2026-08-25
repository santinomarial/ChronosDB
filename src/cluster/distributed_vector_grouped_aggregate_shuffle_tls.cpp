#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tls.hpp"

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

template <typename TimePoint>
[[nodiscard]] bool
valid_limits(const DistributedVectorGroupedAggregateShuffleTlsLimits& limits) noexcept {
  const auto maximum =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  return limits.handshake_timeout.count() > 0 && limits.handshake_timeout <= maximum &&
         limits.exchange_timeout.count() > 0 && limits.exchange_timeout <= maximum &&
         validate_distributed_vector_grouped_aggregate_shuffle_stream_limits(limits.stream);
}

template <typename TimePoint>
[[nodiscard]] TimePoint deadline_after(const TimePoint now,
                                       const std::chrono::milliseconds timeout) noexcept {
  const auto duration = std::chrono::duration_cast<typename TimePoint::duration>(timeout);
  return now > TimePoint::max() - duration ? TimePoint::max() : now + duration;
}

} // namespace

class DistributedVectorGroupedAggregateShuffleTlsClient::Impl {
public:
  Impl(network::TlsSocket socket, DistributedVectorGroupedAggregateShuffleStreamSender sender,
       const DistributedVectorGroupedAggregateShuffleAuthority& authority,
       const DistributedVectorGroupedAggregateShuffleTlsClientConfig config, const TimePoint now)
      : socket_(std::move(socket)), sender_(std::move(sender)), authority_(authority),
        config_(config), deadline_(deadline_after(now, config.limits.handshake_timeout)),
        ack_reader_(authority) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedVectorGroupedAggregateShuffleTlsState::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateShuffleTlsState::kFailed;
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
      return fail(unauthenticated("grouped shuffle TLS server is not authenticated"));
    auto authorized =
        config_.node_authorizer->authorize_node(peer->principal_id, sender_.edge().target_node_id);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized)
      return fail(unauthenticated("grouped shuffle TLS server cannot claim destination node"));
    state_ = DistributedVectorGroupedAggregateShuffleTlsState::kWritingStream;
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
      return fail(unavailable("grouped shuffle TLS handshake closed"));
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
      return fail(unavailable("grouped shuffle TLS stream closed while writing"));
    if (progress->state == network::TlsIoState::kWantRead)
      interest_ = {.want_read = true};
    else if (progress->state == network::TlsIoState::kWantWrite)
      interest_ = {.want_write = true};
    else {
      if (progress->bytes_transferred == 0U)
        return fail(unavailable("grouped shuffle TLS stream write made no progress"));
      common::Status consumed = sender_.consume_written(progress->bytes_transferred);
      if (!consumed.is_ok())
        return fail(std::move(consumed));
      if (sender_.complete()) {
        state_ = DistributedVectorGroupedAggregateShuffleTlsState::kReadingAck;
        interest_ = {.want_read = true};
      } else {
        interest_ = {.want_write = true};
      }
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status read_ack(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.read(scratch_);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped shuffle TLS closed before acknowledgment"));
    if (progress->state == network::TlsIoState::kWantRead)
      interest_ = {.want_read = true};
    else if (progress->state == network::TlsIoState::kWantWrite)
      interest_ = {.want_write = true};
    else {
      if (progress->bytes_transferred == 0U)
        return fail(unavailable("grouped shuffle TLS acknowledgment read made no progress"));
      const common::ByteView received =
          common::ByteView{scratch_}.first(progress->bytes_transferred);
      auto step = ack_reader_.consume(received);
      if (!step.has_value())
        return fail(step.error());
      if (step->consumed_bytes != received.size())
        return fail(corruption("grouped shuffle TLS acknowledgment has a suffix"));
      if (step->ack.has_value()) {
        const auto& ack = *step->ack;
        const auto& edge = sender_.edge();
        if (ack.edge.tablet_id != edge.tablet_id || ack.edge.partition_id != edge.partition_id ||
            ack.edge.source_node_id != edge.source_node_id ||
            ack.edge.target_node_id != edge.target_node_id ||
            ack.edge.hash_version != edge.hash_version ||
            ack.accepted_frames != sender_.frame_count() ||
            ack.accepted_bytes != sender_.encoded_bytes()) {
          return fail(corruption("grouped shuffle TLS acknowledgment extent differs"));
        }
        state_ = DistributedVectorGroupedAggregateShuffleTlsState::kComplete;
        interest_ = {};
      } else {
        interest_ = {.want_read = true};
      }
    }
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedVectorGroupedAggregateShuffleStreamSender sender_;
  const DistributedVectorGroupedAggregateShuffleAuthority& authority_;
  DistributedVectorGroupedAggregateShuffleTlsClientConfig config_;
  TimePoint deadline_;
  DistributedVectorGroupedAggregateShuffleTlsState state_{
      DistributedVectorGroupedAggregateShuffleTlsState::kHandshaking};
  DistributedVectorGroupedAggregateShuffleTlsInterest interest_{.want_write = true};
  DistributedVectorGroupedAggregateShuffleAckV1Reader ack_reader_;
  std::array<std::byte, kTlsScratchSize> scratch_{};
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle TLS client has not failed"};
};

class DistributedVectorGroupedAggregateShuffleTlsServer::Impl {
public:
  Impl(network::TlsSocket socket, query::QueryResourceContext resources,
       const DistributedVectorGroupedAggregateShuffleTlsServerConfig config, const TimePoint now)
      : socket_(std::move(socket)), resources_(std::move(resources)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedVectorGroupedAggregateShuffleTlsState::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedVectorGroupedAggregateShuffleTlsState::kFailed;
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
    auto receiver = DistributedVectorGroupedAggregateShuffleStreamReceiver::create(
        *config_.authority, config_.local_node_id, *config_.node_authorizer, *peer, resources_,
        config_.limits.stream);
    if (!receiver.has_value())
      return fail(receiver.error());
    receiver_.emplace(std::move(*receiver));
    state_ = DistributedVectorGroupedAggregateShuffleTlsState::kReadingStream;
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
      return fail(unavailable("grouped shuffle TLS handshake closed"));
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
        (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.read(scratch_);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped shuffle TLS closed before stream terminal"));
    if (progress->state == network::TlsIoState::kWantRead)
      interest_ = {.want_read = true};
    else if (progress->state == network::TlsIoState::kWantWrite)
      interest_ = {.want_write = true};
    else {
      if (progress->bytes_transferred == 0U)
        return fail(unavailable("grouped shuffle TLS stream read made no progress"));
      const common::ByteView received =
          common::ByteView{scratch_}.first(progress->bytes_transferred);
      auto step = receiver_->consume(received);
      if (!step.has_value())
        return fail(step.error());
      if (step->consumed_bytes != received.size())
        return fail(corruption("grouped shuffle TLS terminal has a suffix"));
      if (step->complete) {
        auto complete = receiver_->take_complete_stream();
        if (!complete.has_value())
          return fail(complete.error());
        DistributedVectorGroupedAggregateShuffleAckV1 ack{
            .query_id = config_.authority->query_id(),
            .edge = complete->edge,
            .partition_count = config_.authority->partition_count(),
            .accepted_frames = static_cast<std::uint32_t>(complete->messages.size()),
            .accepted_bytes = complete->encoded_bytes};
        auto writer = DistributedVectorGroupedAggregateShuffleAckV1WriteCursor::create(
            ack, *config_.authority);
        if (!writer.has_value())
          return fail(writer.error());
        stream_.emplace(std::move(*complete));
        ack_writer_.emplace(std::move(*writer));
        receiver_.reset();
        state_ = DistributedVectorGroupedAggregateShuffleTlsState::kWritingAck;
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
    auto progress = socket_.write(ack_writer_->pending_write());
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped shuffle TLS closed while writing acknowledgment"));
    if (progress->state == network::TlsIoState::kWantRead)
      interest_ = {.want_read = true};
    else if (progress->state == network::TlsIoState::kWantWrite)
      interest_ = {.want_write = true};
    else {
      if (progress->bytes_transferred == 0U)
        return fail(unavailable("grouped shuffle TLS acknowledgment write made no progress"));
      common::Status consumed = ack_writer_->consume_written(progress->bytes_transferred);
      if (!consumed.is_ok())
        return fail(std::move(consumed));
      if (ack_writer_->complete()) {
        state_ = DistributedVectorGroupedAggregateShuffleTlsState::kComplete;
        interest_ = {};
      } else {
        interest_ = {.want_write = true};
      }
    }
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  query::QueryResourceContext resources_;
  DistributedVectorGroupedAggregateShuffleTlsServerConfig config_;
  TimePoint deadline_;
  DistributedVectorGroupedAggregateShuffleTlsState state_{
      DistributedVectorGroupedAggregateShuffleTlsState::kHandshaking};
  DistributedVectorGroupedAggregateShuffleTlsInterest interest_{.want_read = true};
  std::optional<DistributedVectorGroupedAggregateShuffleStreamReceiver> receiver_;
  std::optional<DistributedVectorGroupedAggregateShuffleCompleteStream> stream_;
  std::optional<DistributedVectorGroupedAggregateShuffleAckV1WriteCursor> ack_writer_;
  std::array<std::byte, kTlsScratchSize> scratch_{};
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle TLS server has not failed"};
};

DistributedVectorGroupedAggregateShuffleTlsClient::
    DistributedVectorGroupedAggregateShuffleTlsClient(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleTlsClient::
    ~DistributedVectorGroupedAggregateShuffleTlsClient() = default;
DistributedVectorGroupedAggregateShuffleTlsClient::
    DistributedVectorGroupedAggregateShuffleTlsClient(
        DistributedVectorGroupedAggregateShuffleTlsClient&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleTlsClient&
DistributedVectorGroupedAggregateShuffleTlsClient::operator=(
    DistributedVectorGroupedAggregateShuffleTlsClient&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleTlsClient>
DistributedVectorGroupedAggregateShuffleTlsClient::create(
    network::TlsSocket socket, DistributedVectorGroupedAggregateShuffleStreamSender sender,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const DistributedVectorGroupedAggregateShuffleTlsClientConfig config, const TimePoint now) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      !valid_limits<TimePoint>(config.limits) || sender.complete() ||
      sender.frame_count() > config.limits.stream.maximum_frames ||
      sender.encoded_bytes() > config.limits.stream.maximum_encoded_bytes ||
      !authority.validate_edge(sender.edge()).is_ok()) {
    return common::make_unexpected(invalid("grouped shuffle TLS client configuration is invalid"));
  }
  try {
    return DistributedVectorGroupedAggregateShuffleTlsClient{
        std::make_unique<Impl>(std::move(socket), std::move(sender), authority, config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle TLS client allocation failed"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleTlsClient::on_ready(const bool readable,
                                                                           const bool writable,
                                                                           const TimePoint now) {
  if (!implementation_)
    return invalid("grouped shuffle TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleTlsState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleTlsState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_)
    return impl.fail(
        unavailable(impl.state_ == DistributedVectorGroupedAggregateShuffleTlsState::kHandshaking
                        ? "grouped shuffle TLS handshake timed out"
                        : "grouped shuffle TLS exchange timed out"));
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleTlsState::kHandshaking)
    return impl.handshake(readable, writable, now);
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleTlsState::kWritingStream)
    return impl.write_stream(readable, writable);
  return impl.read_ack(readable, writable);
}

DistributedVectorGroupedAggregateShuffleTlsState
DistributedVectorGroupedAggregateShuffleTlsClient::state() const noexcept {
  return implementation_ ? implementation_->state_
                         : DistributedVectorGroupedAggregateShuffleTlsState::kFailed;
}
DistributedVectorGroupedAggregateShuffleTlsInterest
DistributedVectorGroupedAggregateShuffleTlsClient::interest() const noexcept {
  return implementation_ ? implementation_->interest_
                         : DistributedVectorGroupedAggregateShuffleTlsInterest{};
}
const common::Status& DistributedVectorGroupedAggregateShuffleTlsClient::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle TLS client is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

DistributedVectorGroupedAggregateShuffleTlsServer::
    DistributedVectorGroupedAggregateShuffleTlsServer(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleTlsServer::
    ~DistributedVectorGroupedAggregateShuffleTlsServer() = default;
DistributedVectorGroupedAggregateShuffleTlsServer::
    DistributedVectorGroupedAggregateShuffleTlsServer(
        DistributedVectorGroupedAggregateShuffleTlsServer&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleTlsServer&
DistributedVectorGroupedAggregateShuffleTlsServer::operator=(
    DistributedVectorGroupedAggregateShuffleTlsServer&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleTlsServer>
DistributedVectorGroupedAggregateShuffleTlsServer::create(
    network::TlsSocket socket, query::QueryResourceContext resources,
    const DistributedVectorGroupedAggregateShuffleTlsServerConfig config, const TimePoint now) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      config.authority == nullptr || config.local_node_id == 0U ||
      !valid_limits<TimePoint>(config.limits)) {
    return common::make_unexpected(invalid("grouped shuffle TLS server configuration is invalid"));
  }
  try {
    return DistributedVectorGroupedAggregateShuffleTlsServer{
        std::make_unique<Impl>(std::move(socket), std::move(resources), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle TLS server allocation failed"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleTlsServer::on_ready(const bool readable,
                                                                           const bool writable,
                                                                           const TimePoint now) {
  if (!implementation_)
    return invalid("grouped shuffle TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleTlsState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleTlsState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_)
    return impl.fail(
        unavailable(impl.state_ == DistributedVectorGroupedAggregateShuffleTlsState::kHandshaking
                        ? "grouped shuffle TLS handshake timed out"
                        : "grouped shuffle TLS exchange timed out"));
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleTlsState::kHandshaking)
    return impl.handshake(readable, writable, now);
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleTlsState::kReadingStream)
    return impl.read_stream(readable, writable);
  return impl.write_ack(readable, writable);
}

DistributedVectorGroupedAggregateShuffleTlsState
DistributedVectorGroupedAggregateShuffleTlsServer::state() const noexcept {
  return implementation_ ? implementation_->state_
                         : DistributedVectorGroupedAggregateShuffleTlsState::kFailed;
}
DistributedVectorGroupedAggregateShuffleTlsInterest
DistributedVectorGroupedAggregateShuffleTlsServer::interest() const noexcept {
  return implementation_ ? implementation_->interest_
                         : DistributedVectorGroupedAggregateShuffleTlsInterest{};
}
common::Result<DistributedVectorGroupedAggregateShuffleCompleteStream>
DistributedVectorGroupedAggregateShuffleTlsServer::take_complete_stream() {
  if (!implementation_ ||
      implementation_->state_ != DistributedVectorGroupedAggregateShuffleTlsState::kComplete ||
      !implementation_->stream_.has_value()) {
    return common::make_unexpected(invalid("grouped shuffle TLS complete stream is unavailable"));
  }
  auto stream = std::move(*implementation_->stream_);
  implementation_->stream_.reset();
  return stream;
}
const common::Status& DistributedVectorGroupedAggregateShuffleTlsServer::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
