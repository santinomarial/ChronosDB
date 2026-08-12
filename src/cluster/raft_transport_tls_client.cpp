#include "chronos/cluster/raft_transport_tls_client.hpp"

#include <chrono>
#include <new>
#include <optional>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}
[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}
[[nodiscard]] common::Status unauthenticated(const char* message) {
  return {common::StatusCode::kUnauthenticated, message};
}
[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftTransportTlsClient::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] RaftTransportTlsClient::TimePoint
deadline_after(const RaftTransportTlsClient::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<RaftTransportTlsClient::TimePoint::duration>(timeout);
  if (now > RaftTransportTlsClient::TimePoint::max() - duration)
    return RaftTransportTlsClient::TimePoint::max();
  return now + duration;
}

} // namespace

class RaftTransportTlsClient::Impl {
public:
  struct PendingFrame {
    std::vector<std::byte> bytes;
    std::size_t written_bytes{};
  };

  Impl(network::TlsSocket socket, RaftTransportTlsClientConfig config,
       std::vector<std::optional<PendingFrame>> slots, const TimePoint now)
      : socket_(std::move(socket)), config_(config), slots_(std::move(slots)),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) noexcept {
    if (state_ != RaftTransportTlsClientState::kFailed) {
      failure_ = std::move(status);
      state_ = RaftTransportTlsClientState::kFailed;
      interest_ = {};
    }
    return failure_;
  }

  [[nodiscard]] PendingFrame& front() noexcept {
    return *slots_[head_];
  }

  void pop_front(const TimePoint now) noexcept {
    queued_bytes_ -= front().bytes.size();
    slots_[head_].reset();
    head_ = (head_ + 1U) % slots_.size();
    --count_;
    if (count_ == 0U) {
      interest_ = {};
    } else {
      interest_ = {.want_write = true};
      deadline_ = deadline_after(now, config_.limits.frame_write_timeout);
    }
  }

  [[nodiscard]] common::Status authenticate_server(const TimePoint now) {
    auto fingerprint = socket_.peer_certificate_sha256();
    if (!fingerprint.has_value())
      return fail(fingerprint.error());
    const network::NetworkSecurityConfig security{.mode =
                                                      network::TransportSecurityMode::kTlsRequired,
                                                  .authenticator = config_.authenticator};
    auto authentication =
        network::authenticate_peer(security, {.ipv4_address = config_.peer_ipv4_address,
                                              .transport_authenticated = true,
                                              .peer_certificate_sha256 = *fingerprint});
    if (!authentication.has_value())
      return fail(authentication.error());
    if (!authentication->authorized || authentication->principal_id == 0U)
      return fail(unauthenticated("Raft TLS server principal is not authenticated"));
    auto authorized =
        config_.node_authorizer->authorize_node(authentication->principal_id, config_.peer_node_id);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized)
      return fail(unauthenticated("Raft TLS server principal cannot claim the peer node"));
    state_ = RaftTransportTlsClientState::kReady;
    if (count_ == 0U) {
      interest_ = {};
    } else {
      interest_ = {.want_write = true};
      deadline_ = deadline_after(now, config_.limits.frame_write_timeout);
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_handshake(const bool readable, const bool writable,
                                                 const TimePoint now) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.handshake();
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("Raft TLS client handshake closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    return authenticate_server(now);
  }

  [[nodiscard]] common::Status advance_write(const bool readable, const bool writable,
                                             const TimePoint now) {
    if (count_ == 0U)
      return common::Status::ok();
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    PendingFrame& pending = front();
    auto progress = socket_.write(common::ByteView{pending.bytes}.subspan(pending.written_bytes));
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("Raft TLS output socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U ||
        progress->bytes_transferred > pending.bytes.size() - pending.written_bytes)
      return fail(unavailable("Raft TLS output write made invalid progress"));
    pending.written_bytes += progress->bytes_transferred;
    if (pending.written_bytes == pending.bytes.size())
      pop_front(now);
    else
      interest_ = {.want_write = true};
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  RaftTransportTlsClientConfig config_;
  std::vector<std::optional<PendingFrame>> slots_;
  std::size_t head_{};
  std::size_t tail_{};
  std::size_t count_{};
  std::size_t queued_bytes_{};
  TimePoint deadline_{};
  RaftTransportTlsClientState state_{RaftTransportTlsClientState::kHandshaking};
  RaftTransportTlsClientInterest interest_{.want_read = true};
  common::Status failure_{common::StatusCode::kInternal, "Raft TLS client has not failed"};
};

RaftTransportTlsClient::RaftTransportTlsClient(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftTransportTlsClient::~RaftTransportTlsClient() = default;
RaftTransportTlsClient::RaftTransportTlsClient(RaftTransportTlsClient&&) noexcept = default;
RaftTransportTlsClient&
RaftTransportTlsClient::operator=(RaftTransportTlsClient&&) noexcept = default;

common::Result<RaftTransportTlsClient>
RaftTransportTlsClient::create(network::TlsSocket socket, const RaftTransportTlsClientConfig config,
                               const TimePoint now) {
  const auto& limits = config.limits;
  if (config.local_node_id == 0U || config.peer_node_id == 0U ||
      config.local_node_id == config.peer_node_id || config.authenticator == nullptr ||
      config.node_authorizer == nullptr || limits.maximum_queued_frames == 0U ||
      limits.maximum_queued_bytes <
          raft::kRaftTransportHeaderSize + raft::kRaftTransportTrailerSize ||
      !valid_timeout(limits.handshake_timeout) || !valid_timeout(limits.frame_write_timeout))
    return common::make_unexpected(invalid("Raft TLS client configuration is invalid"));
  auto valid_codec = raft::RaftTransportFrameReader::create(limits.codec);
  if (!valid_codec.has_value())
    return common::make_unexpected(valid_codec.error());
  try {
    std::vector<std::optional<Impl::PendingFrame>> slots(limits.maximum_queued_frames);
    return RaftTransportTlsClient{
        std::make_unique<Impl>(std::move(socket), config, std::move(slots), now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft TLS client allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft TLS client queue exceeds container limits"));
  }
}

common::Status RaftTransportTlsClient::try_enqueue(std::vector<std::byte>& encoded_frame,
                                                   const TimePoint now) {
  if (!implementation_)
    return invalid("Raft TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == RaftTransportTlsClientState::kFailed)
    return impl.failure_;
  auto envelope = raft::decode_raft_transport_envelope_v1(encoded_frame, impl.config_.limits.codec);
  if (!envelope.has_value())
    return envelope.error();
  if (envelope->source != impl.config_.local_node_id ||
      envelope->destination != impl.config_.peer_node_id)
    return invalid("Raft TLS queued frame route differs from connection ownership");
  return try_enqueue_prevalidated(encoded_frame, now);
}

common::Status
RaftTransportTlsClient::try_enqueue_prevalidated(std::vector<std::byte>& encoded_frame,
                                                 const TimePoint now) {
  if (!implementation_)
    return invalid("Raft TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == RaftTransportTlsClientState::kFailed)
    return impl.failure_;
  if (impl.count_ == impl.slots_.size() ||
      encoded_frame.size() > impl.config_.limits.maximum_queued_bytes - impl.queued_bytes_)
    return exhausted("Raft TLS output queue is full");
  const bool was_empty = impl.count_ == 0U;
  const std::size_t size = encoded_frame.size();
  impl.slots_[impl.tail_].emplace(Impl::PendingFrame{std::move(encoded_frame), 0U});
  impl.tail_ = (impl.tail_ + 1U) % impl.slots_.size();
  ++impl.count_;
  impl.queued_bytes_ += size;
  if (was_empty && impl.state_ == RaftTransportTlsClientState::kReady) {
    impl.interest_ = {.want_write = true};
    impl.deadline_ = deadline_after(now, impl.config_.limits.frame_write_timeout);
  }
  return common::Status::ok();
}

common::Status RaftTransportTlsClient::on_ready(const bool readable, const bool writable,
                                                const TimePoint now) {
  if (!implementation_)
    return invalid("Raft TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == RaftTransportTlsClientState::kFailed)
    return impl.failure_;
  if (impl.state_ == RaftTransportTlsClientState::kHandshaking) {
    if (now >= impl.deadline_)
      return impl.fail(unavailable("Raft TLS client handshake timed out"));
    return impl.advance_handshake(readable, writable, now);
  }
  if (impl.count_ != 0U && now >= impl.deadline_)
    return impl.fail(unavailable("Raft TLS frame write timed out"));
  return impl.advance_write(readable, writable, now);
}

common::Result<std::vector<std::vector<std::byte>>> RaftTransportTlsClient::drain_retry_frames() {
  std::vector<std::vector<std::byte>> frames;
  if (!implementation_)
    return common::make_unexpected(invalid("Raft TLS client is empty"));
  Impl& impl = *implementation_;
  try {
    frames.reserve(impl.count_);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft TLS retry drain allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft TLS retry drain exceeds container limits"));
  }
  while (impl.count_ != 0U) {
    frames.push_back(std::move(impl.front().bytes));
    impl.slots_[impl.head_].reset();
    impl.head_ = (impl.head_ + 1U) % impl.slots_.size();
    --impl.count_;
  }
  impl.tail_ = impl.head_;
  impl.queued_bytes_ = 0U;
  if (impl.state_ == RaftTransportTlsClientState::kReady)
    impl.interest_ = {};
  return frames;
}

RaftTransportTlsClientState RaftTransportTlsClient::state() const noexcept {
  return implementation_ ? implementation_->state_ : RaftTransportTlsClientState::kFailed;
}
RaftTransportTlsClientInterest RaftTransportTlsClient::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : RaftTransportTlsClientInterest{};
}
std::size_t RaftTransportTlsClient::queued_frames() const noexcept {
  return implementation_ ? implementation_->count_ : 0U;
}
std::size_t RaftTransportTlsClient::queued_bytes() const noexcept {
  return implementation_ ? implementation_->queued_bytes_ : 0U;
}
std::size_t RaftTransportTlsClient::available_frames() const noexcept {
  return implementation_ ? implementation_->slots_.size() - implementation_->count_ : 0U;
}
std::size_t RaftTransportTlsClient::available_bytes() const noexcept {
  return implementation_
             ? implementation_->config_.limits.maximum_queued_bytes - implementation_->queued_bytes_
             : 0U;
}
raft::NodeId RaftTransportTlsClient::local_node_id() const noexcept {
  return implementation_ ? implementation_->config_.local_node_id : 0U;
}
raft::NodeId RaftTransportTlsClient::peer_node_id() const noexcept {
  return implementation_ ? implementation_->config_.peer_node_id : 0U;
}
const common::Status& RaftTransportTlsClient::failure() const noexcept {
  static const common::Status empty_failure{common::StatusCode::kInvalidArgument,
                                            "Raft TLS client is empty"};
  return implementation_ ? implementation_->failure_ : empty_failure;
}

} // namespace chronos::cluster
