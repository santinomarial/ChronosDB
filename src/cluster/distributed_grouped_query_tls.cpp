#include "chronos/cluster/distributed_grouped_query_tls.hpp"

#include <chrono>
#include <cstddef>
#include <new>
#include <optional>
#include <utility>
#include <vector>

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

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

template <typename TimePoint>
[[nodiscard]] bool valid_limits(const DistributedGroupedQueryTlsLimits& limits) noexcept {
  const auto maximum =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  return limits.handshake_timeout.count() > 0 && limits.handshake_timeout <= maximum &&
         limits.exchange_timeout.count() > 0 && limits.exchange_timeout <= maximum &&
         limits.maximum_response_frames > 0U &&
         limits.maximum_response_frames <= query::kMaximumDistributedCoordinatorMessages;
}

template <typename TimePoint>
[[nodiscard]] TimePoint deadline_after(const TimePoint now,
                                       const std::chrono::milliseconds timeout) noexcept {
  const auto duration = std::chrono::duration_cast<typename TimePoint::duration>(timeout);
  return now > TimePoint::max() - duration ? TimePoint::max() : now + duration;
}

[[nodiscard]] bool terminal_response(const DistributedGroupedQueryResponse& response) {
  if (response.status_code != common::StatusCode::kOk)
    return true;
  if (!response.payload.has_value())
    return false;
  const auto& payload = response.payload.value();
  if (std::holds_alternative<query::GroupedExchangeTerminalMessage>(payload))
    return true;
  const auto* message = std::get_if<query::GroupedFloat64ExchangeMessage>(&payload);
  return message != nullptr && message->terminal;
}

} // namespace

class DistributedGroupedQueryTlsClient::Impl {
public:
  Impl(network::TlsSocket socket, DistributedGroupedQueryFrameWriteCursor request,
       DistributedGroupedQueryRequest identity, const raft::NodeId target,
       DistributedGroupedQueryTlsClientConfig config, const TimePoint now)
      : socket_(std::move(socket)), request_(std::move(request)), identity_(std::move(identity)),
        target_(target), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedGroupedQueryTlsState::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedGroupedQueryTlsState::kFailed;
      interest_ = {};
      responses_.clear();
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
    auto authentication =
        network::authenticate_peer(security, {.ipv4_address = config_.peer_ipv4_address,
                                              .transport_authenticated = true,
                                              .peer_certificate_sha256 = *fingerprint});
    if (!authentication.has_value())
      return fail(authentication.error());
    if (!authentication->authorized || authentication->principal_id == 0U)
      return fail(unauthenticated("grouped query server principal is not authenticated"));
    auto authorized =
        config_.node_authorizer->authorize_node(authentication->principal_id, target_);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized)
      return fail(unauthenticated("TLS server principal cannot claim grouped query target"));
    state_ = DistributedGroupedQueryTlsState::kWritingRequest;
    interest_ = {.want_write = true};
    deadline_ = deadline_after(now, config_.limits.exchange_timeout);
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
      return fail(unavailable("grouped query TLS handshake closed"));
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

  [[nodiscard]] common::Status advance_write(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.write(request_.pending_write());
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped query request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("grouped query request write made no progress"));
    const common::Status consumed = request_.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (request_.complete()) {
      state_ = DistributedGroupedQueryTlsState::kReadingResponses;
      interest_ = {.want_read = true};
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status accept_response(const DistributedGroupedQueryResponse& response) {
    const auto& fragment = identity_.dispatch.fragment.aggregate;
    if (response.source_node_id != identity_.target_node_id ||
        response.target_node_id != identity_.source_node_id ||
        response.query_id != fragment.query_id || response.tablet_id != fragment.tablet_id) {
      return fail(corruption("grouped query TLS response is not correlated"));
    }
    if (responses_.size() == config_.limits.maximum_response_frames)
      return fail(exhausted("grouped query TLS response frame limit exceeded"));
    if (response.status_code == common::StatusCode::kOk) {
      if (!response.payload.has_value())
        return fail(corruption("grouped query success has no payload"));
      if (const auto* message =
              std::get_if<query::GroupedFloat64ExchangeMessage>(&*response.payload)) {
        if (message->sequence != responses_.size() + 1U)
          return fail(corruption("grouped query TLS response sequence is not contiguous"));
      } else if (!responses_.empty() ||
                 std::get<query::GroupedExchangeTerminalMessage>(*response.payload).sequence !=
                     1U) {
        return fail(corruption("grouped query TLS terminal-only response is misplaced"));
      }
    } else if (!responses_.empty()) {
      return fail(corruption("grouped query failure followed successful partials"));
    }
    const bool terminal = terminal_response(response);
    try {
      responses_.push_back(response);
    } catch (const std::bad_alloc&) {
      return fail(exhausted("grouped query TLS response allocation failed"));
    }
    if (terminal) {
      state_ = DistributedGroupedQueryTlsState::kComplete;
      interest_ = {};
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_read(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    auto progress = socket_.read(response_scratch_);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped query response socket closed before terminal"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("grouped query response read made no progress"));
    std::size_t offset = 0U;
    const common::ByteView received =
        common::ByteView{response_scratch_}.first(progress->bytes_transferred);
    while (offset < received.size()) {
      auto step = response_reader_.consume(received.subspan(offset));
      if (!step.has_value())
        return fail(step.error());
      if (step->consumed_bytes == 0U)
        return fail(corruption("grouped query response reader made no progress"));
      offset += step->consumed_bytes;
      if (step->response.has_value()) {
        const auto* response =
            step->response.transform([](const auto& value) { return &value; }).value_or(nullptr);
        common::Status accepted = accept_response(*response);
        if (!accepted.is_ok())
          return accepted;
        if (state_ == DistributedGroupedQueryTlsState::kComplete && offset != received.size())
          return fail(corruption("grouped query terminal response has a coalesced suffix"));
      }
    }
    if (state_ != DistributedGroupedQueryTlsState::kComplete)
      interest_ = {.want_read = true};
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedGroupedQueryFrameWriteCursor request_;
  DistributedGroupedQueryRequest identity_;
  raft::NodeId target_{};
  DistributedGroupedQueryTlsClientConfig config_;
  TimePoint deadline_;
  DistributedGroupedQueryTlsState state_{DistributedGroupedQueryTlsState::kHandshaking};
  DistributedGroupedQueryTlsInterest interest_{.want_write = true};
  DistributedGroupedQueryResponseReader response_reader_;
  std::array<std::byte, kMaximumDistributedGroupedQueryResponseSize> response_scratch_{};
  std::vector<DistributedGroupedQueryResponse> responses_;
  common::Status failure_{common::StatusCode::kInternal, "grouped query TLS client has not failed"};
};

class DistributedGroupedQueryTlsServer::Impl {
public:
  Impl(network::TlsSocket socket, DistributedGroupedQueryTlsServerConfig config,
       const TimePoint now) noexcept
      : socket_(std::move(socket)), config_(config),
        deadline_(deadline_after(now, config.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state_ != DistributedGroupedQueryTlsState::kFailed) {
      failure_ = std::move(status);
      state_ = DistributedGroupedQueryTlsState::kFailed;
      interest_ = {};
      response_writers_.clear();
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
    auto authentication =
        network::authenticate_peer(security, {.ipv4_address = config_.peer_ipv4_address,
                                              .transport_authenticated = true,
                                              .peer_certificate_sha256 = *fingerprint});
    if (!authentication.has_value())
      return fail(authentication.error());
    if (!authentication->authorized || authentication->principal_id == 0U)
      return fail(unauthenticated("grouped query client principal is not authenticated"));
    authenticated_peer_ = *authentication;
    state_ = DistributedGroupedQueryTlsState::kReadingRequest;
    interest_ = {.want_read = true};
    deadline_ = deadline_after(now, config_.limits.exchange_timeout);
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
      return fail(unavailable("grouped query TLS handshake closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    return authenticate_client(now);
  }

  [[nodiscard]] common::Status advance_read(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    auto progress = socket_.read(request_scratch_);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped query request socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("grouped query request read made no progress"));
    const common::ByteView received =
        common::ByteView{request_scratch_}.first(progress->bytes_transferred);
    auto step = request_reader_.consume(received);
    if (!step.has_value())
      return fail(step.error());
    if (step->consumed_bytes != received.size())
      return fail(corruption("grouped query request has a coalesced suffix"));
    if (!step->request.has_value()) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    const auto* decoded_request =
        step->request.transform([](const auto& value) { return &value; }).value_or(nullptr);
    auto request = encode_distributed_grouped_query_request_v1(*decoded_request);
    if (!request.has_value())
      return fail(request.error());
    auto responses = config_.receiver->receive(*request, *authenticated_peer_);
    if (!responses.has_value())
      return fail(responses.error());
    if (responses->empty() || responses->size() > config_.limits.maximum_response_frames)
      return fail(exhausted("grouped query receiver response count exceeds TLS limit"));
    try {
      response_writers_.reserve(responses->size());
      for (auto& response : *responses) {
        auto writer = DistributedGroupedQueryFrameWriteCursor::create(std::move(response));
        if (!writer.has_value())
          return fail(writer.error());
        response_writers_.push_back(std::move(*writer));
      }
    } catch (const std::bad_alloc&) {
      return fail(exhausted("grouped query TLS response allocation failed"));
    }
    state_ = DistributedGroupedQueryTlsState::kWritingResponses;
    interest_ = {.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status advance_write(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto& writer = response_writers_[response_index_];
    auto progress = socket_.write(writer.pending_write());
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed)
      return fail(unavailable("grouped query response socket closed"));
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U)
      return fail(unavailable("grouped query response write made no progress"));
    const common::Status consumed = writer.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (writer.complete()) {
      ++response_index_;
      if (response_index_ == response_writers_.size()) {
        state_ = DistributedGroupedQueryTlsState::kComplete;
        interest_ = {};
      } else {
        interest_ = {.want_write = true};
      }
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedGroupedQueryTlsServerConfig config_;
  TimePoint deadline_;
  DistributedGroupedQueryTlsState state_{DistributedGroupedQueryTlsState::kHandshaking};
  DistributedGroupedQueryTlsInterest interest_{.want_read = true};
  std::optional<network::PeerAuthenticationResult> authenticated_peer_;
  DistributedGroupedQueryRequestReader request_reader_;
  std::array<std::byte, kMaximumDistributedGroupedQueryRequestSize> request_scratch_{};
  std::vector<DistributedGroupedQueryFrameWriteCursor> response_writers_;
  std::size_t response_index_{};
  common::Status failure_{common::StatusCode::kInternal, "grouped query TLS server has not failed"};
};

DistributedGroupedQueryTlsClient::DistributedGroupedQueryTlsClient(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedGroupedQueryTlsClient::~DistributedGroupedQueryTlsClient() = default;
DistributedGroupedQueryTlsClient::DistributedGroupedQueryTlsClient(
    DistributedGroupedQueryTlsClient&&) noexcept = default;
DistributedGroupedQueryTlsClient&
DistributedGroupedQueryTlsClient::operator=(DistributedGroupedQueryTlsClient&&) noexcept = default;

common::Result<DistributedGroupedQueryTlsClient> DistributedGroupedQueryTlsClient::create(
    network::TlsSocket socket, DistributedGroupedQueryAttempt attempt,
    const DistributedGroupedQueryTlsClientConfig config, const TimePoint now) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      !valid_limits<TimePoint>(config.limits) || attempt.attempt_number == 0U ||
      attempt.target_node_id == 0U) {
    return common::make_unexpected(invalid("grouped query TLS client configuration is invalid"));
  }
  auto identity = decode_distributed_grouped_query_request_v1(attempt.request_bytes);
  if (!identity.has_value())
    return common::make_unexpected(identity.error());
  if (identity->target_node_id != attempt.target_node_id)
    return common::make_unexpected(invalid("grouped query TLS attempt target is inconsistent"));
  auto request = DistributedGroupedQueryFrameWriteCursor::create(std::move(attempt.request_bytes));
  if (!request.has_value())
    return common::make_unexpected(request.error());
  try {
    return DistributedGroupedQueryTlsClient{
        std::make_unique<Impl>(std::move(socket), std::move(*request), std::move(*identity),
                               attempt.target_node_id, config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped query TLS client allocation failed"));
  }
}

common::Status DistributedGroupedQueryTlsClient::on_ready(const bool readable, const bool writable,
                                                          const TimePoint now) {
  if (!implementation_)
    return invalid("grouped query TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedGroupedQueryTlsState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedGroupedQueryTlsState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_)
    return impl.fail(unavailable(impl.state_ == DistributedGroupedQueryTlsState::kHandshaking
                                     ? "grouped query TLS handshake timed out"
                                     : "grouped query TLS exchange timed out"));
  if (impl.state_ == DistributedGroupedQueryTlsState::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  if (impl.state_ == DistributedGroupedQueryTlsState::kWritingRequest)
    return impl.advance_write(readable, writable);
  return impl.advance_read(readable, writable);
}

DistributedGroupedQueryTlsState DistributedGroupedQueryTlsClient::state() const noexcept {
  return implementation_ ? implementation_->state_ : DistributedGroupedQueryTlsState::kFailed;
}

DistributedGroupedQueryTlsInterest DistributedGroupedQueryTlsClient::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : DistributedGroupedQueryTlsInterest{};
}

common::Result<std::span<const DistributedGroupedQueryResponse>>
DistributedGroupedQueryTlsClient::responses() const {
  if (!implementation_ || implementation_->state_ != DistributedGroupedQueryTlsState::kComplete)
    return common::make_unexpected(invalid("grouped query TLS responses are unavailable"));
  return std::span<const DistributedGroupedQueryResponse>{implementation_->responses_};
}

const common::Status& DistributedGroupedQueryTlsClient::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped query TLS client is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

DistributedGroupedQueryTlsServer::DistributedGroupedQueryTlsServer(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedGroupedQueryTlsServer::~DistributedGroupedQueryTlsServer() = default;
DistributedGroupedQueryTlsServer::DistributedGroupedQueryTlsServer(
    DistributedGroupedQueryTlsServer&&) noexcept = default;
DistributedGroupedQueryTlsServer&
DistributedGroupedQueryTlsServer::operator=(DistributedGroupedQueryTlsServer&&) noexcept = default;

common::Result<DistributedGroupedQueryTlsServer>
DistributedGroupedQueryTlsServer::create(network::TlsSocket socket,
                                         const DistributedGroupedQueryTlsServerConfig config,
                                         const TimePoint now) {
  if (config.authenticator == nullptr || config.receiver == nullptr ||
      !valid_limits<TimePoint>(config.limits)) {
    return common::make_unexpected(invalid("grouped query TLS server configuration is invalid"));
  }
  try {
    return DistributedGroupedQueryTlsServer{std::make_unique<Impl>(std::move(socket), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped query TLS server allocation failed"));
  }
}

common::Status DistributedGroupedQueryTlsServer::on_ready(const bool readable, const bool writable,
                                                          const TimePoint now) {
  if (!implementation_)
    return invalid("grouped query TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedGroupedQueryTlsState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedGroupedQueryTlsState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_)
    return impl.fail(unavailable(impl.state_ == DistributedGroupedQueryTlsState::kHandshaking
                                     ? "grouped query TLS handshake timed out"
                                     : "grouped query TLS exchange timed out"));
  if (impl.state_ == DistributedGroupedQueryTlsState::kHandshaking)
    return impl.advance_handshake(readable, writable, now);
  if (impl.state_ == DistributedGroupedQueryTlsState::kReadingRequest)
    return impl.advance_read(readable, writable);
  return impl.advance_write(readable, writable);
}

DistributedGroupedQueryTlsState DistributedGroupedQueryTlsServer::state() const noexcept {
  return implementation_ ? implementation_->state_ : DistributedGroupedQueryTlsState::kFailed;
}

DistributedGroupedQueryTlsInterest DistributedGroupedQueryTlsServer::interest() const noexcept {
  return implementation_ ? implementation_->interest_ : DistributedGroupedQueryTlsInterest{};
}

const common::Status& DistributedGroupedQueryTlsServer::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped query TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
