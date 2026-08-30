#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tls.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>
#include <variant>

namespace chronos::cluster {
namespace {

inline constexpr std::size_t kRequestScratchBytes = std::size_t{16U} * 1024U;
inline constexpr std::size_t kResponseScratchBytes = 4096U;

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] common::Status
validate_limits(const DistributedVectorGroupedAggregateShuffleJobControlTlsLimits& limits) {
  if (!valid_timeout(limits.handshake_timeout) || !valid_timeout(limits.exchange_timeout)) {
    return status(common::StatusCode::kInvalidArgument,
                  "grouped shuffle reducer-job TLS timeouts are invalid");
  }
  return validate_distributed_vector_grouped_aggregate_shuffle_job_control_decode_limits(
      limits.request);
}

template <typename TimePoint>
[[nodiscard]] TimePoint deadline_after(const TimePoint now,
                                       const std::chrono::milliseconds timeout) noexcept {
  const auto duration = std::chrono::duration_cast<typename TimePoint::duration>(timeout);
  return now > TimePoint::max() - duration ? TimePoint::max() : now + duration;
}

struct Correlation {
  DistributedVectorGroupedAggregateShuffleJobControlAction action;
  common::Uuid query_id;
  raft::NodeId coordinator_node_id{};
  raft::NodeId target_node_id{};
};

[[nodiscard]] Correlation
correlation(const DistributedVectorGroupedAggregateShuffleJobControlRequest& request) {
  if (const auto* prepare =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobPrepare>(&request)) {
    return {.action = DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
            .query_id = prepare->authority.query_id(),
            .coordinator_node_id = prepare->coordinator_node_id,
            .target_node_id = prepare->target_node_id};
  }
  if (const auto* seal = std::get_if<DistributedVectorGroupedAggregateShuffleJobSeal>(&request)) {
    return {.action = DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal,
            .query_id = seal->query_id,
            .coordinator_node_id = seal->coordinator_node_id,
            .target_node_id = seal->target_node_id};
  }
  if (const auto* routes =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobInstallRoutes>(&request)) {
    return {.action = DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
            .query_id = routes->query_id,
            .coordinator_node_id = routes->coordinator_node_id,
            .target_node_id = routes->target_node_id};
  }
  if (const auto* cancel =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobCancel>(&request)) {
    return {.action = DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
            .query_id = cancel->query_id,
            .coordinator_node_id = cancel->coordinator_node_id,
            .target_node_id = cancel->target_node_id};
  }
  const auto& renewal = std::get<DistributedVectorGroupedAggregateShuffleJobRenewLease>(request);
  return {.action = DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease,
          .query_id = renewal.query_id,
          .coordinator_node_id = renewal.coordinator_node_id,
          .target_node_id = renewal.target_node_id};
}

[[nodiscard]] common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
encode_request(const DistributedVectorGroupedAggregateShuffleJobControlRequest& request) {
  if (const auto* prepare =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobPrepare>(&request)) {
    return encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(*prepare);
  }
  if (const auto* seal = std::get_if<DistributedVectorGroupedAggregateShuffleJobSeal>(&request))
    return encode_distributed_vector_grouped_aggregate_shuffle_job_seal_v1(*seal);
  if (const auto* routes =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobInstallRoutes>(&request)) {
    return encode_distributed_vector_grouped_aggregate_shuffle_job_install_routes_v2(*routes);
  }
  if (const auto* cancel = std::get_if<DistributedVectorGroupedAggregateShuffleJobCancel>(&request))
    return encode_distributed_vector_grouped_aggregate_shuffle_job_cancel_v3(*cancel);
  return encode_distributed_vector_grouped_aggregate_shuffle_job_renew_lease_v4(
      std::get<DistributedVectorGroupedAggregateShuffleJobRenewLease>(request));
}

} // namespace

class DistributedVectorGroupedAggregateShuffleJobControlTlsClient::Impl {
public:
  Impl(network::TlsSocket socket,
       DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor request_writer,
       DistributedVectorGroupedAggregateShuffleJobControlTlsClientConfig config,
       const TimePoint now)
      : socket_(std::move(socket)), request_writer_(std::move(request_writer)),
        config_(std::move(config)), expected_(correlation(config_.request)),
        deadline_(deadline_after(now, config_.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (state_ != DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kFailed) {
      failure_ = std::move(failure);
      state_ = DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kFailed;
      interest_ = {};
      response_.reset();
    }
    return failure_;
  }

  [[nodiscard]] common::Status authenticate(const TimePoint now) {
    auto fingerprint = socket_.peer_certificate_sha256();
    if (!fingerprint.has_value())
      return fail(fingerprint.error());
    const network::NetworkSecurityConfig security{.mode =
                                                      network::TransportSecurityMode::kTlsRequired,
                                                  .authenticator = config_.authenticator};
    auto authenticated =
        network::authenticate_peer(security, {.ipv4_address = config_.peer_ipv4_address,
                                              .transport_authenticated = true,
                                              .peer_certificate_sha256 = *fingerprint});
    if (!authenticated.has_value())
      return fail(authenticated.error());
    if (!authenticated->authorized || authenticated->principal_id == 0U) {
      return fail(status(common::StatusCode::kUnauthenticated,
                         "grouped shuffle reducer-job server principal is not authenticated"));
    }
    auto authorized = config_.node_authorizer->authorize_node(authenticated->principal_id,
                                                              expected_.target_node_id);
    if (!authorized.has_value())
      return fail(authorized.error());
    if (!*authorized) {
      return fail(status(common::StatusCode::kUnauthenticated,
                         "TLS principal cannot claim the grouped shuffle reducer-job target"));
    }
    state_ = DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kWritingRequest;
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
    if (progress->state == network::TlsIoState::kClosed) {
      return fail(status(common::StatusCode::kUnavailable,
                         "grouped shuffle reducer-job TLS client handshake closed"));
    }
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    return authenticate(now);
  }

  [[nodiscard]] common::Status write_request(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto progress = socket_.write(request_writer_.pending_write());
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed) {
      return fail(status(common::StatusCode::kUnavailable,
                         "grouped shuffle reducer-job request socket closed"));
    }
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U) {
      return fail(status(common::StatusCode::kUnavailable,
                         "grouped shuffle reducer-job request write made no progress"));
    }
    const common::Status consumed = request_writer_.consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (request_writer_.complete()) {
      state_ = DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kReadingResponse;
      interest_ = {.want_read = true};
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status read_response(const bool readable, const bool writable) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    auto progress = socket_.read(response_scratch_);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed) {
      return fail(status(common::StatusCode::kUnavailable,
                         "grouped shuffle reducer-job response socket closed"));
    }
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U) {
      return fail(status(common::StatusCode::kUnavailable,
                         "grouped shuffle reducer-job response read made no progress"));
    }
    auto step = response_reader_.consume(
        common::ByteView{response_scratch_}.first(progress->bytes_transferred));
    if (!step.has_value())
      return fail(step.error());
    if (step->consumed_bytes != progress->bytes_transferred) {
      return fail(status(common::StatusCode::kCorruption,
                         "grouped shuffle reducer-job response has a coalesced suffix"));
    }
    if (!step->response.has_value()) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    const auto& response = *step->response;
    if (response.action != expected_.action || response.query_id != expected_.query_id ||
        response.coordinator_node_id != expected_.coordinator_node_id ||
        response.target_node_id != expected_.target_node_id) {
      return fail(status(common::StatusCode::kCorruption,
                         "grouped shuffle reducer-job response correlation differs"));
    }
    response_ = response;
    state_ = DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kComplete;
    interest_ = {};
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor request_writer_;
  DistributedVectorGroupedAggregateShuffleJobControlResponseReader response_reader_;
  DistributedVectorGroupedAggregateShuffleJobControlTlsClientConfig config_;
  Correlation expected_;
  TimePoint deadline_;
  DistributedVectorGroupedAggregateShuffleJobControlTlsClientState state_{
      DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kHandshaking};
  DistributedVectorGroupedAggregateShuffleJobControlTlsInterest interest_{.want_write = true};
  std::array<std::byte, kResponseScratchBytes> response_scratch_{};
  std::optional<DistributedVectorGroupedAggregateShuffleJobControlResponse> response_;
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle reducer-job TLS client has not failed"};
};

DistributedVectorGroupedAggregateShuffleJobControlTlsClient::
    DistributedVectorGroupedAggregateShuffleJobControlTlsClient(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleJobControlTlsClient::
    ~DistributedVectorGroupedAggregateShuffleJobControlTlsClient() = default;
DistributedVectorGroupedAggregateShuffleJobControlTlsClient::
    DistributedVectorGroupedAggregateShuffleJobControlTlsClient(
        DistributedVectorGroupedAggregateShuffleJobControlTlsClient&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleJobControlTlsClient&
DistributedVectorGroupedAggregateShuffleJobControlTlsClient::operator=(
    DistributedVectorGroupedAggregateShuffleJobControlTlsClient&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleJobControlTlsClient>
DistributedVectorGroupedAggregateShuffleJobControlTlsClient::create(
    network::TlsSocket socket,
    DistributedVectorGroupedAggregateShuffleJobControlTlsClientConfig config, const TimePoint now) {
  const common::Status valid = validate_limits(config.limits);
  if (config.authenticator == nullptr || config.node_authorizer == nullptr || !valid.is_ok()) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "grouped shuffle reducer-job TLS client configuration is invalid"));
  }
  auto encoded = encode_request(config.request);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  auto writer = DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor::create(
      std::move(*encoded));
  try {
    return DistributedVectorGroupedAggregateShuffleJobControlTlsClient{
        std::make_unique<Impl>(std::move(socket), std::move(writer), std::move(config), now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted,
               "grouped shuffle reducer-job TLS client allocation failed"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleJobControlTlsClient::on_ready(
    const bool readable, const bool writable, const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "grouped shuffle reducer-job TLS client is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(status(
        common::StatusCode::kUnavailable,
        impl.state_ ==
                DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kHandshaking
            ? "grouped shuffle reducer-job TLS client handshake timed out"
            : "grouped shuffle reducer-job TLS client exchange timed out"));
  }
  if (impl.state_ ==
      DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kHandshaking) {
    return impl.handshake(readable, writable, now);
  }
  if (impl.state_ ==
      DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kWritingRequest) {
    return impl.write_request(readable, writable);
  }
  return impl.read_response(readable, writable);
}

DistributedVectorGroupedAggregateShuffleJobControlTlsClientState
DistributedVectorGroupedAggregateShuffleJobControlTlsClient::state() const noexcept {
  return implementation_
             ? implementation_->state_
             : DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kFailed;
}

DistributedVectorGroupedAggregateShuffleJobControlTlsInterest
DistributedVectorGroupedAggregateShuffleJobControlTlsClient::interest() const noexcept {
  return implementation_ ? implementation_->interest_
                         : DistributedVectorGroupedAggregateShuffleJobControlTlsInterest{};
}

DistributedVectorGroupedAggregateShuffleJobControlTlsClient::TimePoint
DistributedVectorGroupedAggregateShuffleJobControlTlsClient::deadline() const noexcept {
  return implementation_ ? implementation_->deadline_ : TimePoint{};
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
DistributedVectorGroupedAggregateShuffleJobControlTlsClient::result() const {
  if (!implementation_ ||
      implementation_->state_ !=
          DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kComplete ||
      !implementation_->response_.has_value()) {
    return common::make_unexpected(status(common::StatusCode::kUnavailable,
                                          "grouped shuffle reducer-job response is unavailable"));
  }
  return *implementation_->response_;
}

const common::Status&
DistributedVectorGroupedAggregateShuffleJobControlTlsClient::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle reducer-job TLS client is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

class DistributedVectorGroupedAggregateShuffleJobControlTlsServer::Impl {
public:
  Impl(network::TlsSocket socket,
       DistributedVectorGroupedAggregateShuffleJobControlRequestReader request_reader,
       DistributedVectorGroupedAggregateShuffleJobControlTlsServerConfig config,
       const TimePoint now)
      : socket_(std::move(socket)), request_reader_(std::move(request_reader)), config_(config),
        deadline_(deadline_after(now, config_.limits.handshake_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (state_ != DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kFailed) {
      failure_ = std::move(failure);
      state_ = DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kFailed;
      interest_ = {};
      response_writer_.reset();
    }
    return failure_;
  }

  [[nodiscard]] common::Status authenticate(const TimePoint now) {
    auto fingerprint = socket_.peer_certificate_sha256();
    if (!fingerprint.has_value())
      return fail(fingerprint.error());
    const network::NetworkSecurityConfig security{.mode =
                                                      network::TransportSecurityMode::kTlsRequired,
                                                  .authenticator = config_.authenticator};
    auto authenticated =
        network::authenticate_peer(security, {.ipv4_address = config_.peer_ipv4_address,
                                              .transport_authenticated = true,
                                              .peer_certificate_sha256 = *fingerprint});
    if (!authenticated.has_value())
      return fail(authenticated.error());
    if (!authenticated->authorized || authenticated->principal_id == 0U) {
      return fail(status(common::StatusCode::kUnauthenticated,
                         "grouped shuffle reducer-job client principal is not authenticated"));
    }
    authenticated_peer_ = *authenticated;
    state_ = DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kReadingRequest;
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
    if (progress->state == network::TlsIoState::kClosed) {
      return fail(status(common::StatusCode::kUnavailable,
                         "grouped shuffle reducer-job TLS server handshake closed"));
    }
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    return authenticate(now);
  }

  [[nodiscard]] common::Status
  accept_request(DistributedVectorGroupedAggregateShuffleJobControlRequest request,
                 const TimePoint now) {
    common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse> response =
        common::make_unexpected(status(common::StatusCode::kInternal,
                                       "grouped shuffle reducer-job service did not run"));
    try {
      response = config_.service->receive(std::move(request), *authenticated_peer_, now);
    } catch (const std::bad_alloc&) {
      return fail(status(common::StatusCode::kResourceExhausted,
                         "grouped shuffle reducer-job service allocation escaped"));
    } catch (...) {
      return fail(
          status(common::StatusCode::kInternal, "grouped shuffle reducer-job service threw"));
    }
    if (!response.has_value())
      return fail(response.error());
    common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse> encoded =
        response->action == DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes
            ? encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v2(*response)
        : response->action == DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel
            ? encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v3(*response)
        : response->action == DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease
            ? encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v4(*response)
            : encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(
                  *response);
    if (!encoded.has_value())
      return fail(encoded.error());
    response_writer_.emplace(
        DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor::create(
            std::move(*encoded)));
    state_ = DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kWritingResponse;
    interest_ = {.want_write = true};
    return common::Status::ok();
  }

  [[nodiscard]] common::Status read_request(const bool readable, const bool writable,
                                            const TimePoint now) {
    if ((!interest_.want_read || (!readable && socket_.pending_plaintext_bytes() == 0U)) &&
        (!interest_.want_write || !writable)) {
      return common::Status::ok();
    }
    auto progress = socket_.read(request_scratch_);
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed) {
      return fail(status(common::StatusCode::kUnavailable,
                         "grouped shuffle reducer-job request socket closed"));
    }
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U) {
      return fail(status(common::StatusCode::kUnavailable,
                         "grouped shuffle reducer-job request read made no progress"));
    }
    auto step = request_reader_.consume(
        common::ByteView{request_scratch_}.first(progress->bytes_transferred));
    if (!step.has_value())
      return fail(step.error());
    if (step->consumed_bytes != progress->bytes_transferred) {
      return fail(status(common::StatusCode::kCorruption,
                         "grouped shuffle reducer-job request has a coalesced suffix"));
    }
    if (!step->request.has_value()) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    return accept_request(std::move(*step->request), now);
  }

  [[nodiscard]] common::Status write_response(const bool readable, const bool writable) {
    if ((!interest_.want_read || !readable) && (!interest_.want_write || !writable))
      return common::Status::ok();
    auto* writer = response_writer_.transform([](auto& value) { return &value; }).value_or(nullptr);
    if (writer == nullptr) {
      return fail(status(common::StatusCode::kInternal,
                         "grouped shuffle reducer-job response writer is unavailable"));
    }
    auto progress = socket_.write(writer->pending_write());
    if (!progress.has_value())
      return fail(progress.error());
    if (progress->state == network::TlsIoState::kClosed) {
      return fail(status(common::StatusCode::kUnavailable,
                         "grouped shuffle reducer-job response socket closed"));
    }
    if (progress->state == network::TlsIoState::kWantRead) {
      interest_ = {.want_read = true};
      return common::Status::ok();
    }
    if (progress->state == network::TlsIoState::kWantWrite) {
      interest_ = {.want_write = true};
      return common::Status::ok();
    }
    if (progress->bytes_transferred == 0U) {
      return fail(status(common::StatusCode::kUnavailable,
                         "grouped shuffle reducer-job response write made no progress"));
    }
    const common::Status consumed = writer->consume_written(progress->bytes_transferred);
    if (!consumed.is_ok())
      return fail(consumed);
    if (writer->complete()) {
      state_ = DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kComplete;
      interest_ = {};
    } else {
      interest_ = {.want_write = true};
    }
    return common::Status::ok();
  }

  network::TlsSocket socket_;
  DistributedVectorGroupedAggregateShuffleJobControlRequestReader request_reader_;
  DistributedVectorGroupedAggregateShuffleJobControlTlsServerConfig config_;
  TimePoint deadline_;
  DistributedVectorGroupedAggregateShuffleJobControlTlsServerState state_{
      DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kHandshaking};
  DistributedVectorGroupedAggregateShuffleJobControlTlsInterest interest_{.want_read = true};
  std::optional<network::PeerAuthenticationResult> authenticated_peer_;
  std::array<std::byte, kRequestScratchBytes> request_scratch_{};
  std::optional<DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor>
      response_writer_;
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle reducer-job TLS server has not failed"};
};

DistributedVectorGroupedAggregateShuffleJobControlTlsServer::
    DistributedVectorGroupedAggregateShuffleJobControlTlsServer(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleJobControlTlsServer::
    ~DistributedVectorGroupedAggregateShuffleJobControlTlsServer() = default;
DistributedVectorGroupedAggregateShuffleJobControlTlsServer::
    DistributedVectorGroupedAggregateShuffleJobControlTlsServer(
        DistributedVectorGroupedAggregateShuffleJobControlTlsServer&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleJobControlTlsServer&
DistributedVectorGroupedAggregateShuffleJobControlTlsServer::operator=(
    DistributedVectorGroupedAggregateShuffleJobControlTlsServer&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleJobControlTlsServer>
DistributedVectorGroupedAggregateShuffleJobControlTlsServer::create(
    network::TlsSocket socket,
    DistributedVectorGroupedAggregateShuffleJobControlTlsServerConfig config, const TimePoint now) {
  const common::Status valid = validate_limits(config.limits);
  if (config.authenticator == nullptr || config.service == nullptr || !valid.is_ok()) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "grouped shuffle reducer-job TLS server configuration is invalid"));
  }
  auto reader = DistributedVectorGroupedAggregateShuffleJobControlRequestReader::create(
      config.limits.request);
  if (!reader.has_value())
    return common::make_unexpected(reader.error());
  try {
    return DistributedVectorGroupedAggregateShuffleJobControlTlsServer{
        std::make_unique<Impl>(std::move(socket), std::move(*reader), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted,
               "grouped shuffle reducer-job TLS server allocation failed"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleJobControlTlsServer::on_ready(
    const bool readable, const bool writable, const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "grouped shuffle reducer-job TLS server is empty");
  Impl& impl = *implementation_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kFailed)
    return impl.failure_;
  if (impl.state_ == DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kComplete)
    return common::Status::ok();
  if (now >= impl.deadline_) {
    return impl.fail(status(
        common::StatusCode::kUnavailable,
        impl.state_ ==
                DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kHandshaking
            ? "grouped shuffle reducer-job TLS server handshake timed out"
            : "grouped shuffle reducer-job TLS server exchange timed out"));
  }
  if (impl.state_ ==
      DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kHandshaking) {
    return impl.handshake(readable, writable, now);
  }
  if (impl.state_ ==
      DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kReadingRequest) {
    return impl.read_request(readable, writable, now);
  }
  return impl.write_response(readable, writable);
}

DistributedVectorGroupedAggregateShuffleJobControlTlsServerState
DistributedVectorGroupedAggregateShuffleJobControlTlsServer::state() const noexcept {
  return implementation_
             ? implementation_->state_
             : DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kFailed;
}

DistributedVectorGroupedAggregateShuffleJobControlTlsInterest
DistributedVectorGroupedAggregateShuffleJobControlTlsServer::interest() const noexcept {
  return implementation_ ? implementation_->interest_
                         : DistributedVectorGroupedAggregateShuffleJobControlTlsInterest{};
}

DistributedVectorGroupedAggregateShuffleJobControlTlsServer::TimePoint
DistributedVectorGroupedAggregateShuffleJobControlTlsServer::deadline() const noexcept {
  return implementation_ ? implementation_->deadline_ : TimePoint{};
}

const common::Status&
DistributedVectorGroupedAggregateShuffleJobControlTlsServer::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle reducer-job TLS server is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
