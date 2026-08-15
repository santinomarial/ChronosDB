#include "chronos/network/io_uring_reactor.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/subscription_messages.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <liburing.h>
#include <netinet/tcp.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#endif

namespace chronos::network {
namespace {

#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)
inline constexpr std::size_t kMaximumIoUringConnections = 32'766U;

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}
[[nodiscard]] common::Status io_error(std::string message) {
  return {common::StatusCode::kIoError, std::move(message)};
}
[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}
[[nodiscard]] common::Status not_supported(std::string message) {
  return {common::StatusCode::kNotSupported, std::move(message)};
}
#endif

} // namespace

#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)

class IoUringReactor::Impl {
public:
  enum class OperationKind : std::uint8_t { kFree, kAccept, kWake, kReceive, kSend };

  struct Operation {
    OperationKind kind{OperationKind::kFree};
    int socket{-1};
    sockaddr_in peer{};
    socklen_t peer_size{sizeof(sockaddr_in)};
    std::uint64_t wake_value{};
    std::size_t submitted_bytes{};
  };

  struct Connection {
    int socket{-1};
    std::uint64_t id{};
    std::uint64_t principal_id{};
    ConnectionBuffers buffers;
    ServerConnectionState state;
    std::vector<std::byte> read_buffer;
    std::chrono::steady_clock::time_point accepted_at;
    std::chrono::steady_clock::time_point last_progress;
    std::optional<std::size_t> pending_operation;
    bool close_after_write{};
    bool closing{};
    bool timed_out{};
  };

  Impl(EpollServerConfig configured, SpscNetworkTaskQueue& request_queue,
       SpscNetworkTaskQueue& response_queue, io_uring initialized_ring, int listener,
       int wake_descriptor, std::uint16_t actual_port, std::vector<Operation> operations,
       std::vector<std::size_t> free_operations) noexcept
      : config(std::move(configured)), requests(&request_queue), responses(&response_queue),
        ring(initialized_ring), listen_fd(listener), wake_fd(wake_descriptor), port(actual_port),
        operations_(std::move(operations)), free_operations_(std::move(free_operations)) {}

  ~Impl() {
    static_cast<void>(stop());
  }

  [[nodiscard]] common::Status stop() noexcept {
    if (!running)
      return common::Status::ok();
    running = false;
    // Closing the ring synchronously releases every registered request before any connection-owned
    // receive or send buffer is destroyed. The response producer is required to be joined first.
    if (ring_initialized) {
      io_uring_queue_exit(&ring);
      ring_initialized = false;
    }
    for (auto& [socket, connection] : connections) {
      connection.state.close();
      connection.buffers.clear();
      ::close(socket);
      ++stats.closed_connections;
    }
    connections.clear();
    stats.active_connections = 0U;
    if (listen_fd >= 0) {
      ::close(listen_fd);
      listen_fd = -1;
    }
    if (wake_fd >= 0) {
      ::close(wake_fd);
      wake_fd = -1;
    }
    return common::Status::ok();
  }

  [[nodiscard]] std::optional<std::size_t> take_operation() noexcept {
    if (free_operations_.empty())
      return std::nullopt;
    const std::size_t result = free_operations_.back();
    free_operations_.pop_back();
    return result;
  }

  void release_operation(const std::size_t index) noexcept {
    operations_[index] = Operation{};
    free_operations_.push_back(index);
  }

  [[nodiscard]] io_uring_sqe* acquire_sqe(const std::size_t operation) noexcept {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    if (sqe == nullptr) {
      free_operations_.push_back(operation);
      return nullptr;
    }
    return sqe;
  }

  void finalize_close(const int socket) noexcept {
    const auto found = connections.find(socket);
    if (found == connections.end() || found->second.pending_operation.has_value())
      return;
    found->second.buffers.clear();
    ::close(socket);
    if (found->second.timed_out)
      ++stats.timed_out_connections;
    connections.erase(found);
    ++stats.closed_connections;
    stats.active_connections = connections.size();
  }

  void begin_close(const int socket, const bool timed_out) noexcept {
    const auto found = connections.find(socket);
    if (found == connections.end())
      return;
    Connection& connection = found->second;
    if (!connection.closing) {
      for (const std::uint64_t request_id : connection.state.active_request_ids()) {
        Frame cancel{.header = {.message_type = MessageType::kCancel, .request_id = request_id},
                     .payload = {}};
        if (!requests->try_push(
                {.connection_id = connection.id,
                 .principal_id = connection.principal_id,
                 .protocol = {.protocol_major = connection.state.negotiated_major(),
                              .protocol_minor = connection.state.negotiated_minor(),
                              .feature_bits = connection.state.negotiated_feature_bits(),
                              .maximum_payload_size =
                                  connection.state.negotiated_maximum_payload_size()},
                 .frame = std::move(cancel)}))
          break;
      }
      connection.state.close();
      connection.closing = true;
      static_cast<void>(::shutdown(socket, SHUT_RDWR));
    }
    connection.timed_out = connection.timed_out || timed_out;
    finalize_close(socket);
  }

  [[nodiscard]] common::Status enqueue(Connection& connection, const MessageType type,
                                       const std::uint64_t request_id,
                                       const common::ByteView payload,
                                       const std::uint32_t flags = 0U) {
    const std::uint16_t major =
        type == MessageType::kServerHello ? kProtocolMajor : connection.state.negotiated_major();
    const std::uint16_t minor =
        type == MessageType::kServerHello ? 0U : connection.state.negotiated_minor();
    auto encoded = encode_frame({.protocol_major = major,
                                 .protocol_minor = minor,
                                 .message_type = type,
                                 .flags = flags,
                                 .request_id = request_id},
                                payload, config.buffers.protocol);
    if (!encoded.has_value())
      return encoded.error();
    return connection.buffers.enqueue(std::move(*encoded));
  }

  void enqueue_error(Connection& connection, const std::uint64_t request_id,
                     const ProtocolErrorCode code, const std::string_view message,
                     const bool close_after) {
    const int socket = connection.socket;
    auto payload = encode_error_message(code, message, config.buffers.protocol);
    if (!payload.has_value() ||
        !enqueue(connection, MessageType::kError, request_id,
                 payload.has_value() ? common::ByteView{*payload} : common::ByteView{})
             .is_ok()) {
      begin_close(socket, false);
      return;
    }
    connection.close_after_write = close_after;
  }

  void dispatch_frame(Connection& connection, Frame frame) {
    auto action = connection.state.accept(frame);
    if (!action.has_value()) {
      ++stats.protocol_errors;
      enqueue_error(connection, frame.header.request_id, ProtocolErrorCode::kInvalidState,
                    action.error().message(), true);
      return;
    }
    switch (action->kind) {
    case InboundActionKind::kHandshake: {
      auto hello =
          encode_server_hello({.selected_major = action->negotiated_major,
                               .selected_minor = action->negotiated_minor,
                               .feature_bits = action->negotiated_feature_bits,
                               .maximum_payload_size = action->negotiated_maximum_payload_size});
      if (!hello.has_value() ||
          !enqueue(connection, MessageType::kServerHello, 0U,
                   hello.has_value() ? common::ByteView{*hello} : common::ByteView{})
               .is_ok())
        begin_close(connection.socket, false);
      return;
    }
    case InboundActionKind::kPing:
      if (!enqueue(connection, MessageType::kPong, 0U, {}).is_ok())
        begin_close(connection.socket, false);
      return;
    case InboundActionKind::kCancel:
      if (!action->cancellation_was_active)
        return;
      break;
    case InboundActionKind::kIngest:
    case InboundActionKind::kQuery:
    case InboundActionKind::kSubscribe:
    case InboundActionKind::kSubscriptionAcknowledge:
      break;
    }
    if (!requests->try_push(
            {.connection_id = connection.id,
             .principal_id = connection.principal_id,
             .protocol = {.protocol_major = connection.state.negotiated_major(),
                          .protocol_minor = connection.state.negotiated_minor(),
                          .feature_bits = connection.state.negotiated_feature_bits(),
                          .maximum_payload_size =
                              connection.state.negotiated_maximum_payload_size()},
             .frame = std::move(frame)})) {
      ++stats.queue_overloads;
      static_cast<void>(connection.state.complete(action->request_id));
      enqueue_error(connection, action->request_id, ProtocolErrorCode::kOverloaded,
                    "reactor-to-shard queue is full", false);
      return;
    }
    ++stats.dispatched_requests;
  }

  void admit_socket(const int socket, const sockaddr_in& peer) {
    if (connections.size() == config.maximum_connections || next_connection_id == 0U) {
      ++stats.rejected_connections;
      ::close(socket);
      return;
    }
    const int no_delay = 1;
    if (::setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, &no_delay, sizeof(no_delay)) != 0) {
      ++stats.rejected_connections;
      ::close(socket);
      return;
    }
    const std::uint32_t peer_address = ntohl(peer.sin_addr.s_addr);
    const std::array<std::uint8_t, 4> peer_bytes{
        static_cast<std::uint8_t>((peer_address >> 24U) & 0xffU),
        static_cast<std::uint8_t>((peer_address >> 16U) & 0xffU),
        static_cast<std::uint8_t>((peer_address >> 8U) & 0xffU),
        static_cast<std::uint8_t>(peer_address & 0xffU)};
    auto authentication = authenticate_peer(
        config.security, {.ipv4_address = peer_bytes, .transport_authenticated = false});
    if (!authentication.has_value() || !authentication->authorized) {
      ++stats.authentication_rejections;
      ::close(socket);
      return;
    }
    auto buffers = ConnectionBuffers::create(config.buffers);
    auto state = ServerConnectionState::create(config.state);
    if (!buffers.has_value() || !state.has_value()) {
      ++stats.rejected_connections;
      ::close(socket);
      return;
    }
    const auto now = std::chrono::steady_clock::now();
    try {
      connections.emplace(socket,
                          Connection{.socket = socket,
                                     .id = next_connection_id++,
                                     .principal_id = authentication->principal_id,
                                     .buffers = std::move(*buffers),
                                     .state = std::move(*state),
                                     .read_buffer = std::vector<std::byte>(config.read_chunk_bytes),
                                     .accepted_at = now,
                                     .last_progress = now,
                                     .pending_operation = std::nullopt,
                                     .close_after_write = false,
                                     .closing = false,
                                     .timed_out = false});
    } catch (const std::bad_alloc&) {
      ++stats.rejected_connections;
      ::close(socket);
      return;
    }
    ++stats.accepted_connections;
    stats.active_connections = connections.size();
  }

  void drain_responses() {
    for (std::size_t count = 0U; count < config.maximum_events_per_poll; ++count) {
      auto task = responses->try_pop();
      if (!task.has_value())
        return;
      auto found = std::ranges::find_if(
          connections, [&](const auto& item) { return item.second.id == task->connection_id; });
      if (found == connections.end() || found->second.closing)
        continue;
      Frame& frame = task->frame;
      const bool ingest = frame.header.message_type == MessageType::kIngestAcknowledgement;
      const bool quorum_sync =
          frame.header.message_type == MessageType::kQuorumSyncIngestAcknowledgement;
      const bool leader_redirect = frame.header.message_type == MessageType::kLeaderRedirect;
      const bool terminal_error = frame.header.message_type == MessageType::kError;
      const SubscriptionMessageLimits subscription_limits{.protocol = config.buffers.protocol};
      const bool payload_is_valid =
          (frame.header.message_type == MessageType::kQueryResult &&
           decode_query_result_batch(frame.payload, {.protocol = config.buffers.protocol,
                                                     .maximum_rows = 1'048'576U,
                                                     .maximum_columns = 4096U,
                                                     .maximum_column_name_bytes = 1024U})
               .has_value()) ||
          (frame.header.message_type == MessageType::kQueryEnd && frame.payload.empty()) ||
          (ingest &&
           decode_ingest_acknowledgement(
               frame.payload, {.protocol_major = found->second.state.negotiated_major(),
                               .protocol_minor = found->second.state.negotiated_minor(),
                               .feature_bits = found->second.state.negotiated_feature_bits()})
               .has_value()) ||
          (quorum_sync && decode_quorum_sync_ingest_acknowledgement(frame.payload).has_value()) ||
          (leader_redirect && decode_leader_redirect(frame.payload).has_value()) ||
          (frame.header.message_type == MessageType::kSubscriptionReady &&
           decode_subscription_ready(frame.payload, subscription_limits).has_value()) ||
          (frame.header.message_type == MessageType::kSubscriptionChange &&
           decode_subscription_change(frame.payload, subscription_limits).has_value()) ||
          (frame.header.message_type == MessageType::kSubscriptionCheckpoint &&
           decode_subscription_checkpoint(frame.payload, subscription_limits).has_value()) ||
          (frame.header.message_type == MessageType::kSubscriptionEnd &&
           decode_subscription_end(frame.payload, subscription_limits).has_value()) ||
          (terminal_error &&
           decode_error_message(frame.payload, config.buffers.protocol).has_value());
      if (!payload_is_valid || !found->second.state.accept_response(frame).is_ok()) {
        ++stats.dropped_responses;
        continue;
      }
      auto encoded = encode_frame({.protocol_major = found->second.state.negotiated_major(),
                                   .protocol_minor = found->second.state.negotiated_minor(),
                                   .message_type = frame.header.message_type,
                                   .flags = frame.header.flags,
                                   .request_id = frame.header.request_id},
                                  frame.payload, config.buffers.protocol);
      if (!encoded.has_value() || !found->second.buffers.enqueue(std::move(*encoded)).is_ok())
        begin_close(found->first, false);
    }
  }

  [[nodiscard]] bool submit_accept() noexcept {
    if (accept_pending)
      return true;
    const std::optional<std::size_t> operation = take_operation();
    if (!operation.has_value())
      return false;
    io_uring_sqe* sqe = acquire_sqe(*operation);
    if (sqe == nullptr)
      return false;
    Operation& state = operations_[*operation];
    state.kind = OperationKind::kAccept;
    state.peer = {};
    state.peer_size = sizeof(sockaddr_in);
    io_uring_prep_accept(sqe, listen_fd,
                         reinterpret_cast<sockaddr*>(&state.peer), // NOLINT
                         &state.peer_size, SOCK_NONBLOCK | SOCK_CLOEXEC);
    io_uring_sqe_set_data64(sqe, *operation + 1U);
    accept_pending = true;
    return true;
  }

  [[nodiscard]] bool submit_wake() noexcept {
    if (wake_pending)
      return true;
    const std::optional<std::size_t> operation = take_operation();
    if (!operation.has_value())
      return false;
    io_uring_sqe* sqe = acquire_sqe(*operation);
    if (sqe == nullptr)
      return false;
    Operation& state = operations_[*operation];
    state.kind = OperationKind::kWake;
    state.wake_value = 0U;
    io_uring_prep_read(sqe, wake_fd, &state.wake_value, sizeof(state.wake_value), 0U);
    io_uring_sqe_set_data64(sqe, *operation + 1U);
    wake_pending = true;
    return true;
  }

  [[nodiscard]] bool submit_connection(Connection& connection) noexcept {
    if (connection.closing || connection.pending_operation.has_value())
      return true;
    const std::optional<std::size_t> operation = take_operation();
    if (!operation.has_value())
      return false;
    io_uring_sqe* sqe = acquire_sqe(*operation);
    if (sqe == nullptr)
      return false;
    Operation& state = operations_[*operation];
    state.socket = connection.socket;
    const common::ByteView pending = connection.buffers.pending_write();
    if (pending.empty()) {
      state.kind = OperationKind::kReceive;
      io_uring_prep_recv(sqe, connection.socket, connection.read_buffer.data(),
                         connection.read_buffer.size(), 0);
    } else {
      state.kind = OperationKind::kSend;
      state.submitted_bytes = pending.size();
      io_uring_prep_send(sqe, connection.socket, pending.data(), pending.size(), MSG_NOSIGNAL);
    }
    io_uring_sqe_set_data64(sqe, *operation + 1U);
    connection.pending_operation = *operation;
    return true;
  }

  [[nodiscard]] common::Status submit_operations() {
    static_cast<void>(submit_accept());
    static_cast<void>(submit_wake());
    for (auto iterator = connections.begin(); iterator != connections.end();) {
      const int socket = iterator->first;
      Connection& connection = iterator->second;
      ++iterator;
      if (connection.closing) {
        finalize_close(socket);
        continue;
      }
      if (connection.close_after_write && connection.buffers.pending_write().empty()) {
        begin_close(socket, false);
        continue;
      }
      if (!submit_connection(connection))
        break;
    }
    while (io_uring_sq_ready(&ring) != 0U) {
      const int submitted = io_uring_submit(&ring);
      if (submitted <= 0)
        return io_error("io_uring socket operation submission failed");
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status complete_accept(const std::size_t operation, const int result) {
    const sockaddr_in peer = operations_[operation].peer;
    accept_pending = false;
    release_operation(operation);
    if (result >= 0) {
      admit_socket(result, peer);
      return common::Status::ok();
    }
    if (result == -EAGAIN || result == -EINTR || result == -ECONNABORTED || result == -EPROTO)
      return common::Status::ok();
    if (result == -EMFILE || result == -ENFILE)
      return exhausted("io_uring accept exhausted process or system descriptors");
    return io_error("io_uring accept operation failed");
  }

  [[nodiscard]] common::Status complete_wake(const std::size_t operation, const int result) {
    wake_pending = false;
    release_operation(operation);
    if (result == static_cast<int>(sizeof(std::uint64_t))) {
      ++stats.response_wakeups;
      drain_responses();
      return common::Status::ok();
    }
    if (result == -EAGAIN || result == -EINTR)
      return common::Status::ok();
    return io_error("io_uring response wakeup operation failed");
  }

  void complete_receive(const std::size_t operation, const int result) {
    const int socket = operations_[operation].socket;
    auto found = connections.find(socket);
    if (found != connections.end() && found->second.pending_operation == operation)
      found->second.pending_operation.reset();
    release_operation(operation);
    found = connections.find(socket);
    if (found == connections.end())
      return;
    if (found->second.closing) {
      finalize_close(socket);
      return;
    }
    if (result <= 0) {
      if (result == -EAGAIN || result == -EINTR)
        return;
      begin_close(socket, false);
      return;
    }
    Connection& connection = found->second;
    if (static_cast<std::size_t>(result) > connection.read_buffer.size()) {
      begin_close(socket, false);
      return;
    }
    connection.last_progress = std::chrono::steady_clock::now();
    stats.bytes_read += static_cast<std::uint64_t>(result);
    auto frames = connection.buffers.receive(
        common::ByteView{connection.read_buffer}.first(static_cast<std::size_t>(result)));
    if (!frames.has_value()) {
      ++stats.protocol_errors;
      begin_close(socket, false);
      return;
    }
    stats.decoded_frames += frames->size();
    for (Frame& frame : *frames) {
      found = connections.find(socket);
      if (found == connections.end() || found->second.closing)
        return;
      dispatch_frame(found->second, std::move(frame));
    }
  }

  void complete_send(const std::size_t operation, const int result) {
    const int socket = operations_[operation].socket;
    const std::size_t submitted_bytes = operations_[operation].submitted_bytes;
    auto found = connections.find(socket);
    if (found != connections.end() && found->second.pending_operation == operation)
      found->second.pending_operation.reset();
    release_operation(operation);
    found = connections.find(socket);
    if (found == connections.end())
      return;
    if (found->second.closing) {
      finalize_close(socket);
      return;
    }
    if (result <= 0 || static_cast<std::size_t>(result) > submitted_bytes ||
        !found->second.buffers.consume_written(static_cast<std::size_t>(result)).is_ok()) {
      if (result == -EAGAIN || result == -EINTR)
        return;
      begin_close(socket, false);
      return;
    }
    found->second.last_progress = std::chrono::steady_clock::now();
    stats.bytes_written += static_cast<std::uint64_t>(result);
    if (found->second.close_after_write && found->second.buffers.pending_write().empty())
      begin_close(socket, false);
  }

  [[nodiscard]] common::Status complete(io_uring_cqe& completion) {
    const std::uint64_t identity = io_uring_cqe_get_data64(&completion);
    if (identity == 0U || identity > operations_.size())
      return io_error("io_uring returned an unknown operation identity");
    const std::size_t operation = static_cast<std::size_t>(identity - 1U);
    switch (operations_[operation].kind) {
    case OperationKind::kAccept:
      return complete_accept(operation, completion.res);
    case OperationKind::kWake:
      return complete_wake(operation, completion.res);
    case OperationKind::kReceive:
      complete_receive(operation, completion.res);
      break;
    case OperationKind::kSend:
      complete_send(operation, completion.res);
      break;
    case OperationKind::kFree:
      return io_error("io_uring completed an operation slot that was already free");
    }
    return common::Status::ok();
  }

  void expire_connections() noexcept {
    const auto now = std::chrono::steady_clock::now();
    for (auto iterator = connections.begin(); iterator != connections.end();) {
      const int socket = iterator->first;
      const Connection& connection = iterator->second;
      const bool expired =
          !connection.closing && ((connection.state.phase() == ConnectionPhase::kAwaitingHello &&
                                   now - connection.accepted_at >= config.handshake_timeout) ||
                                  now - connection.last_progress >= config.idle_timeout);
      ++iterator;
      if (expired)
        begin_close(socket, true);
    }
  }

  EpollServerConfig config;
  SpscNetworkTaskQueue* requests;
  SpscNetworkTaskQueue* responses;
  io_uring ring{};
  int listen_fd{-1};
  int wake_fd{-1};
  std::uint16_t port{};
  std::vector<Operation> operations_;
  std::vector<std::size_t> free_operations_;
  std::unordered_map<int, Connection> connections;
  std::uint64_t next_connection_id{1U};
  EpollServerMetrics stats;
  bool ring_initialized{true};
  bool accept_pending{};
  bool wake_pending{};
  bool running{true};
};

#else
class IoUringReactor::Impl {};
#endif

IoUringReactor::IoUringReactor() noexcept = default;
IoUringReactor::IoUringReactor(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
IoUringReactor::~IoUringReactor() = default;
IoUringReactor::IoUringReactor(IoUringReactor&&) noexcept = default;
IoUringReactor& IoUringReactor::operator=(IoUringReactor&&) noexcept = default;

common::Result<IoUringReactor> IoUringReactor::start(const EpollServerConfig& config,
                                                     const EpollReactorQueues& queues) {
#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)
  if (queues.requests == nullptr || queues.responses == nullptr ||
      config.maximum_connections == 0U || config.maximum_connections > kMaximumIoUringConnections ||
      config.maximum_events_per_poll == 0U || config.maximum_io_operations_per_event == 0U ||
      config.maximum_io_operations_per_event > 1024U || config.read_chunk_bytes == 0U ||
      config.read_chunk_bytes > std::size_t{1U} * 1024U * 1024U || config.listen_backlog <= 0 ||
      config.handshake_timeout.count() <= 0 || config.idle_timeout.count() <= 0 ||
      config.buffers.protocol.maximum_payload_size != config.state.limits.maximum_payload_size)
    return common::make_unexpected(invalid("io_uring server configuration is invalid"));
  if (const common::Status status =
          validate_network_security_config(config.security, config.bind_address);
      !status.is_ok())
    return common::make_unexpected(status);
  if (config.security.mode == TransportSecurityMode::kTlsRequired)
    return common::make_unexpected(not_supported("io_uring TLS record scheduling is unavailable"));
  const std::optional<std::size_t> operation_count =
      common::checked_add(config.maximum_connections, std::size_t{2U});
  if (!operation_count.has_value() ||
      *operation_count > static_cast<std::size_t>(std::numeric_limits<unsigned>::max()))
    return common::make_unexpected(exhausted("io_uring operation count overflowed"));

  io_uring ring{};
  const int initialized = io_uring_queue_init(static_cast<unsigned>(*operation_count), &ring, 0U);
  if (initialized < 0) {
    return common::make_unexpected(
        initialized == -ENOSYS || initialized == -EINVAL || initialized == -EPERM
            ? not_supported("io_uring is unavailable on this kernel or host policy")
            : io_error("io_uring initialization failed"));
  }
  io_uring_probe* probe = io_uring_get_probe_ring(&ring);
  const bool supported = probe != nullptr && io_uring_opcode_supported(probe, IORING_OP_ACCEPT) &&
                         io_uring_opcode_supported(probe, IORING_OP_RECV) &&
                         io_uring_opcode_supported(probe, IORING_OP_SEND) &&
                         io_uring_opcode_supported(probe, IORING_OP_READ);
  io_uring_free_probe(probe);
  if (!supported) {
    io_uring_queue_exit(&ring);
    return common::make_unexpected(
        not_supported("io_uring required socket operations are unavailable"));
  }

  int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (listener < 0) {
    io_uring_queue_exit(&ring);
    return common::make_unexpected(io_error("io_uring listener socket failed"));
  }
  const int reuse = 1;
  static_cast<void>(::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(config.port);
  std::uint32_t raw_address{};
  for (const std::uint8_t byte : config.bind_address)
    raw_address = (raw_address << 8U) | byte;
  address.sin_addr.s_addr = htonl(raw_address);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
      ::listen(listener, config.listen_backlog) != 0) {
    ::close(listener);
    io_uring_queue_exit(&ring);
    return common::make_unexpected(io_error("io_uring bind/listen failed"));
  }
  socklen_t address_size = sizeof(address);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_size) != 0) {
    ::close(listener);
    io_uring_queue_exit(&ring);
    return common::make_unexpected(io_error("io_uring getsockname failed"));
  }
  const int wake_fd = ::eventfd(0U, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wake_fd < 0) {
    ::close(listener);
    io_uring_queue_exit(&ring);
    return common::make_unexpected(io_error("io_uring eventfd failed"));
  }
  std::unique_ptr<Impl> implementation;
  try {
    std::vector<Impl::Operation> operations(*operation_count);
    std::vector<std::size_t> free_operations;
    free_operations.reserve(*operation_count);
    for (std::size_t index = *operation_count; index != 0U; --index)
      free_operations.push_back(index - 1U);
    implementation = std::make_unique<Impl>(config, *queues.requests, *queues.responses, ring,
                                            listener, wake_fd, ntohs(address.sin_port),
                                            std::move(operations), std::move(free_operations));
    implementation->connections.reserve(config.maximum_connections);
    return IoUringReactor{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    if (implementation == nullptr) {
      ::close(wake_fd);
      ::close(listener);
      io_uring_queue_exit(&ring);
    }
    return common::make_unexpected(exhausted("io_uring reactor allocation failed"));
  } catch (const std::length_error&) {
    if (implementation == nullptr) {
      ::close(wake_fd);
      ::close(listener);
      io_uring_queue_exit(&ring);
    }
    return common::make_unexpected(exhausted("io_uring reactor exceeds container limits"));
  }
#else
  static_cast<void>(config);
  static_cast<void>(queues);
  return common::make_unexpected(common::Status{
      common::StatusCode::kNotSupported, "io_uring reactor requires a Linux liburing build"});
#endif
}

// The portable backend interface is intentionally instance-bound even when this build has no
// liburing implementation to inspect.
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
common::Status IoUringReactor::poll_once(const std::chrono::milliseconds maximum_wait) {
#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)
  if (!implementation_ || !implementation_->running)
    return invalid("io_uring reactor is not running");
  if (maximum_wait.count() < 0 || maximum_wait.count() > std::numeric_limits<std::int32_t>::max())
    return invalid("io_uring wait duration is invalid");
  implementation_->drain_responses();
  if (const common::Status submitted = implementation_->submit_operations(); !submitted.is_ok())
    return submitted;
  __kernel_timespec timeout{maximum_wait.count() / 1000, (maximum_wait.count() % 1000) * 1'000'000};
  io_uring_cqe* completion = nullptr;
  const int waited = io_uring_wait_cqe_timeout(&implementation_->ring, &completion, &timeout);
  if (waited == -ETIME || waited == -EINTR) {
    implementation_->expire_connections();
    return common::Status::ok();
  }
  if (waited < 0 || completion == nullptr)
    return io_error("io_uring completion wait failed");
  for (std::size_t count = 0U; count < implementation_->config.maximum_events_per_poll; ++count) {
    const common::Status status = implementation_->complete(*completion);
    io_uring_cqe_seen(&implementation_->ring, completion);
    if (!status.is_ok())
      return status;
    completion = nullptr;
    if (io_uring_peek_cqe(&implementation_->ring, &completion) != 0 || completion == nullptr)
      break;
  }
  implementation_->expire_connections();
  return common::Status::ok();
#else
  static_cast<void>(maximum_wait);
  return common::Status{common::StatusCode::kNotSupported,
                        "io_uring reactor requires a Linux liburing build"};
#endif
}

// The portable backend interface is intentionally instance-bound on unsupported builds.
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
common::Status IoUringReactor::notify_response_ready() noexcept {
#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)
  if (!implementation_ || implementation_->wake_fd < 0)
    return invalid("io_uring reactor is not running");
  constexpr std::uint64_t kSignal = 1U;
  for (;;) {
    const ssize_t written = ::write(implementation_->wake_fd, &kSignal, sizeof(kSignal));
    if (written == static_cast<ssize_t>(sizeof(kSignal)))
      return common::Status::ok();
    if (written < 0 && errno == EINTR)
      continue;
    if (written < 0 && errno == EAGAIN)
      return common::Status::ok();
    return io_error("io_uring eventfd write failed");
  }
#else
  return common::Status{common::StatusCode::kNotSupported,
                        "io_uring reactor requires a Linux liburing build"};
#endif
}

// The portable backend interface is intentionally instance-bound on unsupported builds.
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
common::Status IoUringReactor::shutdown() noexcept {
#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)
  return implementation_ ? implementation_->stop() : common::Status::ok();
#else
  return common::Status::ok();
#endif
}

// The portable backend interface is intentionally instance-bound on unsupported builds.
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::uint16_t IoUringReactor::bound_port() const noexcept {
#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)
  return implementation_ ? implementation_->port : 0U;
#else
  return 0U;
#endif
}

// The portable backend interface is intentionally instance-bound on unsupported builds.
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
EpollServerMetrics IoUringReactor::metrics() const noexcept {
#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)
  return implementation_ ? implementation_->stats : EpollServerMetrics{};
#else
  return {};
#endif
}

// The portable backend interface is intentionally instance-bound on unsupported builds.
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
bool IoUringReactor::is_running() const noexcept {
#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)
  return implementation_ && implementation_->running;
#else
  return false;
#endif
}

} // namespace chronos::network
