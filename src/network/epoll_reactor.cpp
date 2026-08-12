#include "chronos/network/epoll_reactor.hpp"

#include "chronos/network/messages.hpp"
#include "chronos/network/subscription_messages.hpp"

#include <algorithm>
#include <limits>
#include <new>
#include <optional>
#include <string>
#include <utility>

#if defined(__linux__)
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#endif

namespace chronos::network {
namespace {

#if defined(__linux__)
[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}
[[nodiscard]] common::Status io_error(std::string message) {
  return {common::StatusCode::kIoError, std::move(message)};
}
[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}
#endif

} // namespace

#if defined(__linux__)

class EpollReactor::Impl {
public:
  struct Connection {
    int socket{-1};
    std::uint64_t id{};
    std::uint64_t principal_id{};
    std::array<std::uint8_t, 4> peer_address{};
    ConnectionBuffers buffers;
    ServerConnectionState state;
    std::optional<TlsSocket> tls;
    std::chrono::steady_clock::time_point accepted_at;
    std::chrono::steady_clock::time_point last_progress;
    bool authenticated{};
    bool handshake_wants_write{};
    bool read_wants_write{};
    bool write_wants_read{};
    bool buffered_tls_plaintext{};
    bool close_after_write{};
  };

  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
  Impl(EpollServerConfig configured, SpscNetworkTaskQueue& request_queue,
       SpscNetworkTaskQueue& response_queue, int epoll_descriptor, int listener,
       int wake_descriptor, std::uint16_t actual_port, std::vector<epoll_event> events,
       std::vector<std::byte> scratch, std::optional<TlsServerContext> tls_server_context) noexcept
      : config(std::move(configured)), requests(&request_queue), responses(&response_queue),
        epoll_fd(epoll_descriptor), listen_fd(listener), wake_fd(wake_descriptor),
        port(actual_port), events_(std::move(events)), scratch_(std::move(scratch)),
        tls_context(std::move(tls_server_context)) {}

  ~Impl() {
    static_cast<void>(stop());
  }

  [[nodiscard]] common::Status stop() noexcept {
    if (!running)
      return common::Status::ok();
    while (!connections.empty())
      close_connection(connections.begin()->first, false);
    if (listen_fd >= 0) {
      ::close(listen_fd);
      listen_fd = -1;
    }
    if (wake_fd >= 0) {
      ::close(wake_fd);
      wake_fd = -1;
    }
    if (epoll_fd >= 0) {
      ::close(epoll_fd);
      epoll_fd = -1;
    }
    running = false;
    return common::Status::ok();
  }

  void update_interest(Connection& connection) {
    epoll_event event{};
    event.events = EPOLLIN | EPOLLRDHUP;
    const bool application_write_ready =
        !connection.buffers.pending_write().empty() &&
        (!connection.tls.has_value() || !connection.write_wants_read);
    if (connection.handshake_wants_write || connection.read_wants_write || application_write_ready)
      event.events |= EPOLLOUT;
    event.data.fd = connection.socket;
    static_cast<void>(::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, connection.socket, &event));
  }

  void close_connection(const int fd, const bool timed_out) noexcept {
    const auto found = connections.find(fd);
    if (found == connections.end())
      return;
    Connection& connection = found->second;
    for (const std::uint64_t request_id : connection.state.active_request_ids()) {
      Frame cancel{.header = {.message_type = MessageType::kCancel, .request_id = request_id},
                   .payload = {}};
      if (!requests->try_push({.connection_id = connection.id,
                               .principal_id = connection.principal_id,
                               .frame = std::move(cancel)}))
        break;
    }
    connection.state.close();
    connection.buffers.clear();
    static_cast<void>(::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr));
    connections.erase(found);
    ::close(fd);
    ++stats.closed_connections;
    if (timed_out)
      ++stats.timed_out_connections;
    stats.active_connections = connections.size();
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
    const common::Status status = connection.buffers.enqueue(std::move(*encoded));
    if (status.is_ok())
      update_interest(connection);
    return status;
  }

  void enqueue_error(Connection& connection, const std::uint64_t request_id,
                     const ProtocolErrorCode code, const std::string_view message,
                     const bool close_after) {
    auto payload = encode_error_message(code, message, config.buffers.protocol);
    if (!payload.has_value()) {
      close_connection(connection.socket, false);
      return;
    }
    if (!enqueue(connection, MessageType::kError, request_id, *payload).is_ok()) {
      close_connection(connection.socket, false);
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
        close_connection(connection.socket, false);
      return;
    }
    case InboundActionKind::kPing:
      if (!enqueue(connection, MessageType::kPong, 0U, {}).is_ok())
        close_connection(connection.socket, false);
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
    if (!requests->try_push({.connection_id = connection.id,
                             .principal_id = connection.principal_id,
                             .frame = std::move(frame)})) {
      ++stats.queue_overloads;
      static_cast<void>(connection.state.complete(action->request_id));
      enqueue_error(connection, action->request_id, ProtocolErrorCode::kOverloaded,
                    "reactor-to-shard queue is full", false);
      return;
    }
    ++stats.dispatched_requests;
  }

  [[nodiscard]] bool advance_tls_handshake(const int fd) {
    auto found = connections.find(fd);
    if (found == connections.end())
      return false;
    Connection& connection = found->second;
    if (!connection.tls.has_value() || connection.authenticated)
      return true;

    auto progress = connection.tls->handshake();
    if (!progress.has_value() || progress->state == TlsIoState::kClosed) {
      ++stats.authentication_rejections;
      close_connection(fd, false);
      return false;
    }
    connection.handshake_wants_write = progress->state == TlsIoState::kWantWrite;
    if (progress->state != TlsIoState::kComplete) {
      update_interest(connection);
      return false;
    }
    auto fingerprint = connection.tls->peer_certificate_sha256();
    if (!fingerprint.has_value()) {
      ++stats.authentication_rejections;
      close_connection(fd, false);
      return false;
    }
    auto authentication =
        authenticate_peer(config.security, {.ipv4_address = connection.peer_address,
                                            .transport_authenticated = true,
                                            .peer_certificate_sha256 = *fingerprint});
    if (!authentication.has_value() || !authentication->authorized) {
      ++stats.authentication_rejections;
      close_connection(fd, false);
      return false;
    }
    connection.principal_id = authentication->principal_id;
    connection.authenticated = true;
    connection.handshake_wants_write = false;
    connection.buffered_tls_plaintext = connection.tls->pending_plaintext_bytes() != 0U;
    connection.last_progress = std::chrono::steady_clock::now();
    ++stats.accepted_connections;
    update_interest(connection);
    return true;
  }

  void accept_ready() {
    for (std::size_t accepted = 0U; accepted < config.maximum_io_operations_per_event; ++accepted) {
      sockaddr_in peer{};
      socklen_t peer_size = sizeof(peer);
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
      const int fd = ::accept4(listen_fd, reinterpret_cast<sockaddr*>(&peer), &peer_size,
                               SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (fd < 0) {
        if (errno == EAGAIN)
          return;
        return;
      }
      if (connections.size() == config.maximum_connections || next_connection_id == 0U) {
        ++stats.rejected_connections;
        ::close(fd);
        continue;
      }
      const int no_delay = 1;
      if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &no_delay, sizeof(no_delay)) != 0) {
        ++stats.rejected_connections;
        ::close(fd);
        continue;
      }
      const std::uint32_t peer_address = ntohl(peer.sin_addr.s_addr);
      const std::array<std::uint8_t, 4> peer_bytes{
          static_cast<std::uint8_t>((peer_address >> 24U) & 0xffU),
          static_cast<std::uint8_t>((peer_address >> 16U) & 0xffU),
          static_cast<std::uint8_t>((peer_address >> 8U) & 0xffU),
          static_cast<std::uint8_t>(peer_address & 0xffU)};
      std::uint64_t principal_id{};
      bool authenticated{};
      std::optional<TlsSocket> tls;
      if (config.security.mode == TransportSecurityMode::kTlsRequired) {
        if (!tls_context.has_value()) {
          ++stats.rejected_connections;
          ::close(fd);
          continue;
        }
        auto session = TlsSocket::accept(*tls_context, fd);
        if (!session.has_value()) {
          ++stats.rejected_connections;
          ::close(fd);
          continue;
        }
        tls.emplace(std::move(*session));
      } else {
        auto authentication =
            authenticate_peer(config.security, {.ipv4_address = peer_bytes,
                                                .transport_authenticated = false,
                                                .peer_certificate_sha256 = std::nullopt});
        if (!authentication.has_value() || !authentication->authorized) {
          ++stats.authentication_rejections;
          ::close(fd);
          continue;
        }
        principal_id = authentication->principal_id;
        authenticated = true;
      }
      auto buffers = ConnectionBuffers::create(config.buffers);
      auto state = ServerConnectionState::create(config.state);
      if (!buffers.has_value() || !state.has_value()) {
        ++stats.rejected_connections;
        tls.reset();
        ::close(fd);
        continue;
      }
      epoll_event event{};
      event.events = EPOLLIN | EPOLLRDHUP;
      event.data.fd = fd;
      if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) != 0) {
        tls.reset();
        ::close(fd);
        continue;
      }
      const auto now = std::chrono::steady_clock::now();
      try {
        connections.emplace(fd, Connection{.socket = fd,
                                           .id = next_connection_id++,
                                           .principal_id = principal_id,
                                           .peer_address = peer_bytes,
                                           .buffers = std::move(*buffers),
                                           .state = std::move(*state),
                                           .tls = std::move(tls),
                                           .accepted_at = now,
                                           .last_progress = now,
                                           .authenticated = authenticated});
      } catch (const std::bad_alloc&) {
        static_cast<void>(::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr));
        ::close(fd);
        ++stats.rejected_connections;
        continue;
      }
      if (authenticated)
        ++stats.accepted_connections;
      stats.active_connections = connections.size();
    }
  }

  void read_ready(const int fd) {
    auto found = connections.find(fd);
    if (found == connections.end())
      return;
    for (std::size_t operation = 0U; operation < config.maximum_io_operations_per_event;
         ++operation) {
      found = connections.find(fd);
      if (found == connections.end())
        return;
      Connection& connection = found->second;
      std::size_t count{};
      if (connection.tls.has_value()) {
        auto received = connection.tls->read(common::MutableByteView{scratch_});
        if (!received.has_value() || received->state == TlsIoState::kClosed) {
          close_connection(fd, false);
          return;
        }
        connection.buffered_tls_plaintext = connection.tls->pending_plaintext_bytes() != 0U;
        connection.read_wants_write = received->state == TlsIoState::kWantWrite;
        if (received->state != TlsIoState::kComplete) {
          update_interest(connection);
          return;
        }
        count = received->bytes_transferred;
      } else {
        const ssize_t received = ::recv(fd, scratch_.data(), scratch_.size(), 0);
        if (received == 0) {
          close_connection(fd, false);
          return;
        }
        if (received < 0) {
          if (errno == EAGAIN)
            return;
          close_connection(fd, false);
          return;
        }
        count = static_cast<std::size_t>(received);
      }
      if (count == 0U) {
        close_connection(fd, false);
        return;
      }
      connection.read_wants_write = false;
      connection.last_progress = std::chrono::steady_clock::now();
      stats.bytes_read += count;
      auto frames = connection.buffers.receive(common::ByteView{scratch_}.first(count));
      if (!frames.has_value()) {
        ++stats.protocol_errors;
        close_connection(fd, false);
        return;
      }
      stats.decoded_frames += frames->size();
      for (Frame& frame : *frames) {
        found = connections.find(fd);
        if (found == connections.end())
          return;
        dispatch_frame(found->second, std::move(frame));
      }
      found = connections.find(fd);
      if (found == connections.end())
        return;
      if (found->second.tls.has_value()) {
        if (!found->second.buffered_tls_plaintext)
          return;
      } else if (count < scratch_.size()) {
        return;
      }
    }
  }

  void write_ready(const int fd) {
    auto found = connections.find(fd);
    if (found == connections.end())
      return;
    Connection& connection = found->second;
    for (std::size_t operation = 0U; operation < config.maximum_io_operations_per_event &&
                                     !connection.buffers.pending_write().empty();
         ++operation) {
      const common::ByteView pending = connection.buffers.pending_write();
      std::size_t count{};
      if (connection.tls.has_value()) {
        auto written = connection.tls->write(pending);
        if (!written.has_value() || written->state == TlsIoState::kClosed) {
          close_connection(fd, false);
          return;
        }
        connection.write_wants_read = written->state == TlsIoState::kWantRead;
        if (written->state != TlsIoState::kComplete) {
          update_interest(connection);
          return;
        }
        count = written->bytes_transferred;
      } else {
        const ssize_t written = ::send(fd, pending.data(), pending.size(), MSG_NOSIGNAL);
        if (written < 0) {
          if (errno == EAGAIN) {
            update_interest(connection);
            return;
          }
          close_connection(fd, false);
          return;
        }
        count = static_cast<std::size_t>(written);
      }
      if (count == 0U) {
        close_connection(fd, false);
        return;
      }
      connection.write_wants_read = false;
      connection.last_progress = std::chrono::steady_clock::now();
      stats.bytes_written += count;
      static_cast<void>(connection.buffers.consume_written(count));
    }
    if (connection.close_after_write && connection.buffers.pending_write().empty()) {
      close_connection(fd, false);
      return;
    }
    update_interest(connection);
  }

  void service_ready(const int fd, const bool readable, const bool writable) {
    auto found = connections.find(fd);
    if (found == connections.end())
      return;
    if (found->second.tls.has_value() && !found->second.authenticated && !advance_tls_handshake(fd))
      return;
    found = connections.find(fd);
    if (found == connections.end())
      return;
    bool write_attempted = false;
    if (readable && found->second.write_wants_read) {
      write_ready(fd);
      write_attempted = true;
    }
    found = connections.find(fd);
    if (found == connections.end())
      return;
    if (write_attempted && found->second.write_wants_read)
      return;
    if (found->second.buffered_tls_plaintext || readable ||
        (writable && found->second.read_wants_write))
      read_ready(fd);
    found = connections.find(fd);
    if (found == connections.end())
      return;
    if (found->second.read_wants_write)
      return;
    if (writable && !write_attempted)
      write_ready(fd);
  }

  [[nodiscard]] bool drain_buffered_tls_plaintext() {
    bool serviced = false;
    std::size_t serviced_connections{};
    for (auto iterator = connections.begin();
         iterator != connections.end() && serviced_connections < config.maximum_events_per_poll;) {
      const int fd = iterator->first;
      const bool pending = iterator->second.buffered_tls_plaintext;
      ++iterator;
      if (!pending)
        continue;
      read_ready(fd);
      serviced = true;
      ++serviced_connections;
    }
    return serviced;
  }

  void drain_responses() {
    for (std::size_t count = 0U; count < config.maximum_events_per_poll; ++count) {
      auto task = responses->try_pop();
      if (!task.has_value())
        return;
      auto found = std::ranges::find_if(
          connections, [&](const auto& item) { return item.second.id == task->connection_id; });
      if (found == connections.end())
        continue;
      Frame& frame = task->frame;
      const bool is_ingest_response =
          frame.header.message_type == MessageType::kIngestAcknowledgement;
      const bool is_quorum_sync_response =
          frame.header.message_type == MessageType::kQuorumSyncIngestAcknowledgement;
      const bool is_terminal_error = frame.header.message_type == MessageType::kError;
      const SubscriptionMessageLimits subscription_limits{.protocol = config.buffers.protocol};
      const bool payload_is_valid =
          (frame.header.message_type == MessageType::kQueryResult &&
           decode_query_result_batch(frame.payload, {.protocol = config.buffers.protocol,
                                                     .maximum_rows = 1'048'576U,
                                                     .maximum_columns = 4096U,
                                                     .maximum_column_name_bytes = 1024U})
               .has_value()) ||
          (frame.header.message_type == MessageType::kQueryEnd && frame.payload.empty()) ||
          (is_ingest_response &&
           decode_ingest_acknowledgement(
               frame.payload, {.protocol_major = found->second.state.negotiated_major(),
                               .protocol_minor = found->second.state.negotiated_minor(),
                               .feature_bits = found->second.state.negotiated_feature_bits()})
               .has_value()) ||
          (is_quorum_sync_response &&
           decode_quorum_sync_ingest_acknowledgement(frame.payload).has_value()) ||
          (frame.header.message_type == MessageType::kSubscriptionReady &&
           decode_subscription_ready(frame.payload, subscription_limits).has_value()) ||
          (frame.header.message_type == MessageType::kSubscriptionChange &&
           decode_subscription_change(frame.payload, subscription_limits).has_value()) ||
          (frame.header.message_type == MessageType::kSubscriptionCheckpoint &&
           decode_subscription_checkpoint(frame.payload, subscription_limits).has_value()) ||
          (frame.header.message_type == MessageType::kSubscriptionEnd &&
           decode_subscription_end(frame.payload, subscription_limits).has_value()) ||
          (is_terminal_error &&
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
      if (!encoded.has_value() || !found->second.buffers.enqueue(std::move(*encoded)).is_ok()) {
        close_connection(found->first, false);
        continue;
      }
      update_interest(found->second);
    }
  }

  void drain_wakeup() noexcept {
    std::uint64_t value{};
    for (;;) {
      const ssize_t count = ::read(wake_fd, &value, sizeof(value));
      if (count == static_cast<ssize_t>(sizeof(value))) {
        ++stats.response_wakeups;
        continue;
      }
      if (count < 0 && errno == EINTR)
        continue;
      return;
    }
  }

  void expire_connections() {
    const auto now = std::chrono::steady_clock::now();
    for (auto iterator = connections.begin(); iterator != connections.end();) {
      const int fd = iterator->first;
      const Connection& connection = iterator->second;
      const bool expired = (connection.state.phase() == ConnectionPhase::kAwaitingHello &&
                            now - connection.accepted_at >= config.handshake_timeout) ||
                           now - connection.last_progress >= config.idle_timeout;
      ++iterator;
      if (expired)
        close_connection(fd, true);
    }
  }

  EpollServerConfig config;
  SpscNetworkTaskQueue* requests;
  SpscNetworkTaskQueue* responses;
  int epoll_fd{-1};
  int listen_fd{-1};
  int wake_fd{-1};
  std::uint16_t port{};
  std::vector<epoll_event> events_;
  std::vector<std::byte> scratch_;
  std::optional<TlsServerContext> tls_context;
  std::unordered_map<int, Connection> connections;
  std::uint64_t next_connection_id{1U};
  EpollServerMetrics stats;
  bool running{true};
};

#else
class EpollReactor::Impl {};
#endif

EpollReactor::EpollReactor() noexcept = default;
EpollReactor::EpollReactor(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
EpollReactor::~EpollReactor() = default;
EpollReactor::EpollReactor(EpollReactor&&) noexcept = default;
EpollReactor& EpollReactor::operator=(EpollReactor&&) noexcept = default;

common::Result<EpollReactor> EpollReactor::start(const EpollServerConfig& config,
                                                 const EpollReactorQueues& queues) {
#if defined(__linux__)
  if (queues.requests == nullptr || queues.responses == nullptr ||
      config.maximum_connections == 0U || config.maximum_connections > 1'048'576U ||
      config.maximum_events_per_poll == 0U ||
      config.maximum_events_per_poll > static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
      config.maximum_io_operations_per_event == 0U ||
      config.maximum_io_operations_per_event > 1024U || config.read_chunk_bytes == 0U ||
      config.read_chunk_bytes > std::size_t{1U} * 1024U * 1024U || config.listen_backlog <= 0 ||
      config.handshake_timeout.count() <= 0 || config.idle_timeout.count() <= 0 ||
      config.buffers.protocol.maximum_payload_size != config.state.limits.maximum_payload_size)
    return common::make_unexpected(invalid("epoll server configuration is invalid"));
  if (const common::Status status =
          validate_network_security_config(config.security, config.bind_address);
      !status.is_ok())
    return common::make_unexpected(status);
  std::optional<TlsServerContext> tls_context;
  if (config.security.mode == TransportSecurityMode::kTlsRequired) {
    auto context = TlsServerContext::create(*config.security.tls);
    if (!context.has_value())
      return common::make_unexpected(context.error());
    tls_context.emplace(std::move(*context));
  }
  int epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
  if (epoll_fd < 0)
    return common::make_unexpected(io_error("epoll_create1 failed"));
  int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (listener < 0) {
    ::close(epoll_fd);
    return common::make_unexpected(io_error("socket failed"));
  }
  int reuse = 1;
  static_cast<void>(::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(config.port);
  std::uint32_t raw_address{};
  for (const std::uint8_t byte : config.bind_address)
    raw_address = (raw_address << 8U) | byte;
  address.sin_addr.s_addr = htonl(raw_address);
  // POSIX socket APIs require the generic sockaddr view of this fully initialized IPv4 value.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
      ::listen(listener, config.listen_backlog) != 0) {
    ::close(listener);
    ::close(epoll_fd);
    return common::make_unexpected(io_error("bind/listen failed"));
  }
  socklen_t address_size = sizeof(address);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_size) != 0) {
    ::close(listener);
    ::close(epoll_fd);
    return common::make_unexpected(io_error("getsockname failed"));
  }
  epoll_event event{};
  event.events = EPOLLIN;
  event.data.fd = listener;
  if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listener, &event) != 0) {
    ::close(listener);
    ::close(epoll_fd);
    return common::make_unexpected(io_error("epoll_ctl listener failed"));
  }
  const int wake_fd = ::eventfd(0U, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wake_fd < 0) {
    ::close(listener);
    ::close(epoll_fd);
    return common::make_unexpected(io_error("eventfd failed"));
  }
  event = {};
  event.events = EPOLLIN;
  event.data.fd = wake_fd;
  if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, wake_fd, &event) != 0) {
    ::close(wake_fd);
    ::close(listener);
    ::close(epoll_fd);
    return common::make_unexpected(io_error("epoll_ctl eventfd failed"));
  }
  try {
    auto implementation = std::make_unique<Impl>(
        config, *queues.requests, *queues.responses, epoll_fd, listener, wake_fd,
        ntohs(address.sin_port), std::vector<epoll_event>(config.maximum_events_per_poll),
        std::vector<std::byte>(config.read_chunk_bytes), std::move(tls_context));
    implementation->connections.reserve(config.maximum_connections);
    return EpollReactor{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    ::close(wake_fd);
    ::close(listener);
    ::close(epoll_fd);
    return common::make_unexpected(exhausted("epoll reactor allocation failed"));
  }
#else
  static_cast<void>(config);
  static_cast<void>(queues);
  return common::make_unexpected(
      common::Status{common::StatusCode::kNotSupported, "epoll reactor requires Linux"});
#endif
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
common::Status EpollReactor::poll_once(const std::chrono::milliseconds maximum_wait) {
#if defined(__linux__)
  if (!implementation_ || !implementation_->running)
    return invalid("epoll reactor is not running");
  if (maximum_wait.count() < 0 || maximum_wait.count() > std::numeric_limits<int>::max())
    return invalid("epoll wait duration is invalid");
  implementation_->drain_responses();
  const bool serviced_buffered_tls = implementation_->drain_buffered_tls_plaintext();
  const int ready =
      ::epoll_wait(implementation_->epoll_fd, implementation_->events_.data(),
                   static_cast<int>(implementation_->events_.size()),
                   serviced_buffered_tls ? 0 : static_cast<int>(maximum_wait.count()));
  if (ready < 0 && errno != EINTR)
    return io_error("epoll_wait failed");
  for (int index = 0; index < std::max(ready, 0); ++index) {
    const epoll_event event = implementation_->events_[static_cast<std::size_t>(index)];
    if (event.data.fd == implementation_->listen_fd) {
      implementation_->accept_ready();
      continue;
    }
    if (event.data.fd == implementation_->wake_fd) {
      implementation_->drain_wakeup();
      implementation_->drain_responses();
      continue;
    }
    implementation_->service_ready(event.data.fd, (event.events & EPOLLIN) != 0U,
                                   (event.events & EPOLLOUT) != 0U);
    if ((event.events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0U)
      implementation_->close_connection(event.data.fd, false);
  }
  implementation_->expire_connections();
  return common::Status::ok();
#else
  static_cast<void>(maximum_wait);
  return common::Status{common::StatusCode::kNotSupported, "epoll reactor requires Linux"};
#endif
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
common::Status EpollReactor::notify_response_ready() noexcept {
#if defined(__linux__)
  if (!implementation_ || implementation_->wake_fd < 0)
    return invalid("epoll reactor is not running");
  constexpr std::uint64_t kSignal = 1U;
  for (;;) {
    const ssize_t written = ::write(implementation_->wake_fd, &kSignal, sizeof(kSignal));
    if (written == static_cast<ssize_t>(sizeof(kSignal)))
      return common::Status::ok();
    if (written < 0 && errno == EINTR)
      continue;
    if (written < 0 && errno == EAGAIN)
      return common::Status::ok();
    return io_error("eventfd write failed");
  }
#else
  return common::Status{common::StatusCode::kNotSupported, "epoll reactor requires Linux"};
#endif
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
common::Status EpollReactor::shutdown() noexcept {
#if defined(__linux__)
  if (!implementation_)
    return common::Status::ok();
  return implementation_->stop();
#else
  return common::Status::ok();
#endif
}
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::uint16_t EpollReactor::bound_port() const noexcept {
#if defined(__linux__)
  return implementation_ ? implementation_->port : 0U;
#else
  return 0U;
#endif
}
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
EpollServerMetrics EpollReactor::metrics() const noexcept {
#if defined(__linux__)
  return implementation_ ? implementation_->stats : EpollServerMetrics{};
#else
  return {};
#endif
}
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
bool EpollReactor::is_running() const noexcept {
#if defined(__linux__)
  return implementation_ && implementation_->running;
#else
  return false;
#endif
}

} // namespace chronos::network
