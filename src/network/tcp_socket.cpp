#include "chronos/network/tcp_socket.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <new>
#include <string>
#include <sys/socket.h>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status socket_error(const char* operation, const int error = errno) {
  return {common::StatusCode::kIoError,
          std::string(operation) + ": " +
              std::error_code(error, std::generic_category()).message()};
}

[[nodiscard]] bool zero_address(const std::array<std::uint8_t, 4>& address) noexcept {
  return address[0] == 0U && address[1] == 0U && address[2] == 0U && address[3] == 0U;
}

[[nodiscard]] sockaddr_in socket_address(const Ipv4Endpoint& endpoint_value) noexcept {
  std::uint32_t raw{};
  for (const std::uint8_t byte : endpoint_value.address)
    raw = (raw << 8U) | byte;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(endpoint_value.port);
  address.sin_addr.s_addr = htonl(raw);
  return address;
}

[[nodiscard]] Ipv4Endpoint endpoint(const sockaddr_in& address) noexcept {
  const std::uint32_t raw = ntohl(address.sin_addr.s_addr);
  return {.address = {static_cast<std::uint8_t>((raw >> 24U) & 0xffU),
                      static_cast<std::uint8_t>((raw >> 16U) & 0xffU),
                      static_cast<std::uint8_t>((raw >> 8U) & 0xffU),
                      static_cast<std::uint8_t>(raw & 0xffU)},
          .port = ntohs(address.sin_port)};
}

[[nodiscard]] common::Status configure_descriptor(const int descriptor,
                                                  const bool enable_no_delay) {
  const int status_flags = ::fcntl(descriptor, F_GETFL, 0);
  if (status_flags < 0 || ::fcntl(descriptor, F_SETFL, status_flags | O_NONBLOCK) != 0)
    return socket_error("setting nonblocking TCP descriptor");
  const int descriptor_flags = ::fcntl(descriptor, F_GETFD, 0);
  if (descriptor_flags < 0 || ::fcntl(descriptor, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0) {
    return socket_error("setting close-on-exec TCP descriptor");
  }
  if (enable_no_delay) {
    const int no_delay = 1;
    if (::setsockopt(descriptor, IPPROTO_TCP, TCP_NODELAY, &no_delay, sizeof(no_delay)) != 0)
      return socket_error("setting TCP_NODELAY");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<Ipv4Endpoint> query_endpoint(const int descriptor, const bool peer) {
  sockaddr_in address{};
  socklen_t size = sizeof(address);
  // POSIX requires a generic sockaddr view of this initialized IPv4 value.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  const int result = peer ? ::getpeername(descriptor, reinterpret_cast<sockaddr*>(&address), &size)
                          : ::getsockname(descriptor, reinterpret_cast<sockaddr*>(&address), &size);
  if (result != 0)
    return common::make_unexpected(
        socket_error(peer ? "querying TCP peer" : "querying local TCP endpoint"));
  if (size != sizeof(address) || address.sin_family != AF_INET)
    return common::make_unexpected(socket_error("querying IPv4 TCP endpoint", EAFNOSUPPORT));
  return endpoint(address);
}

} // namespace

class TcpSocket::Impl {
public:
  Impl(const int descriptor, const TcpConnectState state, const Ipv4Endpoint peer) noexcept
      : descriptor_(descriptor), state_(state), peer_(peer) {}
  ~Impl() {
    if (descriptor_ >= 0)
      ::close(descriptor_);
  }

  int descriptor_{-1};
  TcpConnectState state_{TcpConnectState::kInProgress};
  Ipv4Endpoint peer_;
};

class TcpListener::Impl {
public:
  Impl(const int descriptor, const Ipv4Endpoint bound) noexcept
      : descriptor_(descriptor), bound_(bound) {}
  ~Impl() {
    if (descriptor_ >= 0)
      ::close(descriptor_);
  }

  int descriptor_{-1};
  Ipv4Endpoint bound_;
};

TcpSocket::TcpSocket() noexcept = default;
TcpSocket::TcpSocket(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
TcpSocket::~TcpSocket() = default;
TcpSocket::TcpSocket(TcpSocket&&) noexcept = default;
TcpSocket& TcpSocket::operator=(TcpSocket&&) noexcept = default;

common::Result<TcpSocket> TcpSocket::begin_connect(const Ipv4Endpoint remote) {
  if (remote.port == 0U || zero_address(remote.address))
    return common::make_unexpected(invalid("TCP remote endpoint is invalid"));
  const int descriptor = ::socket(AF_INET, SOCK_STREAM, 0);
  if (descriptor < 0)
    return common::make_unexpected(socket_error("creating TCP socket"));
  const common::Status configured = configure_descriptor(descriptor, true);
  if (!configured.is_ok()) {
    ::close(descriptor);
    return common::make_unexpected(configured);
  }
  const sockaddr_in address = socket_address(remote);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  const int result =
      ::connect(descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
  const int connect_error = errno;
  TcpConnectState state = TcpConnectState::kConnected;
  if (result != 0) {
    if (connect_error != EINPROGRESS && connect_error != EWOULDBLOCK) {
      ::close(descriptor);
      return common::make_unexpected(socket_error("connecting TCP socket", connect_error));
    }
    state = TcpConnectState::kInProgress;
  }
  try {
    return TcpSocket{std::make_unique<Impl>(descriptor, state, remote)};
  } catch (const std::bad_alloc&) {
    ::close(descriptor);
    return common::make_unexpected(exhausted("TCP socket owner allocation failed"));
  }
}

common::Result<TcpConnectState> TcpSocket::finish_connect() {
  if (!implementation_ || implementation_->descriptor_ < 0)
    return common::make_unexpected(invalid("TCP socket is empty"));
  if (implementation_->state_ == TcpConnectState::kConnected)
    return TcpConnectState::kConnected;
  int error{};
  socklen_t size = sizeof(error);
  if (::getsockopt(implementation_->descriptor_, SOL_SOCKET, SO_ERROR, &error, &size) != 0)
    return common::make_unexpected(socket_error("finishing TCP connect"));
  if (size != sizeof(error))
    return common::make_unexpected(socket_error("reading TCP connect status", EIO));
  if (error == EINPROGRESS || error == EALREADY)
    return TcpConnectState::kInProgress;
  if (error != 0) {
    const common::Status status = socket_error("connecting TCP socket", error);
    const int descriptor = std::exchange(implementation_->descriptor_, -1);
    ::close(descriptor);
    return common::make_unexpected(status);
  }
  implementation_->state_ = TcpConnectState::kConnected;
  return TcpConnectState::kConnected;
}

TcpConnectState TcpSocket::connect_state() const noexcept {
  return implementation_ ? implementation_->state_ : TcpConnectState::kInProgress;
}

common::Result<Ipv4Endpoint> TcpSocket::local_endpoint() const {
  if (!implementation_ || implementation_->descriptor_ < 0 ||
      implementation_->state_ != TcpConnectState::kConnected) {
    return common::make_unexpected(invalid("connected TCP socket is required"));
  }
  return query_endpoint(implementation_->descriptor_, false);
}

common::Result<Ipv4Endpoint> TcpSocket::peer_endpoint() const {
  if (!implementation_ || implementation_->descriptor_ < 0 ||
      implementation_->state_ != TcpConnectState::kConnected) {
    return common::make_unexpected(invalid("connected TCP socket is required"));
  }
  return implementation_->peer_;
}

int TcpSocket::descriptor() const noexcept {
  return implementation_ ? implementation_->descriptor_ : -1;
}

bool TcpSocket::valid() const noexcept {
  return descriptor() >= 0;
}

common::Status TcpSocket::close() {
  if (!implementation_ || implementation_->descriptor_ < 0)
    return common::Status::ok();
  const int descriptor = std::exchange(implementation_->descriptor_, -1);
  return ::close(descriptor) == 0 ? common::Status::ok() : socket_error("closing TCP socket");
}

TcpListener::TcpListener() noexcept = default;
TcpListener::TcpListener(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
TcpListener::~TcpListener() = default;
TcpListener::TcpListener(TcpListener&&) noexcept = default;
TcpListener& TcpListener::operator=(TcpListener&&) noexcept = default;

common::Result<TcpListener> TcpListener::bind(const TcpListenerConfig config) {
  if (config.backlog <= 0 || config.backlog > 65535)
    return common::make_unexpected(invalid("TCP listener backlog is invalid"));
  const int descriptor = ::socket(AF_INET, SOCK_STREAM, 0);
  if (descriptor < 0)
    return common::make_unexpected(socket_error("creating TCP listener"));
  const common::Status configured = configure_descriptor(descriptor, false);
  if (!configured.is_ok()) {
    ::close(descriptor);
    return common::make_unexpected(configured);
  }
  if (config.reuse_address) {
    const int reuse = 1;
    if (::setsockopt(descriptor, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) {
      const common::Status status = socket_error("setting TCP listener reuse-address");
      ::close(descriptor);
      return common::make_unexpected(status);
    }
  }
  const sockaddr_in address = socket_address(config.bind_endpoint);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::bind(descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
      ::listen(descriptor, config.backlog) != 0) {
    const common::Status status = socket_error("binding/listening TCP socket");
    ::close(descriptor);
    return common::make_unexpected(status);
  }
  auto bound = query_endpoint(descriptor, false);
  if (!bound.has_value()) {
    ::close(descriptor);
    return common::make_unexpected(bound.error());
  }
  try {
    return TcpListener{std::make_unique<Impl>(descriptor, *bound)};
  } catch (const std::bad_alloc&) {
    ::close(descriptor);
    return common::make_unexpected(exhausted("TCP listener owner allocation failed"));
  }
}

common::Result<std::optional<TcpSocket>> TcpListener::accept_one() {
  if (!implementation_ || implementation_->descriptor_ < 0)
    return common::make_unexpected(invalid("TCP listener is empty"));
  sockaddr_in address{};
  socklen_t size = sizeof(address);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  const int descriptor =
      ::accept(implementation_->descriptor_, reinterpret_cast<sockaddr*>(&address), &size);
  const int accept_error = errno;
  if (descriptor < 0) {
    if (accept_error == EAGAIN || accept_error == EWOULDBLOCK)
      return std::optional<TcpSocket>{};
    return common::make_unexpected(socket_error("accepting TCP socket", accept_error));
  }
  if (size != sizeof(address) || address.sin_family != AF_INET) {
    ::close(descriptor);
    return common::make_unexpected(socket_error("accepting IPv4 TCP peer", EAFNOSUPPORT));
  }
  const common::Status configured = configure_descriptor(descriptor, true);
  if (!configured.is_ok()) {
    ::close(descriptor);
    return common::make_unexpected(configured);
  }
  try {
    return std::optional<TcpSocket>{TcpSocket{std::make_unique<TcpSocket::Impl>(
        descriptor, TcpConnectState::kConnected, endpoint(address))}};
  } catch (const std::bad_alloc&) {
    ::close(descriptor);
    return common::make_unexpected(exhausted("accepted TCP socket owner allocation failed"));
  }
}

Ipv4Endpoint TcpListener::bound_endpoint() const noexcept {
  return implementation_ ? implementation_->bound_ : Ipv4Endpoint{};
}

int TcpListener::descriptor() const noexcept {
  return implementation_ ? implementation_->descriptor_ : -1;
}

bool TcpListener::valid() const noexcept {
  return descriptor() >= 0;
}

common::Status TcpListener::close() {
  if (!implementation_ || implementation_->descriptor_ < 0)
    return common::Status::ok();
  const int descriptor = std::exchange(implementation_->descriptor_, -1);
  return ::close(descriptor) == 0 ? common::Status::ok() : socket_error("closing TCP listener");
}

} // namespace chronos::network
