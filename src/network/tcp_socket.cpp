#include "chronos/network/tcp_socket.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <charconv>
#include <fcntl.h>
#include <limits>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <new>
#include <ranges>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

template <typename Integer>
[[nodiscard]] bool parse_canonical_decimal(const std::string_view text, Integer& value) {
  if (text.empty() || (text.size() > 1U && text.front() == '0'))
    return false;
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status unavailable(const std::string& message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status socket_error(const char* operation, const int error = errno) {
  return {common::StatusCode::kIoError,
          std::string(operation) + ": " +
              std::error_code(error, std::generic_category()).message()};
}

[[nodiscard]] bool zero_address(const std::array<std::uint8_t, 4>& address) noexcept {
  return address[0] == 0U && address[1] == 0U && address[2] == 0U && address[3] == 0U;
}

struct DnsEndpoint {
  std::string_view hostname;
  std::uint16_t port{};
};

[[nodiscard]] bool is_lowercase_dns_name(const std::string_view text,
                                         const std::size_t maximum_bytes) noexcept {
  if (text.empty() || text.size() > maximum_bytes || text.size() > 253U || text.front() == '.' ||
      text.back() == '.') {
    return false;
  }
  std::size_t label_start{};
  for (std::size_t index = 0U; index <= text.size(); ++index) {
    if (index != text.size() && text[index] != '.')
      continue;
    const std::size_t label_size = index - label_start;
    if (label_size == 0U || label_size > 63U || text[label_start] == '-' ||
        text[index - 1U] == '-') {
      return false;
    }
    for (std::size_t character = label_start; character < index; ++character) {
      const char value = text[character];
      if (!((value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '-'))
        return false;
    }
    label_start = index + 1U;
  }
  return true;
}

[[nodiscard]] common::Result<DnsEndpoint>
parse_dns_endpoint(const std::string_view text, const std::size_t maximum_hostname_bytes) {
  const std::size_t colon = text.find(':');
  const std::string_view hostname = text.substr(0U, colon);
  const bool numeric_looking =
      !hostname.empty() && std::ranges::all_of(hostname, [](const char value) {
        return (value >= '0' && value <= '9') || value == '.';
      });
  if (colon == std::string_view::npos || text.find(':', colon + 1U) != std::string_view::npos ||
      numeric_looking || !is_lowercase_dns_name(hostname, maximum_hostname_bytes)) {
    return common::make_unexpected(invalid("DNS endpoint is not canonical"));
  }
  unsigned int port{};
  if (!parse_canonical_decimal(text.substr(colon + 1U), port) || port == 0U ||
      port > std::numeric_limits<std::uint16_t>::max()) {
    return common::make_unexpected(invalid("DNS endpoint port is invalid"));
  }
  return DnsEndpoint{.hostname = hostname, .port = static_cast<std::uint16_t>(port)};
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
  auto* generic_address = reinterpret_cast<sockaddr*>(&address);
  const int result = peer ? ::getpeername(descriptor, generic_address, &size)
                          : ::getsockname(descriptor, generic_address, &size);
  if (result != 0)
    return common::make_unexpected(
        socket_error(peer ? "querying TCP peer" : "querying local TCP endpoint"));
  if (size != sizeof(address) || address.sin_family != AF_INET)
    return common::make_unexpected(socket_error("querying IPv4 TCP endpoint", EAFNOSUPPORT));
  return endpoint(address);
}

} // namespace

common::Result<Ipv4Endpoint> parse_ipv4_endpoint(const std::string_view text) {
  const std::size_t colon = text.find(':');
  if (colon == std::string_view::npos || text.find(':', colon + 1U) != std::string_view::npos)
    return common::make_unexpected(invalid("IPv4 endpoint is malformed"));

  Ipv4Endpoint parsed;
  std::size_t offset = 0U;
  bool nonzero_address = false;
  for (std::size_t index = 0U; index < parsed.address.size(); ++index) {
    const std::size_t dot = text.find('.', offset);
    const bool final_octet = index + 1U == parsed.address.size();
    const std::size_t end = final_octet ? colon : dot;
    if ((!final_octet && (dot == std::string_view::npos || dot >= colon)) ||
        (final_octet && offset >= colon)) {
      return common::make_unexpected(invalid("IPv4 endpoint address is malformed"));
    }
    unsigned int octet{};
    if (!parse_canonical_decimal(text.substr(offset, end - offset), octet) || octet > 255U)
      return common::make_unexpected(invalid("IPv4 endpoint address is not canonical"));
    parsed.address[index] = static_cast<std::uint8_t>(octet);
    nonzero_address = nonzero_address || octet != 0U;
    offset = end + 1U;
  }
  if (offset != colon + 1U)
    return common::make_unexpected(invalid("IPv4 endpoint address has extra octets"));

  unsigned int port{};
  if (!parse_canonical_decimal(text.substr(colon + 1U), port) || port == 0U ||
      port > std::numeric_limits<std::uint16_t>::max()) {
    return common::make_unexpected(invalid("IPv4 endpoint port is invalid"));
  }
  if (!nonzero_address)
    return common::make_unexpected(invalid("IPv4 endpoint address is zero"));
  parsed.port = static_cast<std::uint16_t>(port);
  return parsed;
}

common::Result<std::vector<Ipv4Endpoint>>
resolve_ipv4_endpoints(const std::string_view text, const Ipv4EndpointResolutionLimits limits) {
  if (limits.maximum_addresses == 0U || limits.maximum_addresses > 1024U ||
      limits.maximum_hostname_bytes == 0U || limits.maximum_hostname_bytes > 253U) {
    return common::make_unexpected(invalid("IPv4 resolution limits are invalid"));
  }
  try {
    if (auto numeric = parse_ipv4_endpoint(text); numeric.has_value())
      return std::vector<Ipv4Endpoint>{*numeric};

    auto endpoint_value = parse_dns_endpoint(text, limits.maximum_hostname_bytes);
    if (!endpoint_value.has_value())
      return common::make_unexpected(endpoint_value.error());
    const std::string hostname{endpoint_value->hostname};
    const std::string service = std::to_string(endpoint_value->port);
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_NUMERICSERV;
    addrinfo* raw_results{};
    const int resolved = ::getaddrinfo(hostname.c_str(), service.c_str(), &hints, &raw_results);
    if (resolved != 0) {
      if (resolved == EAI_MEMORY)
        return common::make_unexpected(exhausted("IPv4 DNS resolution allocation failed"));
      return common::make_unexpected(
          unavailable(std::string("resolving IPv4 DNS endpoint: ") + ::gai_strerror(resolved)));
    }
    const std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> results(raw_results,
                                                                       &::freeaddrinfo);
    std::vector<Ipv4Endpoint> addresses;
    addresses.reserve(std::min<std::size_t>(limits.maximum_addresses, 16U));
    for (const addrinfo* candidate = results.get(); candidate != nullptr;
         candidate = candidate->ai_next) {
      if (candidate->ai_family != AF_INET || candidate->ai_addr == nullptr ||
          candidate->ai_addrlen < sizeof(sockaddr_in)) {
        continue;
      }
      // POSIX returns an initialized sockaddr_in behind the generic address pointer for AF_INET.
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
      const auto* ipv4 = reinterpret_cast<const sockaddr_in*>(candidate->ai_addr);
      Ipv4Endpoint address = endpoint(*ipv4);
      if (address.port != endpoint_value->port || zero_address(address.address) ||
          std::ranges::find(addresses, address) != addresses.end()) {
        continue;
      }
      if (addresses.size() >= limits.maximum_addresses) {
        return common::make_unexpected(exhausted("IPv4 DNS answer limit is exhausted"));
      }
      addresses.push_back(address);
    }
    if (addresses.empty())
      return common::make_unexpected(unavailable("IPv4 DNS endpoint has no usable addresses"));
    return addresses;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("IPv4 DNS resolution allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("IPv4 DNS resolution exceeds container limits"));
  }
}

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
  // POSIX connect accepts the initialized IPv4 address through its generic sockaddr view.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  const auto* generic_address = reinterpret_cast<const sockaddr*>(&address);
  const int result = ::connect(descriptor, generic_address, sizeof(address));
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
  // POSIX accept writes an IPv4 peer through its generic sockaddr output view.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  auto* generic_address = reinterpret_cast<sockaddr*>(&address);
  const int descriptor = ::accept(implementation_->descriptor_, generic_address, &size);
  const int accept_error = errno;
  if (descriptor < 0) {
    bool would_block = accept_error == EAGAIN;
#if EWOULDBLOCK != EAGAIN
    would_block = would_block || accept_error == EWOULDBLOCK;
#endif
    if (would_block)
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
