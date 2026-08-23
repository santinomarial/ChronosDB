#include "chronos/service/native_client_tls_route_owner.hpp"

#include "chronos/network/tls_socket.hpp"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

constexpr std::size_t kMaximumRouteConfigBytes{std::size_t{16U} * 1024U * 1024U};
constexpr std::size_t kMaximumTlsFileBytes{std::size_t{64U} * 1024U * 1024U};

[[nodiscard]] common::Status invalid(const std::string& message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status io_error(const std::string& operation, const int error = errno) {
  return {common::StatusCode::kIoError,
          operation + ": " + std::error_code{error, std::generic_category()}.message()};
}

class Descriptor {
public:
  explicit Descriptor(const int descriptor) noexcept : descriptor_(descriptor) {}
  ~Descriptor() {
    if (descriptor_ >= 0)
      static_cast<void>(::close(descriptor_));
  }
  Descriptor(const Descriptor&) = delete;
  Descriptor& operator=(const Descriptor&) = delete;
  [[nodiscard]] int get() const noexcept {
    return descriptor_;
  }

private:
  int descriptor_{};
};

[[nodiscard]] bool valid_path(const std::string& path) noexcept {
  return !path.empty() && path.find('\0') == std::string::npos;
}

[[nodiscard]] common::Result<struct stat> regular_file_metadata(const int descriptor,
                                                                const std::string_view kind) {
  struct stat metadata {};
  int result{};
  do {
    result = ::fstat(descriptor, &metadata);
  } while (result != 0 && errno == EINTR);
  if (result != 0)
    return common::make_unexpected(io_error("inspecting native client " + std::string{kind}));
  if (!S_ISREG(metadata.st_mode))
    return common::make_unexpected(
        invalid("native client " + std::string{kind} + " is not a regular file"));
  return metadata;
}

[[nodiscard]] common::Result<std::string> read_route_file(const std::string& path,
                                                          const std::size_t maximum_bytes) {
  int descriptor{};
  do {
    descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  } while (descriptor < 0 && errno == EINTR);
  if (descriptor < 0)
    return common::make_unexpected(io_error("opening native client route config"));
  const Descriptor owned{descriptor};
  auto metadata = regular_file_metadata(owned.get(), "route config");
  if (!metadata.has_value())
    return common::make_unexpected(metadata.error());
  if (metadata->st_size <= 0 || static_cast<std::uintmax_t>(metadata->st_size) > maximum_bytes)
    return common::make_unexpected(
        exhausted("native client route config file size exceeds its bound"));
  if ((metadata->st_mode & 0022) != 0)
    return common::make_unexpected(
        invalid("native client route config must not be writable by group or other"));

  std::string bytes(static_cast<std::size_t>(metadata->st_size), '\0');
  std::size_t offset{};
  while (offset < bytes.size()) {
    const ssize_t count = ::read(owned.get(), bytes.data() + offset, bytes.size() - offset);
    if (count < 0 && errno == EINTR)
      continue;
    if (count <= 0)
      return common::make_unexpected(
          io_error("reading complete native client route config", count == 0 ? EIO : errno));
    offset += static_cast<std::size_t>(count);
  }
  char extra{};
  ssize_t trailing{};
  do {
    trailing = ::read(owned.get(), &extra, 1U);
  } while (trailing < 0 && errno == EINTR);
  if (trailing < 0)
    return common::make_unexpected(io_error("finishing native client route config read"));
  if (trailing != 0)
    return common::make_unexpected(invalid("native client route config changed while being read"));
  return bytes;
}

[[nodiscard]] common::Status qualify_tls_file(const std::string& path, const std::string_view kind,
                                              const std::size_t maximum_bytes,
                                              const bool private_key) {
  int descriptor{};
  do {
    descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  } while (descriptor < 0 && errno == EINTR);
  if (descriptor < 0)
    return io_error("opening native client " + std::string{kind});
  const Descriptor owned{descriptor};
  auto metadata = regular_file_metadata(owned.get(), kind);
  if (!metadata.has_value())
    return metadata.error();
  if (metadata->st_size <= 0 || static_cast<std::uintmax_t>(metadata->st_size) > maximum_bytes)
    return invalid("native client " + std::string{kind} + " has an invalid bounded size");
  if (private_key) {
    if ((metadata->st_mode & 0077) != 0)
      return invalid("native client private key must be inaccessible to group and other");
  } else if ((metadata->st_mode & 0022) != 0) {
    return invalid("native client " + std::string{kind} +
                   " must not be writable by group or other");
  }
  return common::Status::ok();
}

[[nodiscard]] bool valid_limits(const NativeClientTlsRouteFileLimits& limits) noexcept {
  return limits.route_config.maximum_bytes != 0U &&
         limits.route_config.maximum_bytes <= kMaximumRouteConfigBytes &&
         limits.route_config.maximum_nodes != 0U && limits.route_config.maximum_nodes <= 65'535U &&
         limits.route_config.maximum_tls_identity_bytes != 0U &&
         limits.route_config.maximum_tls_identity_bytes <= 253U &&
         limits.maximum_tls_file_bytes != 0U &&
         limits.maximum_tls_file_bytes <= kMaximumTlsFileBytes;
}

} // namespace

class NativeClientTlsRouteOwner::Impl {
public:
  explicit Impl(NativeClientRouteAuthority configured_authority) noexcept
      : route_authority(std::move(configured_authority)) {}

  [[nodiscard]] common::Status initialize(const NativeClientTlsCredentials& credentials) {
    try {
      const auto configured = route_authority.routes();
      tls_contexts.reserve(configured.size());
      routes.reserve(configured.size());
      for (const NativeClientRoute& route : configured) {
        auto context = network::TlsClientContext::create(
            {.certificate_chain_file = credentials.certificate_chain_file,
             .private_key_file = credentials.private_key_file,
             .trust_store_file = credentials.trust_store_file,
             .expected_server_identity = route.tls_server_identity});
        if (!context.has_value())
          return context.error();
        tls_contexts.push_back(std::move(*context));
        routes.push_back({.node_id = route.node_id,
                          .endpoint = route.endpoint,
                          .tls_context = std::addressof(tls_contexts.back())});
      }
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return exhausted("native client TLS route ownership allocation failed");
    } catch (const std::length_error&) {
      return exhausted("native client TLS route ownership exceeds container limits");
    }
  }

  NativeClientRouteAuthority route_authority;
  std::vector<network::TlsClientContext> tls_contexts;
  std::vector<network::NativeLeaderRoute> routes;
};

NativeClientTlsRouteOwner::NativeClientTlsRouteOwner(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
NativeClientTlsRouteOwner::~NativeClientTlsRouteOwner() = default;
NativeClientTlsRouteOwner::NativeClientTlsRouteOwner(NativeClientTlsRouteOwner&&) noexcept =
    default;
NativeClientTlsRouteOwner&
NativeClientTlsRouteOwner::operator=(NativeClientTlsRouteOwner&&) noexcept = default;

common::Result<NativeClientTlsRouteOwner>
NativeClientTlsRouteOwner::load(const NativeClientTlsRouteOwnerConfig& config) {
  if (!valid_path(config.route_config_file) || !valid_path(config.tls.certificate_chain_file) ||
      !valid_path(config.tls.private_key_file) || !valid_path(config.tls.trust_store_file) ||
      !valid_limits(config.limits)) {
    return common::make_unexpected(invalid("native client TLS route configuration is invalid"));
  }
  try {
    auto text = read_route_file(config.route_config_file, config.limits.route_config.maximum_bytes);
    if (!text.has_value())
      return common::make_unexpected(text.error());
    auto configured_routes = parse_native_client_route_config(*text, config.limits.route_config);
    if (!configured_routes.has_value())
      return common::make_unexpected(configured_routes.error());

    common::Status qualified =
        qualify_tls_file(config.tls.certificate_chain_file, "certificate chain",
                         config.limits.maximum_tls_file_bytes, false);
    if (!qualified.is_ok())
      return common::make_unexpected(std::move(qualified));
    qualified = qualify_tls_file(config.tls.private_key_file, "private key",
                                 config.limits.maximum_tls_file_bytes, true);
    if (!qualified.is_ok())
      return common::make_unexpected(std::move(qualified));
    qualified = qualify_tls_file(config.tls.trust_store_file, "trust store",
                                 config.limits.maximum_tls_file_bytes, false);
    if (!qualified.is_ok())
      return common::make_unexpected(std::move(qualified));

    auto authority = NativeClientRouteAuthority::create(std::move(*configured_routes));
    if (!authority.has_value())
      return common::make_unexpected(authority.error());
    auto impl = std::make_unique<Impl>(std::move(*authority));
    const common::Status initialized = impl->initialize(config.tls);
    if (!initialized.is_ok())
      return common::make_unexpected(initialized);
    return NativeClientTlsRouteOwner{std::move(impl)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("native client TLS route owner allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("native client TLS route owner configuration exceeds limits"));
  }
}

NativeClientRouteAuthority& NativeClientTlsRouteOwner::authority() noexcept {
  return impl_->route_authority;
}

const NativeClientRouteAuthority& NativeClientTlsRouteOwner::authority() const noexcept {
  return impl_->route_authority;
}

std::span<const NativeClientRoute> NativeClientTlsRouteOwner::configured_routes() const noexcept {
  return impl_ ? impl_->route_authority.routes() : std::span<const NativeClientRoute>{};
}

std::span<const network::NativeLeaderRoute>
NativeClientTlsRouteOwner::leader_routes() const noexcept {
  return impl_ ? std::span<const network::NativeLeaderRoute>{impl_->routes}
               : std::span<const network::NativeLeaderRoute>{};
}

} // namespace chronos::service
