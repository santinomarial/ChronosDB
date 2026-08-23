#include "chronos/service/native_server_principal_authority.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status unauthenticated(const char* message) {
  return {common::StatusCode::kUnauthenticated, message};
}

} // namespace

class NativeServerPrincipalAuthority::Impl {
public:
  explicit Impl(std::vector<NativeServerPrincipal> configured) noexcept
      : configured_principals(std::move(configured)) {}

  std::vector<NativeServerPrincipal> configured_principals;
};

NativeServerPrincipalAuthority::NativeServerPrincipalAuthority(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
NativeServerPrincipalAuthority::~NativeServerPrincipalAuthority() = default;
NativeServerPrincipalAuthority::NativeServerPrincipalAuthority(
    NativeServerPrincipalAuthority&&) noexcept = default;
NativeServerPrincipalAuthority&
NativeServerPrincipalAuthority::operator=(NativeServerPrincipalAuthority&&) noexcept = default;

common::Result<NativeServerPrincipalAuthority>
NativeServerPrincipalAuthority::create(std::vector<NativeServerPrincipal> principals) {
  if (principals.empty())
    return common::make_unexpected(invalid("native server principal authority is empty"));
  try {
    for (std::size_t index = 0U; index < principals.size(); ++index) {
      if (principals[index].principal_id == 0U ||
          (index != 0U && principals[index - 1U].principal_id >= principals[index].principal_id)) {
        return common::make_unexpected(
            invalid("native server principal authority is not canonical"));
      }
      if (std::ranges::any_of(
              std::span{principals}.first(index), [&](const NativeServerPrincipal& previous) {
                return previous.certificate_sha256 == principals[index].certificate_sha256;
              })) {
        return common::make_unexpected(
            invalid("native server principal authority has a duplicate certificate"));
      }
    }
    return NativeServerPrincipalAuthority{std::make_unique<Impl>(std::move(principals))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("native server principal authority allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("native server principal authority exceeds limits"));
  }
}

common::Result<network::PeerAuthenticationResult>
NativeServerPrincipalAuthority::authenticate(const network::PeerAuthenticationRequest& request) {
  if (!request.transport_authenticated || !request.peer_certificate_sha256.has_value()) {
    return common::make_unexpected(
        unauthenticated("native server principal authority requires a verified TLS certificate"));
  }
  const auto found =
      std::ranges::find(impl_->configured_principals, *request.peer_certificate_sha256,
                        &NativeServerPrincipal::certificate_sha256);
  if (found == impl_->configured_principals.end())
    return network::PeerAuthenticationResult{};
  return network::PeerAuthenticationResult{.authorized = true, .principal_id = found->principal_id};
}

std::span<const NativeServerPrincipal> NativeServerPrincipalAuthority::principals() const noexcept {
  return impl_->configured_principals;
}

} // namespace chronos::service
