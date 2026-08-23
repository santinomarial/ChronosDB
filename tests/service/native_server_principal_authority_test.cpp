#include "chronos/service/native_server_principal_authority.hpp"

#include "gtest/gtest.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] network::PeerCertificateSha256 fingerprint(const std::uint8_t first) {
  network::PeerCertificateSha256 value{};
  value.front() = first;
  return value;
}

TEST(NativeServerPrincipalAuthorityTest, AuthenticatesOnlyConfiguredVerifiedCertificates) {
  auto authority =
      NativeServerPrincipalAuthority::create({{7U, fingerprint(1U)}, {42U, fingerprint(2U)}});
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  ASSERT_EQ(authority->principals().size(), 2U);

  auto authenticated = authority->authenticate({.ipv4_address = {203U, 0U, 113U, 9U},
                                                .transport_authenticated = true,
                                                .peer_certificate_sha256 = fingerprint(2U)});
  ASSERT_TRUE(authenticated.has_value()) << authenticated.error().to_string();
  EXPECT_TRUE(authenticated->authorized);
  EXPECT_EQ(authenticated->principal_id, 42U);

  auto unknown = authority->authenticate({.ipv4_address = {127U, 0U, 0U, 1U},
                                          .transport_authenticated = true,
                                          .peer_certificate_sha256 = fingerprint(3U)});
  ASSERT_TRUE(unknown.has_value()) << unknown.error().to_string();
  EXPECT_FALSE(unknown->authorized);
  EXPECT_EQ(unknown->principal_id, 0U);

  const auto missing_certificate = authority->authenticate(
      {.ipv4_address = {127U, 0U, 0U, 1U}, .transport_authenticated = true});
  ASSERT_FALSE(missing_certificate.has_value());
  EXPECT_EQ(missing_certificate.error().code(), common::StatusCode::kUnauthenticated);
  const auto unverified = authority->authenticate({.ipv4_address = {127U, 0U, 0U, 1U},
                                                   .transport_authenticated = false,
                                                   .peer_certificate_sha256 = fingerprint(1U)});
  ASSERT_FALSE(unverified.has_value());
  EXPECT_EQ(unverified.error().code(), common::StatusCode::kUnauthenticated);
}

TEST(NativeServerPrincipalAuthorityTest, RejectsNoncanonicalConstruction) {
  for (auto principals :
       {std::vector<NativeServerPrincipal>{},
        std::vector<NativeServerPrincipal>{{0U, fingerprint(1U)}},
        std::vector<NativeServerPrincipal>{{2U, fingerprint(1U)}, {1U, fingerprint(2U)}},
        std::vector<NativeServerPrincipal>{{1U, fingerprint(1U)}, {2U, fingerprint(1U)}}}) {
    const auto authority = NativeServerPrincipalAuthority::create(std::move(principals));
    ASSERT_FALSE(authority.has_value());
    EXPECT_EQ(authority.error().code(), common::StatusCode::kInvalidArgument);
  }
}

TEST(NativeServerPrincipalAuthorityTest, SupportsConcurrentImmutableAuthentication) {
  auto authority =
      NativeServerPrincipalAuthority::create({{7U, fingerprint(1U)}, {42U, fingerprint(2U)}});
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  std::atomic<bool> failed{false};
  const auto authenticate = [&] {
    for (std::size_t iteration = 0U; iteration < 10'000U; ++iteration) {
      auto result = authority->authenticate({.ipv4_address = {198U, 51U, 100U, 7U},
                                             .transport_authenticated = true,
                                             .peer_certificate_sha256 = fingerprint(1U)});
      if (!result.has_value() || !result->authorized || result->principal_id != 7U)
        failed.store(true, std::memory_order_relaxed);
    }
  };
  std::thread first{authenticate};
  std::thread second{authenticate};
  first.join();
  second.join();
  EXPECT_FALSE(failed.load(std::memory_order_relaxed));
}

} // namespace
} // namespace chronos::service
