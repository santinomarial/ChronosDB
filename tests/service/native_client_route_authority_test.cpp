#include "chronos/service/native_client_route_authority.hpp"

#include <atomic>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

namespace chronos::service {
namespace {

struct RouteValues {
  std::uint64_t node_id{};
  std::uint8_t address{};
  std::uint16_t port{};
  std::uint8_t fingerprint{};
};

[[nodiscard]] NativeClientRoute route(const RouteValues values) {
  NativeClientRoute value{.node_id = values.node_id,
                          .endpoint = {{127U, 0U, 0U, values.address}, values.port},
                          .tls_server_identity = "node.example.test"};
  value.certificate_sha256.fill(values.fingerprint);
  return value;
}

TEST(NativeClientRouteAuthorityTest, AuthenticatesAndAuthorizesOneExactNativeNode) {
  auto authority = NativeClientRouteAuthority::create(
      {route({.node_id = 1U, .address = 1U, .port = 7421U, .fingerprint = 11U}),
       route({.node_id = 2U, .address = 2U, .port = 7422U, .fingerprint = 22U})});
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  const auto fingerprint = authority->routes()[1].certificate_sha256;
  auto authenticated = authority->authenticate({.ipv4_address = {127U, 0U, 0U, 2U},
                                                .transport_authenticated = true,
                                                .peer_certificate_sha256 = fingerprint});
  ASSERT_TRUE(authenticated.has_value()) << authenticated.error().to_string();
  EXPECT_TRUE(authenticated->authorized);
  EXPECT_EQ(authenticated->principal_id, 2U);
  EXPECT_TRUE(*authority->authorize_node(authenticated->principal_id, 2U));
  EXPECT_FALSE(*authority->authorize_node(authenticated->principal_id, 1U));
  EXPECT_FALSE(*authority->authorize_node(3U, 3U));
  ASSERT_NE(authority->find_route(1U), nullptr);
  EXPECT_EQ(authority->find_route(1U)->endpoint.port, 7421U);
  EXPECT_EQ(authority->find_route(3U), nullptr);
}

TEST(NativeClientRouteAuthorityTest, RejectsMissingAndContradictoryTransportIdentity) {
  auto authority = NativeClientRouteAuthority::create(
      {route({.node_id = 1U, .address = 1U, .port = 7421U, .fingerprint = 11U}),
       route({.node_id = 2U, .address = 2U, .port = 7422U, .fingerprint = 22U})});
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  const auto fingerprint = authority->routes()[0].certificate_sha256;
  auto unauthenticated = authority->authenticate(
      {.ipv4_address = {127U, 0U, 0U, 1U}, .transport_authenticated = false});
  ASSERT_FALSE(unauthenticated.has_value());
  EXPECT_EQ(unauthenticated.error().code(), common::StatusCode::kUnauthenticated);
  auto wrong_address = authority->authenticate({.ipv4_address = {127U, 0U, 0U, 2U},
                                                .transport_authenticated = true,
                                                .peer_certificate_sha256 = fingerprint});
  ASSERT_TRUE(wrong_address.has_value()) << wrong_address.error().to_string();
  EXPECT_FALSE(wrong_address->authorized);
  EXPECT_EQ(wrong_address->principal_id, 0U);
}

TEST(NativeClientRouteAuthorityTest, SupportsConcurrentImmutableAuthenticationAndAuthorization) {
  auto authority = NativeClientRouteAuthority::create(
      {route({.node_id = 1U, .address = 1U, .port = 7421U, .fingerprint = 11U}),
       route({.node_id = 2U, .address = 2U, .port = 7422U, .fingerprint = 22U})});
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  const NativeClientRoute first_route = authority->routes()[0];
  const NativeClientRoute second_route = authority->routes()[1];
  std::atomic<bool> exact{true};
  const auto exercise = [&](const NativeClientRoute& expected) {
    for (std::size_t iteration = 0U; iteration < 1000U; ++iteration) {
      auto authenticated =
          authority->authenticate({.ipv4_address = expected.endpoint.address,
                                   .transport_authenticated = true,
                                   .peer_certificate_sha256 = expected.certificate_sha256});
      if (!authenticated.has_value() || !authenticated->authorized ||
          authenticated->principal_id != expected.node_id) {
        exact.store(false, std::memory_order_relaxed);
        continue;
      }
      auto authorized = authority->authorize_node(authenticated->principal_id, expected.node_id);
      if (!authorized.has_value() || !*authorized)
        exact.store(false, std::memory_order_relaxed);
    }
  };
  std::thread first{exercise, first_route};
  std::thread second{exercise, second_route};
  first.join();
  second.join();
  EXPECT_TRUE(exact.load(std::memory_order_relaxed));
}

TEST(NativeClientRouteAuthorityTest, RevalidatesCanonicalAndUniqueAuthority) {
  EXPECT_EQ(NativeClientRouteAuthority::create({}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(NativeClientRouteAuthority::create(
                {route({.node_id = 2U, .address = 2U, .port = 7422U, .fingerprint = 22U}),
                 route({.node_id = 1U, .address = 1U, .port = 7421U, .fingerprint = 11U})})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  auto duplicate_endpoint =
      route({.node_id = 2U, .address = 1U, .port = 7421U, .fingerprint = 22U});
  EXPECT_EQ(NativeClientRouteAuthority::create(
                {route({.node_id = 1U, .address = 1U, .port = 7421U, .fingerprint = 11U}),
                 std::move(duplicate_endpoint)})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  auto duplicate_fingerprint =
      route({.node_id = 2U, .address = 2U, .port = 7422U, .fingerprint = 11U});
  EXPECT_EQ(NativeClientRouteAuthority::create(
                {route({.node_id = 1U, .address = 1U, .port = 7421U, .fingerprint = 11U}),
                 std::move(duplicate_fingerprint)})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  auto unusable_endpoint = route({.node_id = 1U, .address = 0U, .port = 7421U, .fingerprint = 11U});
  unusable_endpoint.endpoint.address = {};
  EXPECT_EQ(NativeClientRouteAuthority::create({std::move(unusable_endpoint)}).error().code(),
            common::StatusCode::kInvalidArgument);
  auto invalid_identity = route({.node_id = 1U, .address = 1U, .port = 7421U, .fingerprint = 11U});
  invalid_identity.tls_server_identity = "Node.example.test";
  EXPECT_EQ(NativeClientRouteAuthority::create({std::move(invalid_identity)}).error().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::service
