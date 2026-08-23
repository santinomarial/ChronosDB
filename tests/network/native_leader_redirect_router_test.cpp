#include "chronos/network/native_leader_redirect_router.hpp"

#include "gtest/gtest.h"
#include <array>
#include <cstdint>
#include <utility>

namespace chronos::network {
namespace {

[[nodiscard]] common::Uuid group(const std::uint8_t seed = 1U) {
  std::array<std::byte, 16U> bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] NativeLeaderRoute route(const std::uint64_t node_id, const std::uint16_t port,
                                      const TlsClientContext& tls) {
  return {.node_id = node_id,
          .endpoint = {{127U, 0U, 0U, static_cast<std::uint8_t>(node_id)}, port},
          .tls_context = &tls};
}

[[nodiscard]] NativeLeaderRedirectRouterConfig config(const TlsClientContext& first,
                                                      const TlsClientContext& second,
                                                      const TlsClientContext& third) {
  return {.group_id = group(),
          .initial_node_id = 1U,
          .minimum_placement_epoch = 7U,
          .routes = {route(1U, 7401U, first), route(2U, 7402U, second), route(3U, 7403U, third)},
          .limits = {.maximum_routes = 3U, .maximum_redirects = 2U}};
}

TEST(NativeLeaderRedirectRouterTest, ResolvesMonotonicRedirectsThroughAuthenticatedRoutes) {
  TlsClientContext first;
  TlsClientContext second;
  TlsClientContext third;
  auto router = NativeLeaderRedirectRouter::create(config(first, second, third));
  ASSERT_TRUE(router.has_value()) << router.error().to_string();

  const LeaderRedirect to_second{
      .group_id = group(), .leader_node_id = 2U, .leader_term = 11U, .placement_epoch = 7U};
  auto selected = router->accept(to_second);
  ASSERT_TRUE(selected.has_value()) << selected.error().to_string();
  EXPECT_EQ(selected->route, route(2U, 7402U, second));
  EXPECT_EQ(selected->authority, to_second);
  EXPECT_EQ(selected->redirect_number, 1U);
  EXPECT_EQ(router->current_node_id(), 2U);
  EXPECT_EQ(router->last_authority(), to_second);

  const LeaderRedirect to_third{
      .group_id = group(), .leader_node_id = 3U, .leader_term = 12U, .placement_epoch = 8U};
  selected = router->accept(to_third);
  ASSERT_TRUE(selected.has_value()) << selected.error().to_string();
  EXPECT_EQ(selected->route, route(3U, 7403U, third));
  EXPECT_EQ(selected->redirect_number, 2U);
  EXPECT_EQ(router->accepted_redirects(), 2U);
  EXPECT_EQ(router->routes().size(), 3U);
}

TEST(NativeLeaderRedirectRouterTest, RejectsInvalidAuthorityWithoutChangingState) {
  TlsClientContext first;
  TlsClientContext second;
  TlsClientContext third;
  auto router = NativeLeaderRedirectRouter::create(config(first, second, third));
  ASSERT_TRUE(router.has_value()) << router.error().to_string();
  const LeaderRedirect accepted{
      .group_id = group(), .leader_node_id = 2U, .leader_term = 11U, .placement_epoch = 7U};
  ASSERT_TRUE(router->accept(accepted).has_value());

  const std::array rejected{
      LeaderRedirect{group(9U), 3U, 12U, 8U}, LeaderRedirect{group(), 3U, 10U, 8U},
      LeaderRedirect{group(), 3U, 11U, 8U},   LeaderRedirect{group(), 2U, 12U, 8U},
      LeaderRedirect{group(), 4U, 12U, 8U},   LeaderRedirect{group(), 3U, 12U, 6U}};
  for (const LeaderRedirect& redirect : rejected) {
    EXPECT_FALSE(router->accept(redirect).has_value());
    EXPECT_EQ(router->current_node_id(), 2U);
    EXPECT_EQ(router->accepted_redirects(), 1U);
    EXPECT_EQ(router->last_authority(), accepted);
  }
}

TEST(NativeLeaderRedirectRouterTest, EnforcesFiniteRetryBudget) {
  TlsClientContext first;
  TlsClientContext second;
  TlsClientContext third;
  auto configured = config(first, second, third);
  configured.limits.maximum_redirects = 1U;
  auto router = NativeLeaderRedirectRouter::create(std::move(configured));
  ASSERT_TRUE(router.has_value()) << router.error().to_string();
  ASSERT_TRUE(router->accept({group(), 2U, 11U, 7U}).has_value());
  auto exhausted = router->accept({group(), 3U, 12U, 8U});
  ASSERT_FALSE(exhausted.has_value());
  EXPECT_EQ(exhausted.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(router->current_node_id(), 2U);
}

TEST(NativeLeaderRedirectRouterTest, RejectsInvalidAndNoncanonicalConfiguration) {
  TlsClientContext first;
  TlsClientContext second;
  TlsClientContext third;

  auto invalid_group = config(first, second, third);
  invalid_group.group_id = {};
  EXPECT_FALSE(NativeLeaderRedirectRouter::create(std::move(invalid_group)).has_value());
  auto missing_initial = config(first, second, third);
  missing_initial.initial_node_id = 4U;
  EXPECT_FALSE(NativeLeaderRedirectRouter::create(std::move(missing_initial)).has_value());
  auto unsorted = config(first, second, third);
  std::swap(unsorted.routes[0], unsorted.routes[1]);
  EXPECT_FALSE(NativeLeaderRedirectRouter::create(std::move(unsorted)).has_value());
  auto duplicate_endpoint = config(first, second, third);
  duplicate_endpoint.routes[1].endpoint = duplicate_endpoint.routes[0].endpoint;
  EXPECT_FALSE(NativeLeaderRedirectRouter::create(std::move(duplicate_endpoint)).has_value());
  auto missing_tls = config(first, second, third);
  missing_tls.routes[1].tls_context = nullptr;
  EXPECT_FALSE(NativeLeaderRedirectRouter::create(std::move(missing_tls)).has_value());
  auto over_limit = config(first, second, third);
  over_limit.limits.maximum_routes = 2U;
  auto rejected = NativeLeaderRedirectRouter::create(std::move(over_limit));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::network
