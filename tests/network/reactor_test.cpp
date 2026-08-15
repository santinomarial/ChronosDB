#include "chronos/network/reactor.hpp"

#include <gtest/gtest.h>

namespace chronos::network {
namespace {

TEST(ReactorBackendTest, SelectionNamesAreStableAndOptionalBackendIsExplicit) {
  // Exercise the public validation boundary with a value that cannot name a compiled backend.
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  const auto unknown = static_cast<ReactorBackend>(0U);
  EXPECT_EQ(reactor_backend_name(ReactorBackend::kEpoll), "epoll");
  EXPECT_EQ(reactor_backend_name(ReactorBackend::kIoUring), "io_uring");
  EXPECT_EQ(reactor_backend_name(unknown), "unknown");
  EXPECT_TRUE(reactor_backend_compiled(ReactorBackend::kEpoll));
  EXPECT_FALSE(reactor_backend_compiled(unknown));
#if !defined(CHRONOS_HAS_LIBURING)
  EXPECT_FALSE(reactor_backend_compiled(ReactorBackend::kIoUring));
#endif

  const auto invalid = Reactor::start(unknown, {}, {});
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::network
