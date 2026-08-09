#include "chronos/network/reactor.hpp"

#include <gtest/gtest.h>

namespace chronos::network {
namespace {

TEST(ReactorBackendTest, SelectionNamesAreStableAndOptionalBackendIsExplicit) {
  EXPECT_EQ(reactor_backend_name(ReactorBackend::kEpoll), "epoll");
  EXPECT_EQ(reactor_backend_name(ReactorBackend::kIoUring), "io_uring");
  EXPECT_TRUE(reactor_backend_compiled(ReactorBackend::kEpoll));
#if !defined(CHRONOS_HAS_LIBURING)
  EXPECT_FALSE(reactor_backend_compiled(ReactorBackend::kIoUring));
#endif
}

} // namespace
} // namespace chronos::network
