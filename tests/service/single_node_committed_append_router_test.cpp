#include "chronos/service/single_node_committed_append_router.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <utility>

namespace chronos::service {
namespace {

class RecordingObserver final : public SingleNodeCommittedAppendObserver {
public:
  void on_applied(AppliedSingleNodeColumnarAppend append) noexcept override {
    ++calls;
    last.emplace(std::move(append));
  }

  std::size_t calls{};
  std::optional<AppliedSingleNodeColumnarAppend> last;
};

[[nodiscard]] AppliedSingleNodeColumnarAppend append() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{1};
  return {.tablet_id = schema::TabletId::from_uuid(common::Uuid{bytes}).value(),
          .position = {},
          .batch = {},
          .outcome = {}};
}

TEST(SingleNodeCommittedAppendRouterTest, BindsOneStableDelegateAndReportsUnboundAppends) {
  SingleNodeCommittedAppendRouter router;
  RecordingObserver first;
  RecordingObserver second;

  router.on_applied(append());
  EXPECT_EQ(router.metrics().unbound_appends, 1U);
  ASSERT_TRUE(router.bind(first).is_ok());
  EXPECT_FALSE(router.bind(second).is_ok());
  EXPECT_FALSE(router.bind(router).is_ok());

  router.on_applied(append());
  EXPECT_EQ(first.calls, 1U);
  EXPECT_EQ(second.calls, 0U);
  EXPECT_EQ(router.metrics().forwarded_appends, 1U);
  EXPECT_TRUE(router.metrics().bound);

  router.unbind(second);
  EXPECT_TRUE(router.metrics().bound);
  router.unbind(first);
  EXPECT_FALSE(router.metrics().bound);
  router.on_applied(append());
  EXPECT_EQ(router.metrics().unbound_appends, 2U);
}

} // namespace
} // namespace chronos::service
