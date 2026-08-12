#include "chronos/common/uuid_generator.hpp"

#include <gtest/gtest.h>
#include <set>

namespace chronos::common {
namespace {

TEST(SystemUuidGeneratorTest, ProducesNonnilDistinctDurableIdentities) {
  SystemUuidGenerator generator;
  std::set<Uuid> generated;
  for (std::size_t index = 0U; index < 32U; ++index) {
    Result<Uuid> id = generator.generate();
    ASSERT_TRUE(id.has_value()) << id.error().to_string();
    EXPECT_FALSE(id->is_nil());
    EXPECT_TRUE(generated.insert(*id).second);
  }
}

} // namespace
} // namespace chronos::common
