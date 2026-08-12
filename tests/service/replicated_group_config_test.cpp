#include "chronos/service/replicated_group_config.hpp"

#include <gtest/gtest.h>
#include <string>
#include <string_view>

namespace chronos::service {
namespace {

TEST(ReplicatedGroupConfigTest, ParsesAndSortsStrictBoundedMembership) {
  const std::string text = "CHRONOSDB_REPLICATED_GROUPS_V1\n"
                           "22222222-2222-2222-2222-222222222222=1,3,9\n"
                           "11111111-1111-1111-1111-111111111111=2\n";
  auto parsed = parse_replicated_group_config(text);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().to_string();
  ASSERT_EQ(parsed->size(), 2U);
  EXPECT_EQ(std::to_integer<unsigned>((*parsed)[0].group_id.bytes().front()), 0x11U);
  EXPECT_EQ((*parsed)[0].voters, (std::vector<raft::NodeId>{2U}));
  EXPECT_EQ(std::to_integer<unsigned>((*parsed)[1].group_id.bytes().front()), 0x22U);
  EXPECT_EQ((*parsed)[1].voters, (std::vector<raft::NodeId>{1U, 3U, 9U}));
}

TEST(ReplicatedGroupConfigTest, RejectsNoncanonicalDamageAndLimits) {
  constexpr std::string_view prefix{"CHRONOSDB_REPLICATED_GROUPS_V1\n"};
  const auto reject = [&](const std::string_view suffix, const common::StatusCode expected =
                                                             common::StatusCode::kInvalidArgument) {
    auto parsed = parse_replicated_group_config(std::string{prefix} + std::string{suffix});
    ASSERT_FALSE(parsed.has_value()) << suffix;
    EXPECT_EQ(parsed.error().code(), expected) << suffix;
  };
  reject("");
  reject("11111111-1111-1111-1111-111111111111=0");
  reject("11111111-1111-1111-1111-111111111111=01");
  reject("11111111-1111-1111-1111-111111111111=2,2");
  reject("11111111-1111-1111-1111-111111111111=2,1");
  reject("11111111-1111-1111-1111-11111111111A=1");
  reject("00000000-0000-0000-0000-000000000000=1");
  reject("11111111-1111-1111-1111-111111111111=1\n\n");
  reject("11111111-1111-1111-1111-111111111111=1\r\n");
  reject("11111111-1111-1111-1111-111111111111=1\n"
         "11111111-1111-1111-1111-111111111111=2");
  auto voters = parse_replicated_group_config(std::string{prefix} +
                                                  "11111111-1111-1111-1111-111111111111=1,2",
                                              {.maximum_voters_per_group = 1U});
  ASSERT_FALSE(voters.has_value());
  EXPECT_EQ(voters.error().code(), common::StatusCode::kResourceExhausted);
  auto groups =
      parse_replicated_group_config(std::string{prefix} + "11111111-1111-1111-1111-111111111111=1\n"
                                                          "22222222-2222-2222-2222-222222222222=2",
                                    {.maximum_groups = 1U});
  ASSERT_FALSE(groups.has_value());
  EXPECT_EQ(groups.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::service
