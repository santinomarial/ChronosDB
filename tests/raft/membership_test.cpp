#include "chronos/raft/membership.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

TEST(MembershipCommandTest, CanonicallyRoundTripsJointAndFinalCommands) {
  auto joint = encode_membership_command_v1(
      JointMembershipCommand{.old_voters = {3U, 1U, 2U}, .new_voters = {4U, 2U, 3U}});
  ASSERT_TRUE(joint.has_value()) << joint.error().to_string();
  auto decoded_joint = decode_membership_command_v1(*joint);
  ASSERT_TRUE(decoded_joint.has_value()) << decoded_joint.error().to_string();
  EXPECT_EQ(std::get<JointMembershipCommand>(*decoded_joint),
            (JointMembershipCommand{{1U, 2U, 3U}, {2U, 3U, 4U}}));
  EXPECT_EQ(encode_membership_command_v1(*decoded_joint).value(), *joint);

  auto final = encode_membership_command_v1(
      FinalMembershipCommand{.joint_index = 9U, .new_voters = {4U, 2U, 3U}});
  ASSERT_TRUE(final.has_value());
  auto decoded_final = decode_membership_command_v1(*final);
  ASSERT_TRUE(decoded_final.has_value());
  EXPECT_EQ(std::get<FinalMembershipCommand>(*decoded_final),
            (FinalMembershipCommand{9U, {2U, 3U, 4U}}));
  EXPECT_EQ(encode_membership_command_v1(*decoded_final).value(), *final);
}

TEST(MembershipCommandTest, RejectsDamageInvalidIdentityAndBounds) {
  auto bytes = encode_membership_command_v1(
                   JointMembershipCommand{.old_voters = {1U}, .new_voters = {1U, 2U}})
                   .value();
  bytes[kMembershipCommandHeaderSize] ^= std::byte{0x01U};
  EXPECT_EQ(decode_membership_command_v1(bytes).error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(encode_membership_command_v1(
                JointMembershipCommand{.old_voters = {1U, 1U}, .new_voters = {1U, 2U}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      encode_membership_command_v1(FinalMembershipCommand{.joint_index = 0U, .new_voters = {1U}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(decode_membership_command_v1(
                encode_membership_command_v1(
                    JointMembershipCommand{.old_voters = {1U, 2U}, .new_voters = {2U, 3U}})
                    .value(),
                1U)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(encode_membership_command_v1(
                JointMembershipCommand{.old_voters = {1U, 2U}, .new_voters = {3U, 4U}}, 3U)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::raft
