#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/tablet_reconfiguration_action_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] GroupId group(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return GroupId{bytes};
}

[[nodiscard]] TabletReconfigurationAction begin_action() {
  return {{id<schema::TabletId>(3U), 7U, TabletReconfigurationActionKind::kBeginJointMembership},
          TabletReconfigurationActionKind::kBeginJointMembership,
          {group(10U), BeginMembershipChangeOperation{{1U, 2U, 3U, 4U}}}};
}

[[nodiscard]] TabletReconfigurationAction finalize_action() {
  return {{id<schema::TabletId>(3U), 7U, TabletReconfigurationActionKind::kFinalizeJointMembership},
          TabletReconfigurationActionKind::kFinalizeJointMembership,
          {group(10U), FinalizeMembershipChangeOperation{}}};
}

[[nodiscard]] TabletReconfigurationAction placement_action() {
  auto payload = encode_metadata_command_v1(TabletPlacementMetadata{
      id<schema::TableId>(2U), id<schema::TabletId>(3U), 8U, {1U, 2U, 3U, 4U}, 1U});
  EXPECT_TRUE(payload.has_value());
  return {{id<schema::TabletId>(3U), 7U, TabletReconfigurationActionKind::kPublishPlacement},
          TabletReconfigurationActionKind::kPublishPlacement,
          {group(11U), ProposeOperation{kRaftMetadataCommandEntryType, std::move(*payload)}}};
}

TEST(TabletReconfigurationActionCodecTest, RoundTripsEverySupportedActionExactly) {
  for (TabletReconfigurationAction action :
       {begin_action(), finalize_action(), placement_action()}) {
    auto encoded = encode_tablet_reconfiguration_action_v1(action);
    ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
    auto decoded = decode_tablet_reconfiguration_action_v1(*encoded);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
    EXPECT_EQ(decoded->id, action.id);
    EXPECT_EQ(decoded->kind, action.kind);
    EXPECT_EQ(decoded->request.group_id, action.request.group_id);
    if (action.kind == TabletReconfigurationActionKind::kBeginJointMembership) {
      EXPECT_EQ(std::get<BeginMembershipChangeOperation>(decoded->request.operation).new_voters,
                std::get<BeginMembershipChangeOperation>(action.request.operation).new_voters);
    } else if (action.kind == TabletReconfigurationActionKind::kFinalizeJointMembership) {
      EXPECT_TRUE(
          std::holds_alternative<FinalizeMembershipChangeOperation>(decoded->request.operation));
    } else {
      const auto& decoded_proposal = std::get<ProposeOperation>(decoded->request.operation);
      const auto& proposal = std::get<ProposeOperation>(action.request.operation);
      EXPECT_EQ(decoded_proposal.type, proposal.type);
      EXPECT_EQ(decoded_proposal.payload, proposal.payload);
    }
  }
}

TEST(TabletReconfigurationActionCodecTest, RejectsDamageMismatchAndUnsupportedOperations) {
  auto encoded = encode_tablet_reconfiguration_action_v1(begin_action()).value();
  encoded[kTabletReconfigurationActionHeaderSize] ^= std::byte{1U};
  auto damaged = decode_tablet_reconfiguration_action_v1(encoded);
  ASSERT_FALSE(damaged.has_value());
  EXPECT_EQ(damaged.error().code(), common::StatusCode::kCorruption);

  auto mismatched = begin_action();
  mismatched.id.kind = TabletReconfigurationActionKind::kPublishPlacement;
  auto mismatch = encode_tablet_reconfiguration_action_v1(mismatched);
  ASSERT_FALSE(mismatch.has_value());
  EXPECT_EQ(mismatch.error().code(), common::StatusCode::kInvalidArgument);

  auto unsupported = begin_action();
  unsupported.request.operation = HeartbeatOperation{};
  auto rejected = encode_tablet_reconfiguration_action_v1(unsupported);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);

  auto wrong_placement = placement_action();
  auto wrong_payload = encode_metadata_command_v1(TabletPlacementMetadata{
      id<schema::TableId>(2U), id<schema::TabletId>(9U), 8U, {1U, 2U, 3U}, 1U});
  ASSERT_TRUE(wrong_payload.has_value());
  std::get<ProposeOperation>(wrong_placement.request.operation).payload = std::move(*wrong_payload);
  auto foreign = encode_tablet_reconfiguration_action_v1(wrong_placement);
  ASSERT_FALSE(foreign.has_value());
  EXPECT_EQ(foreign.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::raft
