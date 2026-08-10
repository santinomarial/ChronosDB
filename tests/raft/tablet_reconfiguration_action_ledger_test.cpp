#include "chronos/raft/tablet_reconfiguration_action_ledger.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-action-ledger-XXXXXX").string();
    if (char* created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] schema::TabletId tablet() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{3U};
  return schema::TabletId::from_bytes(bytes).value();
}
[[nodiscard]] schema::TabletId other_tablet() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{4U};
  return schema::TabletId::from_bytes(bytes).value();
}
[[nodiscard]] GroupId group() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{10U};
  return GroupId{bytes};
}
[[nodiscard]] TabletReconfigurationAction action() {
  return {{tablet(), 7U, TabletReconfigurationActionKind::kBeginJointMembership},
          TabletReconfigurationActionKind::kBeginJointMembership,
          {group(), BeginMembershipChangeOperation{{1U, 2U, 3U, 4U}}}};
}
[[nodiscard]] TabletReconfigurationActionLedgerConfig
config(const std::filesystem::path& path, const schema::TabletId owner = tablet()) {
  return {.directory_path = path.string(), .tablet_id = owner};
}

TEST(TabletReconfigurationActionLedgerTest, PreparesRetriesConflictsAndReopens) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  {
    auto ledger = TabletReconfigurationActionLedger::create(config(directory.path()));
    ASSERT_TRUE(ledger.has_value()) << ledger.error().to_string();
    auto locked = TabletReconfigurationActionLedger::open_existing(config(directory.path()));
    ASSERT_FALSE(locked.has_value());
    EXPECT_EQ(locked.error().code(), common::StatusCode::kUnavailable);
    auto prepared = ledger->prepare(action());
    ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
    EXPECT_FALSE(prepared->already_present);
    auto retry = ledger->prepare(action());
    ASSERT_TRUE(retry.has_value());
    EXPECT_TRUE(retry->already_present);
    auto conflict = action();
    std::get<BeginMembershipChangeOperation>(conflict.request.operation).new_voters = {1U, 2U, 4U};
    auto rejected = ledger->prepare(conflict);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  }
  {
    auto reopened = TabletReconfigurationActionLedger::open_existing(config(directory.path()));
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    auto loaded = reopened->load(action().id);
    ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
    EXPECT_EQ(loaded->action.id, action().id);
    EXPECT_EQ(std::get<BeginMembershipChangeOperation>(loaded->action.request.operation).new_voters,
              (std::vector<NodeId>{1U, 2U, 3U, 4U}));
  }
  auto wrong_owner =
      TabletReconfigurationActionLedger::open_existing(config(directory.path(), other_tablet()));
  ASSERT_TRUE(wrong_owner.has_value());
  TabletReconfigurationActionId foreign_id = action().id;
  foreign_id.tablet_id = other_tablet();
  auto foreign = wrong_owner->load(foreign_id);
  ASSERT_FALSE(foreign.has_value());
  EXPECT_EQ(foreign.error().code(), common::StatusCode::kCorruption);
}

TEST(TabletReconfigurationActionLedgerTest, UsesCanonicalIdentityBoundName) {
  EXPECT_EQ(*tablet_reconfiguration_action_file_name(action().id),
            "action-00000000000000000007-001.ract");
}

TEST(TabletReconfigurationActionLedgerTest, CleansTemporaryAndRejectsInstalledDamage) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  {
    auto ledger = TabletReconfigurationActionLedger::create(config(directory.path()));
    ASSERT_TRUE(ledger.has_value());
  }
  const std::string name = *tablet_reconfiguration_action_file_name(action().id);
  {
    std::ofstream temporary{directory.path() / (name + ".tmp"), std::ios::binary};
    temporary.put('x');
  }
  {
    auto ledger = TabletReconfigurationActionLedger::open_existing(config(directory.path()));
    ASSERT_TRUE(ledger.has_value()) << ledger.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(directory.path() / (name + ".tmp")));
    ASSERT_TRUE(ledger->prepare(action()).has_value());
  }
  {
    std::fstream file{directory.path() / name, std::ios::binary | std::ios::in | std::ios::out};
    ASSERT_TRUE(file.good());
    file.seekp(100);
    file.put('x');
  }
  auto ledger = TabletReconfigurationActionLedger::open_existing(config(directory.path()));
  ASSERT_TRUE(ledger.has_value());
  auto loaded = ledger->load(action().id);
  ASSERT_FALSE(loaded.has_value());
  EXPECT_EQ(loaded.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::raft
