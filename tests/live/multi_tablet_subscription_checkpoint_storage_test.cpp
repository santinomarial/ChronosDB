#include "chronos/live/multi_tablet_subscription_checkpoint_storage.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>

namespace chronos::live {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-subscription-checkpoint-XXXXXX")
            .string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
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

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::byte seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

[[nodiscard]] wal::WalId wal_id(const std::byte seed) {
  wal::WalId value{};
  value.bytes.fill(seed);
  return value;
}

struct Fixture {
  common::Uuid database_id{uuid(std::byte{1})};
  schema::TableId table_id{identifier<schema::TableId>(std::byte{2})};
  schema::TabletId tablet_a{identifier<schema::TabletId>(std::byte{3})};
  schema::TabletId tablet_b{identifier<schema::TabletId>(std::byte{4})};
  wal::WalId wal_a{wal_id(std::byte{5})};
  wal::WalId wal_b{wal_id(std::byte{6})};
  schema::SchemaId schema_id{identifier<schema::SchemaId>(std::byte{7})};
  PlanFingerprint plan{};

  Fixture() {
    plan.fill(std::byte{8});
  }

  [[nodiscard]] MultiTabletSubscriptionCheckpointStorageConfig
  config(const std::filesystem::path& directory) const {
    return {.directory_path = directory.string(),
            .identity = {database_id,
                         table_id,
                         plan,
                         schema_id,
                         schema::SchemaVersion::initial(),
                         {{tablet_a, wal_a}, {tablet_b, wal_b}}}};
  }

  [[nodiscard]] BoundMultiTabletSubscriptionCheckpoint
  checkpoint(const std::uint64_t generation) const {
    return {generation,
            {database_id,
             table_id,
             plan,
             schema_id,
             schema::SchemaVersion::initial(),
             {{{tablet_a, wal_a, 1U}, 0U}, {{tablet_b, wal_b, 1U}, 0U}},
             {{{tablet_a, wal_a, 1U},
               schema_id,
               schema::SchemaVersion::initial(),
               LogicalChangeOperation::kUpsert,
               {std::byte{9}},
               {std::byte{10}}},
              {{tablet_b, wal_b, 1U},
               schema_id,
               schema::SchemaVersion::initial(),
               LogicalChangeOperation::kDelete,
               {std::byte{11}},
               {}}}}};
  }
};

TEST(MultiTabletSubscriptionCheckpointStorageTest, InstallsSelectsAndReopensExactGenerations) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  Fixture fixture;
  {
    auto storage =
        MultiTabletSubscriptionCheckpointStorage::create(fixture.config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    const auto locked =
        MultiTabletSubscriptionCheckpointStorage::open_existing(fixture.config(directory.path()));
    ASSERT_FALSE(locked.has_value());
    EXPECT_EQ(locked.error().code(), common::StatusCode::kUnavailable);
    const auto installed = storage->install(fixture.checkpoint(1U));
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_FALSE(installed->already_present);
    const auto repeated = storage->install(fixture.checkpoint(1U));
    ASSERT_TRUE(repeated.has_value());
    EXPECT_TRUE(repeated->already_present);
    auto conflicting = fixture.checkpoint(1U);
    conflicting.state.retained_changes.front().payload = {std::byte{12}};
    const auto conflict = storage->install(conflicting);
    ASSERT_FALSE(conflict.has_value());
    EXPECT_EQ(conflict.error().code(), common::StatusCode::kCorruption);
    EXPECT_FALSE(storage->install(fixture.checkpoint(3U)).has_value());
    const auto second = storage->install(fixture.checkpoint(2U));
    ASSERT_TRUE(second.has_value()) << second.error().to_string();
    EXPECT_EQ(second->file_name, "generation-00000000000000000002.subc");
    const auto latest = storage->load_latest();
    ASSERT_TRUE(latest.has_value());
    ASSERT_TRUE(latest->has_value());
    EXPECT_EQ((*latest)->checkpoint, fixture.checkpoint(2U));
  }
  auto reopened =
      MultiTabletSubscriptionCheckpointStorage::open_existing(fixture.config(directory.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  const auto loaded = reopened->load_generation(1U);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  EXPECT_EQ(loaded->checkpoint, fixture.checkpoint(1U));
}

TEST(MultiTabletSubscriptionCheckpointStorageTest, RejectsCorruptInstalledGeneration) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  Fixture fixture;
  std::filesystem::path installed_path;
  {
    auto storage =
        MultiTabletSubscriptionCheckpointStorage::create(fixture.config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    const auto installed = storage->install(fixture.checkpoint(1U));
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    installed_path = directory.path() / installed->file_name;
  }
  {
    std::fstream file{installed_path, std::ios::binary | std::ios::in | std::ios::out};
    ASSERT_TRUE(file.good());
    file.seekp(100);
    file.put('x');
  }
  auto reopened =
      MultiTabletSubscriptionCheckpointStorage::open_existing(fixture.config(directory.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  const auto latest = reopened->load_latest();
  ASSERT_FALSE(latest.has_value());
  EXPECT_EQ(latest.error().code(), common::StatusCode::kCorruption);
}

TEST(MultiTabletSubscriptionCheckpointStorageTest, UsesCanonicalNonzeroGenerationNames) {
  EXPECT_FALSE(multi_tablet_subscription_checkpoint_generation_file_name(0U).has_value());
  EXPECT_EQ(*multi_tablet_subscription_checkpoint_generation_file_name(42U),
            "generation-00000000000000000042.subc");
}

TEST(MultiTabletSubscriptionCheckpointStorageTest, CleansRecognizedInterruptedTemporaryOnReopen) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  Fixture fixture;
  {
    auto storage =
        MultiTabletSubscriptionCheckpointStorage::create(fixture.config(directory.path()));
    ASSERT_TRUE(storage.has_value());
  }
  const std::string temporary =
      *multi_tablet_subscription_checkpoint_generation_file_name(1U) + ".tmp";
  {
    std::ofstream output{directory.path() / temporary, std::ios::binary};
    output.put('x');
  }
  auto reopened =
      MultiTabletSubscriptionCheckpointStorage::open_existing(fixture.config(directory.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(directory.path() / temporary));
}

} // namespace
} // namespace chronos::live
