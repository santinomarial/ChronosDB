#include "chronos/live/multi_tablet_subscription_checkpoint_storage.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace chronos::live {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-subscription-storage-alloc-XXXXXX")
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
  schema::TabletId tablet_id{identifier<schema::TabletId>(std::byte{3})};
  wal::WalId wal{wal_id(std::byte{4})};
  schema::SchemaId schema_id{identifier<schema::SchemaId>(std::byte{5})};
  PlanFingerprint plan{};

  Fixture() {
    plan.fill(std::byte{6});
  }

  [[nodiscard]] MultiTabletSubscriptionCheckpointStorageConfig
  config(const std::filesystem::path& directory) const {
    return {.directory_path = directory.string(),
            .identity = {database_id,
                         table_id,
                         plan,
                         schema_id,
                         schema::SchemaVersion::initial(),
                         {{tablet_id, wal}}}};
  }

  [[nodiscard]] BoundMultiTabletSubscriptionCheckpoint checkpoint() const {
    return {1U,
            {database_id,
             table_id,
             plan,
             schema_id,
             schema::SchemaVersion::initial(),
             {{{tablet_id, wal, 1U}, 0U}},
             {{{tablet_id, wal, 1U},
               schema_id,
               schema::SchemaVersion::initial(),
               LogicalChangeOperation::kUpsert,
               {std::byte{7}},
               {std::byte{8}}}}}};
  }
};

template <typename Operation>
[[nodiscard]] auto run_with_allocation_failure(const std::size_t fail_after, std::size_t& observed,
                                               Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    observed = failure.observed_allocations();
    failure.disable();
  }
  return std::move(*result);
}

void expect_latest(const MultiTabletSubscriptionCheckpointStorage& storage,
                   const BoundMultiTabletSubscriptionCheckpoint& expected) {
  const auto latest = storage.load_latest();
  ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
  EXPECT_EQ(latest->transform([](const LoadedMultiTabletSubscriptionCheckpoint& loaded) {
    return loaded.checkpoint;
  }),
            std::optional<BoundMultiTabletSubscriptionCheckpoint>{expected});
}

TEST(MultiTabletSubscriptionCheckpointStorageAllocationFailureTest,
     InstallIsRecoverableAtEveryOwnedAllocation) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    TemporaryDirectory directory;
    ASSERT_FALSE(directory.path().empty());
    const Fixture fixture;
    const BoundMultiTabletSubscriptionCheckpoint checkpoint = fixture.checkpoint();
    auto storage =
        MultiTabletSubscriptionCheckpointStorage::create(fixture.config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();

    std::size_t observed = 0U;
    auto installed = run_with_allocation_failure(fail_after, observed,
                                                 [&] { return storage->install(checkpoint); });
    EXPECT_GT(observed, 0U);
    if (installed.has_value()) {
      EXPECT_FALSE(installed->already_present);
      expect_latest(*storage, checkpoint);
      reached_success = true;
      break;
    }

    EXPECT_EQ(installed.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(storage->is_usable());
    const auto recovered = storage->install(checkpoint);
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    expect_latest(*storage, checkpoint);
  }
  EXPECT_TRUE(reached_success);
}

TEST(MultiTabletSubscriptionCheckpointStorageAllocationFailureTest,
     LoadsClassifyEveryOwnedAllocationWithoutPoisoningStorage) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const Fixture fixture;
  const BoundMultiTabletSubscriptionCheckpoint checkpoint = fixture.checkpoint();
  auto storage = MultiTabletSubscriptionCheckpointStorage::create(fixture.config(directory.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  ASSERT_TRUE(storage->install(checkpoint).has_value());

  bool generation_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto loaded = run_with_allocation_failure(fail_after, observed,
                                              [&] { return storage->load_generation(1U); });
    EXPECT_GT(observed, 0U);
    if (loaded.has_value()) {
      EXPECT_EQ(loaded->checkpoint, checkpoint);
      generation_success = true;
      break;
    }
    EXPECT_EQ(loaded.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(storage->is_usable());
    expect_latest(*storage, checkpoint);
  }
  EXPECT_TRUE(generation_success);

  bool latest_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto loaded =
        run_with_allocation_failure(fail_after, observed, [&] { return storage->load_latest(); });
    EXPECT_GT(observed, 0U);
    if (loaded.has_value()) {
      EXPECT_EQ(loaded->transform([](const LoadedMultiTabletSubscriptionCheckpoint& selected) {
        return selected.checkpoint;
      }),
                std::optional<BoundMultiTabletSubscriptionCheckpoint>{checkpoint});
      latest_success = true;
      break;
    }
    EXPECT_EQ(loaded.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(storage->is_usable());
    expect_latest(*storage, checkpoint);
  }
  EXPECT_TRUE(latest_success);
}

} // namespace
} // namespace chronos::live
