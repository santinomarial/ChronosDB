#include "chronos/live/durable_materialized_view.hpp"
#include "chronos/live/materialized_view_checkpoint_storage.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
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
        (std::filesystem::temp_directory_path() / "chronos-view-checkpoint-alloc-XXXXXX").string();
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
  common::Uuid view_id{uuid(std::byte{2})};
  schema::TableId table_id{identifier<schema::TableId>(std::byte{3})};
  schema::SchemaId schema_id{identifier<schema::SchemaId>(std::byte{4})};
  schema::TabletId tablet_id{identifier<schema::TabletId>(std::byte{5})};
  wal::WalId wal{wal_id(std::byte{6})};
  PlanFingerprint plan{};
  WindowDefinition definition{10, 10, 2, 16U, 16U};

  Fixture() {
    plan.fill(std::byte{7});
  }

  [[nodiscard]] MaterializedViewCheckpointIdentity identity() const {
    return {.database_id = database_id,
            .view_id = view_id,
            .table_id = table_id,
            .schema_id = schema_id,
            .schema_version = schema::SchemaVersion::initial(),
            .plan_fingerprint = plan};
  }

  [[nodiscard]] MaterializedViewCheckpointStorageConfig
  storage_config(const std::filesystem::path& directory) const {
    return {.directory_path = directory.string(), .identity = identity()};
  }

  [[nodiscard]] DurableWindowedMaterializedViewConfig
  durable_config(const std::filesystem::path& directory) const {
    return {.storage = storage_config(directory),
            .tablet_id = tablet_id,
            .wal_id = wal,
            .definition = definition};
  }

  [[nodiscard]] BoundMaterializedViewCheckpoint checkpoint() const {
    auto view = WindowedMaterializedView::create(tablet_id, wal, definition);
    EXPECT_TRUE(view.has_value()) << view.error().to_string();
    EXPECT_TRUE(view->apply_committed(SourcePosition{tablet_id, wal, 1U},
                                      MaterializedViewInput{{1U, 1, 1U, 10.0, 1.0}, false})
                    .has_value());
    return {.identity = identity(),
            .checkpoint_generation = 1U,
            .state = std::move(view->checkpoint().value())};
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

void expect_latest(const MaterializedViewCheckpointStorage& storage,
                   const BoundMaterializedViewCheckpoint& expected) {
  const auto latest = storage.load_latest();
  ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
  EXPECT_EQ(latest->transform(
                [](const LoadedMaterializedViewCheckpoint& loaded) { return loaded.checkpoint; }),
            std::optional<BoundMaterializedViewCheckpoint>{expected});
}

TEST(MaterializedViewCheckpointStorageAllocationFailureTest,
     InstallIsRecoverableAtEveryOwnedAllocation) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    TemporaryDirectory directory;
    ASSERT_FALSE(directory.path().empty());
    const Fixture fixture;
    const BoundMaterializedViewCheckpoint checkpoint = fixture.checkpoint();
    auto storage =
        MaterializedViewCheckpointStorage::create(fixture.storage_config(directory.path()));
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

TEST(MaterializedViewCheckpointStorageAllocationFailureTest,
     LoadsClassifyEveryOwnedAllocationWithoutPoisoningStorage) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const Fixture fixture;
  const BoundMaterializedViewCheckpoint checkpoint = fixture.checkpoint();
  auto storage =
      MaterializedViewCheckpointStorage::create(fixture.storage_config(directory.path()));
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
      EXPECT_EQ(loaded->transform([](const LoadedMaterializedViewCheckpoint& selected) {
        return selected.checkpoint;
      }),
                std::optional<BoundMaterializedViewCheckpoint>{checkpoint});
      latest_success = true;
      break;
    }
    EXPECT_EQ(loaded.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(storage->is_usable());
    expect_latest(*storage, checkpoint);
  }
  EXPECT_TRUE(latest_success);
}

TEST(DurableMaterializedViewAllocationFailureTest,
     CheckpointNeverPublishesAnUninstalledGenerationOrSequence) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    TemporaryDirectory directory;
    ASSERT_FALSE(directory.path().empty());
    const Fixture fixture;
    auto view =
        DurableWindowedMaterializedView::create_new(fixture.durable_config(directory.path()));
    ASSERT_TRUE(view.has_value()) << view.error().to_string();
    ASSERT_TRUE(view->apply_committed(SourcePosition{fixture.tablet_id, fixture.wal, 1U},
                                      MaterializedViewInput{{1U, 1, 1U, 10.0, 1.0}, false})
                    .has_value());
    ASSERT_TRUE(view->checkpoint().has_value());
    ASSERT_EQ(view->checkpoint_generation(), 1U);
    ASSERT_EQ(view->durable_record_sequence(), 1U);

    ASSERT_TRUE(view->apply_committed(SourcePosition{fixture.tablet_id, fixture.wal, 2U},
                                      MaterializedViewInput{{2U, 2, 2U, 20.0, 1.0}, false})
                    .has_value());
    std::size_t observed = 0U;
    auto second =
        run_with_allocation_failure(fail_after, observed, [&] { return view->checkpoint(); });
    EXPECT_GT(observed, 0U);
    if (second.has_value()) {
      EXPECT_EQ(second->checkpoint_generation, 2U);
      EXPECT_EQ(view->checkpoint_generation(), 2U);
      EXPECT_EQ(view->durable_record_sequence(), 2U);
      reached_success = true;
      break;
    }

    EXPECT_EQ(second.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(view->applied_position().record_sequence, 2U);
    EXPECT_EQ(view->checkpoint_generation(), 1U);
    EXPECT_EQ(view->durable_record_sequence(), 1U);

    const auto recovered = view->checkpoint();
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    EXPECT_EQ(recovered->checkpoint_generation, 2U);
    EXPECT_EQ(view->checkpoint_generation(), 2U);
    EXPECT_EQ(view->durable_record_sequence(), 2U);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::live
