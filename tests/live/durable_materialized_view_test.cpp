#include "chronos/live/durable_materialized_view.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
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
        (std::filesystem::temp_directory_path() / "chronos-durable-view-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
    }
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet_id() {
  return schema::TabletId::from_uuid(uuid(6U)).value();
}

[[nodiscard]] wal::WalId wal_id() {
  wal::WalId wal;
  wal.bytes.fill(std::byte{0x71U});
  return wal;
}

[[nodiscard]] DurableWindowedMaterializedViewConfig config(const TemporaryDirectory& directory) {
  PlanFingerprint plan{};
  plan.fill(std::byte{0x72U});
  return {.storage = {.directory_path = directory.path().string(),
                      .identity = {.database_id = uuid(1U),
                                   .view_id = uuid(2U),
                                   .table_id = schema::TableId::from_uuid(uuid(3U)).value(),
                                   .schema_id = schema::SchemaId::from_uuid(uuid(4U)).value(),
                                   .schema_version = schema::SchemaVersion::initial(),
                                   .plan_fingerprint = plan}},
          .tablet_id = tablet_id(),
          .wal_id = wal_id(),
          .definition = {10, 10, 2, 16U, 16U}};
}

TEST(DurableMaterializedViewTest, CheckpointsWatermarkAndReplaysCommittedSuffixAfterReopen) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  {
    auto view = DurableWindowedMaterializedView::create_new(config(directory));
    ASSERT_TRUE(view.has_value()) << view.error().to_string();
    ASSERT_TRUE(view->apply_committed(SourcePosition{tablet_id(), wal_id(), 1U},
                                      MaterializedViewInput{{1U, 1, 1U, 10.0, 1.0}, false})
                    .has_value());
    EXPECT_EQ(view->durable_record_sequence(), 0U);
    auto first = view->checkpoint();
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    EXPECT_EQ(first->checkpoint_generation, 1U);
    EXPECT_EQ(view->durable_record_sequence(), 1U);
    auto repeated = view->checkpoint();
    ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
    EXPECT_EQ(repeated->checkpoint_generation, 1U);
    EXPECT_TRUE(repeated->already_present);

    ASSERT_TRUE(view->advance_watermark(12).has_value());
    auto watermarked = view->checkpoint();
    ASSERT_TRUE(watermarked.has_value()) << watermarked.error().to_string();
    EXPECT_EQ(watermarked->checkpoint_generation, 2U);
    EXPECT_EQ(watermarked->record_sequence, 1U);
  }

  {
    auto reopened = DurableWindowedMaterializedView::open_existing(config(directory));
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_EQ(reopened->checkpoint_generation(), 2U);
    EXPECT_EQ(reopened->durable_record_sequence(), 1U);
    EXPECT_EQ(reopened->applied_position().record_sequence, 1U);
    EXPECT_EQ(reopened->watermark(), 12);
    EXPECT_EQ(reopened->retained_rows(), 1U);
    EXPECT_EQ(reopened
                  ->apply_committed(SourcePosition{tablet_id(), wal_id(), 3U},
                                    MaterializedViewInput{{3U, 3, 3U, 30.0, 1.0}, false})
                  .error()
                  .code(),
              common::StatusCode::kInvalidArgument);
    ASSERT_TRUE(reopened
                    ->apply_committed(SourcePosition{tablet_id(), wal_id(), 2U},
                                      MaterializedViewInput{{2U, 2, 2U, 20.0, 1.0}, false})
                    .has_value());
    auto third = reopened->checkpoint();
    ASSERT_TRUE(third.has_value()) << third.error().to_string();
    EXPECT_EQ(third->checkpoint_generation, 3U);
    EXPECT_EQ(third->record_sequence, 2U);
  }

  {
    auto final = DurableWindowedMaterializedView::open_existing(config(directory));
    ASSERT_TRUE(final.has_value()) << final.error().to_string();
    EXPECT_EQ(final->checkpoint_generation(), 3U);
    EXPECT_EQ(final->durable_record_sequence(), 2U);
    EXPECT_EQ(final->retained_rows(), 2U);
  }
  EXPECT_EQ(DurableWindowedMaterializedView::create_new(config(directory)).error().code(),
            common::StatusCode::kAlreadyExists);
}

} // namespace
} // namespace chronos::live
