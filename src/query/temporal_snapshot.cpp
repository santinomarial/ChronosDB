#include "chronos/query/temporal_snapshot.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

struct ByteVectorLess {
  [[nodiscard]] bool operator()(const std::vector<std::byte>& left,
                                const std::vector<std::byte>& right) const noexcept {
    return std::lexicographical_compare(left.begin(), left.end(), right.begin(), right.end(),
                                        [](const std::byte lhs, const std::byte rhs) {
                                          return std::to_integer<unsigned int>(lhs) <
                                                 std::to_integer<unsigned int>(rhs);
                                        });
  }
};

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

} // namespace

class TemporalSnapshotProvider::Impl {
public:
  struct Version {
    TemporalMutation mutation;
    std::uint64_t commit_position{};
    std::int64_t commit_time_ns{};
  };

  Impl(std::shared_ptr<const schema::TableSchema> schema_value, TemporalStoreLimits limits_value)
      : schema(std::move(schema_value)), limits(limits_value) {}

  std::shared_ptr<const schema::TableSchema> schema;
  TemporalStoreLimits limits;
  mutable std::mutex mutex;
  std::map<std::vector<std::byte>, std::vector<Version>, ByteVectorLess> histories;
  std::map<std::int64_t, std::uint64_t> time_to_position;
  std::uint64_t latest_position{};
  std::int64_t latest_time{};
  std::optional<std::int64_t> earliest_retained_time;
  std::size_t versions{};
};

TemporalSnapshotProvider::TemporalSnapshotProvider(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
TemporalSnapshotProvider::~TemporalSnapshotProvider() = default;
common::Result<std::unique_ptr<TemporalSnapshotProvider>>
TemporalSnapshotProvider::create(std::shared_ptr<const schema::TableSchema> schema,
                                 const TemporalStoreLimits limits) {
  if (schema == nullptr) {
    return common::make_unexpected(invalid("temporal store schema is required"));
  }
  if (limits.maximum_logical_rows == 0U || limits.maximum_versions == 0U ||
      limits.maximum_identity_bytes == 0U ||
      limits.maximum_logical_rows > limits.maximum_versions) {
    return common::make_unexpected(invalid("temporal store limits are invalid"));
  }
  return std::unique_ptr<TemporalSnapshotProvider>{
      new TemporalSnapshotProvider{std::make_unique<Impl>(std::move(schema), limits)}};
}

common::Status TemporalSnapshotProvider::apply_committed(const std::uint64_t system_commit_position,
                                                         const std::int64_t system_commit_time_ns,
                                                         std::vector<TemporalMutation> mutations) {
  if (system_commit_position == 0U || mutations.empty()) {
    return invalid("temporal commit position and mutation batch must be nonzero");
  }
  std::scoped_lock lock{impl_->mutex};
  if (system_commit_position <= impl_->latest_position ||
      (impl_->latest_position != 0U && system_commit_time_ns < impl_->latest_time)) {
    return invalid("temporal commits must advance position and nondecreasing system time");
  }
  if (mutations.size() > impl_->limits.maximum_versions - impl_->versions) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "temporal version capacity is exhausted"};
  }

  std::set<std::vector<std::byte>, ByteVectorLess> batch_identities;
  std::size_t new_identities = 0U;
  for (const TemporalMutation& mutation : mutations) {
    if (mutation.logical_identity.empty() ||
        mutation.logical_identity.size() > impl_->limits.maximum_identity_bytes ||
        mutation.wal_id.is_nil() || mutation.record_sequence == 0U ||
        mutation.columns.size() != impl_->schema->columns().size()) {
      return invalid("temporal mutation identity, source position, or row shape is invalid");
    }
    if (!batch_identities.insert(mutation.logical_identity).second) {
      return invalid("one temporal commit batch contains duplicate logical identities");
    }
    const bool exists = impl_->histories.contains(mutation.logical_identity);
    if (!exists) {
      ++new_identities;
      if (mutation.kind != TemporalMutationKind::kOriginal) {
        return invalid("correction, replacement, or tombstone requires an existing identity");
      }
    } else if (mutation.kind == TemporalMutationKind::kOriginal) {
      return invalid("an existing temporal identity requires correction or replacement semantics");
    }
    if (mutation.kind != TemporalMutationKind::kTombstone) {
      const std::vector<std::byte> generated_identity = impl_->schema->deduplication_key().empty()
                                                            ? mutation.logical_identity
                                                            : std::vector<std::byte>{};
      auto validated = ScalarTableSnapshot::create(
          impl_->schema, system_commit_position,
          {ScalarInputRow{mutation.columns, generated_identity, mutation.wal_id,
                          mutation.record_sequence, system_commit_position, mutation.row_ordinal}});
      if (!validated.has_value()) {
        return validated.error();
      }
    }
  }
  if (new_identities > impl_->limits.maximum_logical_rows - impl_->histories.size()) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "temporal logical-row capacity is exhausted"};
  }

  for (TemporalMutation& mutation : mutations) {
    auto& history = impl_->histories[mutation.logical_identity];
    history.push_back(
        Impl::Version{std::move(mutation), system_commit_position, system_commit_time_ns});
    ++impl_->versions;
  }
  impl_->latest_position = system_commit_position;
  impl_->latest_time = system_commit_time_ns;
  impl_->time_to_position.insert_or_assign(system_commit_time_ns, system_commit_position);
  return common::Status::ok();
}

common::Result<std::shared_ptr<const ScalarTableSnapshot>>
TemporalSnapshotProvider::resolve(const std::shared_ptr<const schema::TableSchema>& bound_schema,
                                  const std::optional<std::int64_t> as_of_system_time_ns) const {
  if (bound_schema == nullptr || bound_schema->schema_id() != impl_->schema->schema_id() ||
      bound_schema->version() != impl_->schema->version()) {
    return common::make_unexpected(invalid("temporal snapshot schema is incompatible"));
  }
  std::scoped_lock lock{impl_->mutex};
  if (as_of_system_time_ns.has_value() && impl_->earliest_retained_time.has_value() &&
      *as_of_system_time_ns < *impl_->earliest_retained_time) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "requested system history has expired"});
  }

  std::uint64_t boundary = impl_->latest_position;
  if (as_of_system_time_ns.has_value()) {
    const auto later = impl_->time_to_position.upper_bound(*as_of_system_time_ns);
    boundary = later == impl_->time_to_position.begin() ? 0U : std::prev(later)->second;
  }
  std::vector<ScalarInputRow> visible;
  visible.reserve(impl_->histories.size());
  for (const auto& [identity, history] : impl_->histories) {
    const auto later =
        std::upper_bound(history.begin(), history.end(), boundary,
                         [](const std::uint64_t position, const Impl::Version& version) {
                           return position < version.commit_position;
                         });
    if (later == history.begin()) {
      continue;
    }
    const Impl::Version& version = *std::prev(later);
    if (version.mutation.kind == TemporalMutationKind::kTombstone) {
      continue;
    }
    visible.push_back(ScalarInputRow{
        version.mutation.columns,
        impl_->schema->deduplication_key().empty() ? identity : std::vector<std::byte>{},
        version.mutation.wal_id, version.mutation.record_sequence, version.commit_position,
        version.mutation.row_ordinal});
  }
  auto snapshot = ScalarTableSnapshot::create(impl_->schema, boundary, std::move(visible));
  if (!snapshot.has_value()) {
    return common::make_unexpected(snapshot.error());
  }
  std::shared_ptr<const ScalarTableSnapshot> output =
      std::make_shared<const ScalarTableSnapshot>(std::move(*snapshot));
  return output;
}

common::Status
TemporalSnapshotProvider::compact_history(const std::uint64_t oldest_observable_commit_position,
                                          const std::int64_t retained_system_time_ns) {
  std::scoped_lock lock{impl_->mutex};
  if (oldest_observable_commit_position > impl_->latest_position ||
      retained_system_time_ns > impl_->latest_time) {
    return invalid("temporal retention boundary is ahead of committed state");
  }
  for (auto& [identity, history] : impl_->histories) {
    static_cast<void>(identity);
    const auto keep =
        std::lower_bound(history.begin(), history.end(), oldest_observable_commit_position,
                         [](const Impl::Version& version, const std::uint64_t position) {
                           return version.commit_position < position;
                         });
    if (keep == history.begin()) {
      continue;
    }
    auto predecessor = std::prev(keep);
    while (predecessor != history.begin() &&
           predecessor->commit_time_ns >= retained_system_time_ns) {
      --predecessor;
    }
    const std::size_t removed =
        static_cast<std::size_t>(std::distance(history.begin(), predecessor));
    history.erase(history.begin(), predecessor);
    impl_->versions -= removed;
  }
  impl_->earliest_retained_time = retained_system_time_ns;
  while (!impl_->time_to_position.empty() &&
         impl_->time_to_position.begin()->first < retained_system_time_ns) {
    impl_->time_to_position.erase(impl_->time_to_position.begin());
  }
  return common::Status::ok();
}

std::uint64_t TemporalSnapshotProvider::latest_commit_position() const noexcept {
  std::scoped_lock lock{impl_->mutex};
  return impl_->latest_position;
}

std::size_t TemporalSnapshotProvider::logical_row_count() const noexcept {
  std::scoped_lock lock{impl_->mutex};
  return impl_->histories.size();
}

std::size_t TemporalSnapshotProvider::version_count() const noexcept {
  std::scoped_lock lock{impl_->mutex};
  return impl_->versions;
}

} // namespace chronos::query
