#include "chronos/query/temporal_snapshot.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
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

[[nodiscard]] common::Status corruption(const char* message) {
  return common::Status{common::StatusCode::kCorruption, message};
}

[[nodiscard]] bool equal_scalar(const ScalarValue& left, const ScalarValue& right) {
  if (left.type() != right.type() || left.storage().index() != right.storage().index()) {
    return false;
  }
  if (const auto* left_float = std::get_if<float>(&left.storage()); left_float != nullptr) {
    return std::bit_cast<std::uint32_t>(*left_float) ==
           std::bit_cast<std::uint32_t>(std::get<float>(right.storage()));
  }
  if (const auto* left_double = std::get_if<double>(&left.storage()); left_double != nullptr) {
    return std::bit_cast<std::uint64_t>(*left_double) ==
           std::bit_cast<std::uint64_t>(std::get<double>(right.storage()));
  }
  return left.storage() == right.storage();
}

[[nodiscard]] bool equal_mutation(const TemporalMutation& left, const TemporalMutation& right) {
  return left.logical_identity == right.logical_identity &&
         left.event_time_ns == right.event_time_ns &&
         left.receive_time_ns == right.receive_time_ns && left.wal_id == right.wal_id &&
         left.record_sequence == right.record_sequence && left.row_ordinal == right.row_ordinal &&
         left.kind == right.kind && left.columns.size() == right.columns.size() &&
         std::ranges::equal(left.columns, right.columns, equal_scalar);
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

  [[nodiscard]] common::Status validate(const std::uint64_t system_commit_position,
                                        const std::int64_t system_commit_time_ns,
                                        const std::span<const TemporalMutation> mutations,
                                        const bool require_source_position,
                                        const bool allow_retained_predecessor = false) const {
    if (failed) {
      return common::Status{common::StatusCode::kUnavailable,
                            "temporal provider failed closed and requires recovery"};
    }
    if (system_commit_position == 0U || mutations.empty() ||
        system_commit_position <= latest_position ||
        (latest_position != 0U && system_commit_time_ns < latest_time)) {
      return invalid("temporal commits must advance position and nondecreasing system time");
    }
    if (mutations.size() > limits.maximum_versions - versions) {
      return common::Status{common::StatusCode::kResourceExhausted,
                            "temporal version capacity is exhausted"};
    }

    std::set<std::vector<std::byte>, ByteVectorLess> batch_identities;
    std::size_t new_identities = 0U;
    for (const TemporalMutation& mutation : mutations) {
      if (mutation.logical_identity.empty() ||
          mutation.logical_identity.size() > limits.maximum_identity_bytes ||
          (require_source_position &&
           (mutation.wal_id.is_nil() || mutation.record_sequence == 0U)) ||
          mutation.columns.size() != schema->columns().size() ||
          mutation.kind < TemporalMutationKind::kOriginal ||
          mutation.kind > TemporalMutationKind::kTombstone) {
        return invalid("temporal mutation identity, source position, or row shape is invalid");
      }
      if (!batch_identities.insert(mutation.logical_identity).second) {
        return invalid("one temporal commit batch contains duplicate logical identities");
      }
      const bool exists = histories.contains(mutation.logical_identity);
      if (!exists) {
        ++new_identities;
        if (!allow_retained_predecessor && mutation.kind != TemporalMutationKind::kOriginal) {
          return invalid("correction, replacement, or tombstone requires an existing identity");
        }
      } else if (mutation.kind == TemporalMutationKind::kOriginal) {
        return invalid(
            "an existing temporal identity requires correction or replacement semantics");
      }
      if (mutation.kind != TemporalMutationKind::kTombstone) {
        const std::vector<std::byte> generated_identity = schema->deduplication_key().empty()
                                                              ? mutation.logical_identity
                                                              : std::vector<std::byte>{};
        auto validated = ScalarTableSnapshot::create(
            schema, system_commit_position,
            {ScalarInputRow{mutation.columns, generated_identity,
                            require_source_position ? mutation.wal_id : schema->schema_id().uuid(),
                            require_source_position ? mutation.record_sequence
                                                    : system_commit_position,
                            system_commit_position, mutation.row_ordinal}});
        if (!validated.has_value()) {
          return validated.error();
        }
      }
    }
    if (new_identities > limits.maximum_logical_rows - histories.size()) {
      return common::Status{common::StatusCode::kResourceExhausted,
                            "temporal logical-row capacity is exhausted"};
    }
    return common::Status::ok();
  }

  std::shared_ptr<const schema::TableSchema> schema;
  TemporalStoreLimits limits;
  mutable std::mutex mutex;
  std::map<std::vector<std::byte>, std::vector<Version>, ByteVectorLess> histories;
  std::map<std::int64_t, std::uint64_t> time_to_position;
  std::uint64_t latest_position{};
  std::int64_t latest_time{};
  std::optional<std::uint64_t> oldest_observable_position;
  std::optional<std::int64_t> earliest_retained_time;
  std::size_t versions{};
  bool failed{false};
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
  try {
    std::scoped_lock lock{impl_->mutex};
    const common::Status validation =
        impl_->validate(system_commit_position, system_commit_time_ns, mutations, true);
    if (!validation.is_ok()) {
      return validation;
    }

    // Stage the complete next state before publication. Copy-on-commit is intentionally preferred
    // over a partially installed history if any allocation fails; profiling and a persistent
    // copy-on-write representation remain later optimization work.
    auto staged_histories = impl_->histories;
    auto staged_time_to_position = impl_->time_to_position;
    for (TemporalMutation& mutation : mutations) {
      staged_histories[mutation.logical_identity].push_back(
          Impl::Version{std::move(mutation), system_commit_position, system_commit_time_ns});
    }
    staged_time_to_position.insert_or_assign(system_commit_time_ns, system_commit_position);
    impl_->histories.swap(staged_histories);
    impl_->time_to_position.swap(staged_time_to_position);
    impl_->versions += mutations.size();
    impl_->latest_position = system_commit_position;
    impl_->latest_time = system_commit_time_ns;
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "temporal commit staging allocation failed"};
  } catch (const std::length_error&) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "temporal commit staging exceeded container limits"};
  }
}

common::Status
TemporalSnapshotProvider::restore_retained_history(const std::int64_t retained_system_time_ns,
                                                   std::vector<RetainedTemporalVersion> versions) {
  if (versions.empty()) {
    return invalid("retained temporal restore requires at least one version");
  }
  try {
    std::scoped_lock lock{impl_->mutex};
    if (impl_->failed || impl_->latest_position != 0U || impl_->versions != 0U ||
        !impl_->histories.empty() || !impl_->time_to_position.empty() ||
        impl_->earliest_retained_time.has_value()) {
      return invalid("retained temporal restore requires one fresh provider");
    }

    Impl staged{impl_->schema, impl_->limits};
    const common::Uuid source_id = versions.front().mutation.wal_id;
    std::size_t begin = 0U;
    while (begin < versions.size()) {
      const std::uint64_t position = versions[begin].system_commit_position;
      const std::int64_t commit_time = versions[begin].system_commit_time_ns;
      std::size_t end = begin + 1U;
      while (end < versions.size() && versions[end].system_commit_position == position) {
        ++end;
      }
      if (position == 0U || source_id.is_nil() ||
          (begin != 0U && position <= versions[begin - 1U].system_commit_position) ||
          (begin != 0U && commit_time < versions[begin - 1U].system_commit_time_ns)) {
        return invalid("retained temporal commits are not in canonical order");
      }

      std::vector<TemporalMutation> mutations;
      mutations.reserve(end - begin);
      for (std::size_t index = begin; index < end; ++index) {
        RetainedTemporalVersion& retained = versions[index];
        if (retained.system_commit_time_ns != commit_time ||
            retained.mutation.wal_id != source_id ||
            retained.mutation.record_sequence != position ||
            (index != begin &&
             retained.mutation.row_ordinal <= versions[index - 1U].mutation.row_ordinal)) {
          return invalid("retained temporal commit rows are not canonical");
        }
        mutations.push_back(std::move(retained.mutation));
      }
      common::Status validation = staged.validate(position, commit_time, mutations, true, true);
      if (!validation.is_ok()) {
        return validation;
      }
      for (TemporalMutation& mutation : mutations) {
        staged.histories[mutation.logical_identity].push_back(
            Impl::Version{std::move(mutation), position, commit_time});
      }
      staged.time_to_position.insert_or_assign(commit_time, position);
      staged.versions += end - begin;
      staged.latest_position = position;
      staged.latest_time = commit_time;
      begin = end;
    }
    if (retained_system_time_ns > staged.latest_time) {
      return invalid("retained temporal boundary is ahead of restored history");
    }
    staged.earliest_retained_time = retained_system_time_ns;
    impl_->histories.swap(staged.histories);
    impl_->time_to_position.swap(staged.time_to_position);
    impl_->latest_position = staged.latest_position;
    impl_->latest_time = staged.latest_time;
    impl_->earliest_retained_time = staged.earliest_retained_time;
    impl_->versions = staged.versions;
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "retained temporal restore allocation failed"};
  } catch (const std::length_error&) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "retained temporal restore exceeded container limits"};
  }
}

common::Status TemporalSnapshotProvider::verify_retained_commit(
    const std::uint64_t system_commit_position, const std::int64_t system_commit_time_ns,
    const std::span<const TemporalMutation> mutations) const {
  if (system_commit_position == 0U || mutations.empty()) {
    return invalid("retained temporal verification input is invalid");
  }
  try {
    std::scoped_lock lock{impl_->mutex};
    if (impl_->failed) {
      return common::Status{common::StatusCode::kUnavailable,
                            "temporal provider failed closed and requires recovery"};
    }
    std::size_t stored_count = 0U;
    for (const auto& [identity, history] : impl_->histories) {
      static_cast<void>(identity);
      stored_count += static_cast<std::size_t>(
          std::ranges::count(history, system_commit_position, &Impl::Version::commit_position));
    }
    const bool may_be_partially_reclaimed = impl_->earliest_retained_time.has_value() &&
                                            system_commit_time_ns < *impl_->earliest_retained_time;
    if ((!may_be_partially_reclaimed && stored_count != mutations.size()) ||
        stored_count > mutations.size()) {
      return corruption("checkpoint-covered temporal commit has different retained row coverage");
    }
    for (const auto& [identity, history] : impl_->histories) {
      const auto version =
          std::ranges::find(history, system_commit_position, &Impl::Version::commit_position);
      if (version == history.end()) {
        continue;
      }
      const auto mutation = std::ranges::find_if(mutations, [&identity](const auto& candidate) {
        return candidate.logical_identity == identity;
      });
      if (mutation == mutations.end() || version->commit_time_ns != system_commit_time_ns ||
          !equal_mutation(version->mutation, *mutation)) {
        return corruption("checkpoint-covered temporal version disagrees with retained history");
      }
    }
    return common::Status::ok();
  } catch (const std::system_error&) {
    return common::Status{common::StatusCode::kInternal,
                          "retained temporal verification lock failed"};
  }
}

common::Status TemporalSnapshotProvider::validate_next_commit(
    const std::int64_t system_commit_time_ns,
    const std::span<const TemporalMutation> mutations) const {
  try {
    std::scoped_lock lock{impl_->mutex};
    if (impl_->latest_position == std::numeric_limits<std::uint64_t>::max()) {
      return common::Status{common::StatusCode::kOutOfRange,
                            "temporal system commit position is exhausted"};
    }
    return impl_->validate(impl_->latest_position + 1U, system_commit_time_ns, mutations, false);
  } catch (const std::bad_alloc&) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "temporal precommit validation allocation failed"};
  } catch (const std::length_error&) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "temporal precommit validation exceeded container limits"};
  }
}

common::Status TemporalSnapshotProvider::fail_closed() {
  try {
    std::scoped_lock lock{impl_->mutex};
    impl_->failed = true;
    return common::Status::ok();
  } catch (const std::system_error& error) {
    return common::Status{common::StatusCode::kInternal,
                          std::string{"temporal fail-closed lock failed: "} + error.what()};
  }
}

bool TemporalSnapshotProvider::is_failed() const noexcept {
  std::scoped_lock lock{impl_->mutex};
  return impl_->failed;
}

const schema::TableSchema& TemporalSnapshotProvider::schema() const noexcept {
  return *impl_->schema;
}

common::Result<std::shared_ptr<const ScalarTableSnapshot>>
TemporalSnapshotProvider::resolve(const std::shared_ptr<const schema::TableSchema>& bound_schema,
                                  const std::optional<std::int64_t> as_of_system_time_ns) const {
  if (bound_schema == nullptr || bound_schema->schema_id() != impl_->schema->schema_id() ||
      bound_schema->version() != impl_->schema->version()) {
    return common::make_unexpected(invalid("temporal snapshot schema is incompatible"));
  }
  std::scoped_lock lock{impl_->mutex};
  if (impl_->failed) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnavailable, "temporal provider failed closed and requires recovery"});
  }
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

common::Result<TemporalHistoryCompactionReport>
TemporalSnapshotProvider::compact_history(const std::uint64_t oldest_observable_commit_position,
                                          const std::int64_t retained_system_time_ns) {
  std::scoped_lock lock{impl_->mutex};
  if (impl_->failed) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnavailable, "temporal provider failed closed and requires recovery"});
  }
  if (oldest_observable_commit_position > impl_->latest_position ||
      retained_system_time_ns > impl_->latest_time) {
    return common::make_unexpected(
        invalid("temporal retention boundary is ahead of committed state"));
  }
  if ((impl_->oldest_observable_position.has_value() &&
       oldest_observable_commit_position < *impl_->oldest_observable_position) ||
      (impl_->earliest_retained_time.has_value() &&
       retained_system_time_ns < *impl_->earliest_retained_time)) {
    return common::make_unexpected(invalid("temporal retention boundaries cannot regress"));
  }
  const std::optional<std::uint64_t> previous_position = impl_->oldest_observable_position;
  const std::optional<std::int64_t> previous_time = impl_->earliest_retained_time;
  std::size_t removed_versions = 0U;
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
    removed_versions += removed;
  }
  impl_->oldest_observable_position = oldest_observable_commit_position;
  impl_->earliest_retained_time = retained_system_time_ns;
  const auto first_retained_time = impl_->time_to_position.lower_bound(retained_system_time_ns);
  if (first_retained_time != impl_->time_to_position.begin()) {
    impl_->time_to_position.erase(impl_->time_to_position.begin(), std::prev(first_retained_time));
  }
  return TemporalHistoryCompactionReport{
      .previous_oldest_observable_commit_position = previous_position,
      .previous_retained_system_time_ns = previous_time,
      .oldest_observable_commit_position = oldest_observable_commit_position,
      .retained_system_time_ns = retained_system_time_ns,
      .removed_version_count = removed_versions,
      .retained_version_count = impl_->versions};
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
