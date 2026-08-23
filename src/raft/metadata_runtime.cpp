#include "chronos/raft/metadata_runtime.hpp"

#include "chronos/raft/membership.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <openssl/evp.h>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return common::Status{common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status unsupported(std::string message) {
  return common::Status{common::StatusCode::kNotSupported, std::move(message)};
}

using MessageDigest = std::unique_ptr<EVP_MD, decltype(&EVP_MD_free)>;
using MessageDigestContext = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;

void store_u64(std::array<std::byte, 17U>& bytes, const std::size_t offset,
               const std::uint64_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

[[nodiscard]] common::Result<std::array<std::byte, 32U>>
metadata_entry_digest(const std::span<const MetadataSnapshotEntry> entries) {
  MessageDigest digest{EVP_MD_fetch(nullptr, "SHA256", nullptr), EVP_MD_free};
  if (!digest)
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "OpenSSL SHA-256 provider is unavailable"});
  MessageDigestContext context{EVP_MD_CTX_new(), EVP_MD_CTX_free};
  if (!context)
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "metadata snapshot digest allocation failed"});
  if (EVP_DigestInit_ex2(context.get(), digest.get(), nullptr) != 1)
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "metadata snapshot digest init failed"});
  constexpr std::array<std::byte, 8U> domain{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                             std::byte{'M'}, std::byte{'A'}, std::byte{'S'},
                                             std::byte{'N'}, std::byte{1U}};
  if (EVP_DigestUpdate(context.get(), domain.data(), domain.size()) != 1)
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "metadata snapshot digest update failed"});
  for (const MetadataSnapshotEntry& entry : entries) {
    std::array<std::byte, 17U> header{};
    store_u64(header, 0U, entry.index);
    store_u64(header, 8U, entry.term);
    header[16U] = static_cast<std::byte>(entry.type);
    std::array<std::byte, 8U> size{};
    const std::uint64_t payload_size = entry.payload.size();
    for (std::size_t index = 0U; index < sizeof(std::uint64_t); ++index)
      size[index] = static_cast<std::byte>(payload_size >> (index * 8U));
    if (EVP_DigestUpdate(context.get(), header.data(), header.size()) != 1 ||
        EVP_DigestUpdate(context.get(), size.data(), size.size()) != 1 ||
        (!entry.payload.empty() &&
         EVP_DigestUpdate(context.get(), entry.payload.data(), entry.payload.size()) != 1)) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "metadata snapshot digest update failed"});
    }
  }
  std::array<unsigned char, 32U> output{};
  unsigned int output_size{};
  if (EVP_DigestFinal_ex(context.get(), output.data(), &output_size) != 1 ||
      output_size != output.size()) {
    return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                  "metadata snapshot digest finalization failed"});
  }
  std::array<std::byte, 32U> result{};
  std::ranges::transform(output, result.begin(),
                         [](const unsigned char value) { return static_cast<std::byte>(value); });
  return result;
}

} // namespace

class DurableMetadataStateMachine::Impl {
public:
  using Application = std::variant<std::monostate, MetadataCommand, CatalogTableDefinition,
                                   TabletGroupBindingMetadata>;

  Impl(GroupId configured_group_id, DurableMultiRaftRuntime& configured_runtime,
       std::optional<MetadataSnapshotStorage> configured_snapshot_storage,
       MetadataStateMachine configured_state,
       const MetadataCommandCodecLimits configured_codec_limits,
       const SchemaDefinitionCodecLimits configured_schema_codec_limits) noexcept
      : group_id(configured_group_id), runtime(&configured_runtime),
        snapshot_storage(std::move(configured_snapshot_storage)),
        metadata(std::move(configured_state)), codec_limits(configured_codec_limits),
        schema_codec_limits(configured_schema_codec_limits) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (failure.is_ok())
      failure = std::move(status);
    return failure;
  }

  [[nodiscard]] common::Result<MetadataApplicationReport>
  apply_entries(const std::span<const LogEntry> entries, const bool persist_applied) {
    MetadataApplicationReport report;
    if (entries.empty())
      return report;
    std::vector<Application> commands;
    commands.reserve(entries.size());
    for (const LogEntry& entry : entries) {
      if (is_internal_raft_entry_type(entry.type)) {
        commands.emplace_back(std::monostate{});
        continue;
      }
      if (entry.type == kRaftSchemaDefinitionEntryType) {
        auto decoded = decode_schema_definition_v1(entry.payload, schema_codec_limits);
        if (!decoded.has_value())
          return common::make_unexpected(fail(decoded.error()));
        commands.emplace_back(std::move(*decoded));
        continue;
      }
      if (entry.type == kRaftTabletGroupBindingEntryType) {
        auto decoded = decode_tablet_group_binding_v1(entry.payload);
        if (!decoded.has_value())
          return common::make_unexpected(fail(decoded.error()));
        if (decoded->group_id == group_id)
          return common::make_unexpected(
              fail(invalid("tablet group binding names the metadata group")));
        commands.emplace_back(*decoded);
        continue;
      }
      if (entry.type != kRaftMetadataCommandEntryType) {
        return common::make_unexpected(
            fail(unsupported("committed metadata Raft entry type is unsupported")));
      }
      auto decoded = decode_metadata_command_v1(entry.payload, codec_limits);
      if (!decoded.has_value())
        return common::make_unexpected(fail(decoded.error()));
      commands.emplace_back(std::move(*decoded));
    }
    report.first_applied_index = entries.front().index;
    for (std::size_t ordinal = 0U; ordinal < entries.size(); ++ordinal) {
      common::Status status = common::Status::ok();
      if (auto* command = std::get_if<MetadataCommand>(&commands[ordinal])) {
        status = metadata.apply_committed(entries[ordinal].index, std::move(*command));
      } else if (auto* definition = std::get_if<CatalogTableDefinition>(&commands[ordinal])) {
        status = metadata.apply_committed_schema_definition(entries[ordinal].index,
                                                            std::move(*definition));
      } else if (auto* binding = std::get_if<TabletGroupBindingMetadata>(&commands[ordinal])) {
        status = metadata.apply_committed_tablet_group_binding(entries[ordinal].index, *binding);
      } else {
        status = metadata.apply_internal_noop(entries[ordinal].index);
      }
      if (!status.is_ok())
        return common::make_unexpected(fail(status));
      if (!std::holds_alternative<std::monostate>(commands[ordinal])) {
        ++report.applied_commands;
      }
      report.last_applied_index = entries[ordinal].index;
    }
    if (!persist_applied)
      return report;
    auto marked =
        runtime->execute_batch({{group_id, MarkAppliedOperation{report.last_applied_index}}});
    if (!marked.has_value())
      return common::make_unexpected(fail(marked.error()));
    if (marked->size() != 1U || !marked->front().status.is_ok()) {
      return common::make_unexpected(fail(
          marked->empty() ? unavailable("metadata applied-index persistence returned no result")
                          : marked->front().status));
    }
    return report;
  }

  [[nodiscard]] common::Status apply_snapshot(MetadataApplicationSnapshot snapshot) {
    auto digest = metadata_entry_digest(snapshot.entries);
    if (!digest.has_value())
      return digest.error();
    if (*digest != snapshot.raft_snapshot.part_set_checksum)
      return corruption("metadata snapshot application-entry digest mismatch");
    try {
      std::vector<Application> commands;
      commands.reserve(snapshot.entries.size());
      for (const MetadataSnapshotEntry& entry : snapshot.entries) {
        if (entry.type == kRaftMetadataCommandEntryType) {
          auto decoded = decode_metadata_command_v1(entry.payload, codec_limits);
          if (!decoded.has_value())
            return decoded.error();
          commands.emplace_back(std::move(*decoded));
        } else if (entry.type == kRaftSchemaDefinitionEntryType) {
          auto decoded = decode_schema_definition_v1(entry.payload, schema_codec_limits);
          if (!decoded.has_value())
            return decoded.error();
          commands.emplace_back(std::move(*decoded));
        } else if (entry.type == kRaftTabletGroupBindingEntryType) {
          auto decoded = decode_tablet_group_binding_v1(entry.payload);
          if (!decoded.has_value())
            return decoded.error();
          if (decoded->group_id == group_id)
            return invalid("tablet group binding names the metadata group");
          commands.emplace_back(*decoded);
        } else {
          return corruption("metadata snapshot contains an unsupported application type");
        }
      }
      for (std::size_t ordinal = 0U; ordinal < snapshot.entries.size(); ++ordinal) {
        const MetadataSnapshotEntry& entry = snapshot.entries[ordinal];
        if (entry.index > metadata.applied_index() && entry.index - metadata.applied_index() > 1U) {
          common::Status advanced = metadata.apply_internal_noops_through(entry.index - 1U);
          if (!advanced.is_ok())
            return advanced;
        }
        common::Status applied = common::Status::ok();
        if (auto* command = std::get_if<MetadataCommand>(&commands[ordinal])) {
          applied = metadata.apply_committed(entry.index, std::move(*command));
        } else if (auto* definition = std::get_if<CatalogTableDefinition>(&commands[ordinal])) {
          applied = metadata.apply_committed_schema_definition(entry.index, std::move(*definition));
        } else {
          applied = metadata.apply_committed_tablet_group_binding(
              entry.index, std::get<TabletGroupBindingMetadata>(commands[ordinal]));
        }
        if (!applied.is_ok())
          return applied;
      }
      return metadata.apply_internal_noops_through(snapshot.raft_snapshot.last_included_index);
    } catch (const std::bad_alloc&) {
      return {common::StatusCode::kResourceExhausted, "metadata snapshot replay allocation failed"};
    }
  }

  [[nodiscard]] common::Result<MetadataSnapshotCompactionReport>
  compact_applied_prefix(const LogIndex last_included_index) {
    if (!failure.is_ok())
      return common::make_unexpected(failure);
    if (!snapshot_storage.has_value())
      return common::make_unexpected(
          unsupported("metadata compaction requires application-snapshot storage ownership"));
    MetadataSnapshotStorage& storage = snapshot_storage.value();
    const RaftNode* node = runtime->find_group(group_id);
    if (node == nullptr)
      return common::make_unexpected(fail(unavailable("metadata Raft group disappeared")));
    const PersistentState& persistent = node->persistent_state();
    const SnapshotMetadata& current_snapshot = persistent.snapshot;
    if (current_snapshot.last_included_index == 0U) {
      if (installed_snapshot.has_value()) {
        return common::make_unexpected(
            fail(corruption("metadata application snapshot boundary changed outside its owner")));
      }
    } else {
      if (!installed_snapshot.has_value() || installed_snapshot.value() != current_snapshot) {
        return common::make_unexpected(
            fail(corruption("metadata application snapshot boundary changed outside its owner")));
      }
    }
    if (last_included_index <= current_snapshot.last_included_index ||
        last_included_index > persistent.applied_index || node->joint_membership_active()) {
      return common::make_unexpected(
          invalid("metadata snapshot must cover a newer applied stable-configuration prefix"));
    }
    const LogIndex relative_index = last_included_index - current_snapshot.last_included_index;
    if (relative_index == 0U || relative_index > persistent.log.size()) {
      return common::make_unexpected(
          fail(corruption("metadata snapshot boundary is absent from the retained log")));
    }
    const LogEntry& boundary = persistent.log[static_cast<std::size_t>(relative_index - 1U)];
    if (boundary.index != last_included_index || boundary.term == 0U)
      return common::make_unexpected(
          fail(corruption("metadata snapshot boundary term cannot be derived")));
    if (metadata.applied_index() < last_included_index)
      return common::make_unexpected(
          fail(corruption("metadata application trails the requested snapshot boundary")));

    MetadataApplicationSnapshot application_snapshot{
        .group_id = group_id, .raft_snapshot = SnapshotMetadata{}, .entries = {}};
    SnapshotMetadata next_snapshot{.last_included_index = last_included_index,
                                   .last_included_term = boundary.term,
                                   .manifest_generation = last_included_index,
                                   .part_set_checksum = {},
                                   .configuration_index = current_snapshot.configuration_index,
                                   .voters = {}};
    auto prepared_snapshot = node->prepare_snapshot_metadata(next_snapshot);
    if (!prepared_snapshot.has_value())
      return common::make_unexpected(prepared_snapshot.error());
    next_snapshot = std::move(*prepared_snapshot);
    try {
      if (current_snapshot.last_included_index != 0U) {
        auto loaded = storage.load(current_snapshot.last_included_index);
        if (!loaded.has_value())
          return common::make_unexpected(fail(loaded.error()));
        if (loaded->snapshot.group_id != group_id ||
            loaded->snapshot.raft_snapshot != current_snapshot) {
          return common::make_unexpected(
              fail(corruption("installed metadata snapshot changed during compaction")));
        }
        auto prior_digest = metadata_entry_digest(loaded->snapshot.entries);
        if (!prior_digest.has_value())
          return common::make_unexpected(prior_digest.error());
        if (*prior_digest != current_snapshot.part_set_checksum) {
          return common::make_unexpected(
              fail(corruption("installed metadata snapshot entry digest changed")));
        }
        application_snapshot.entries = std::move(loaded->snapshot.entries);
      }
      const std::size_t appended_capacity = static_cast<std::size_t>(relative_index);
      if (appended_capacity >
          application_snapshot.entries.max_size() - application_snapshot.entries.size()) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kResourceExhausted,
                           "metadata snapshot entry capacity exceeds the platform limit"});
      }
      application_snapshot.entries.reserve(application_snapshot.entries.size() + appended_capacity);
      for (const LogEntry& entry : persistent.log) {
        if (entry.index > last_included_index)
          break;
        if (is_internal_raft_entry_type(entry.type))
          continue;
        if (entry.type != kRaftMetadataCommandEntryType &&
            entry.type != kRaftSchemaDefinitionEntryType &&
            entry.type != kRaftTabletGroupBindingEntryType) {
          return common::make_unexpected(
              fail(corruption("applied metadata prefix contains an unknown entry type")));
        }
        application_snapshot.entries.push_back({.index = entry.index,
                                                .term = entry.term,
                                                .type = entry.type,
                                                .payload = entry.payload});
      }
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                    "metadata snapshot allocation failed"});
    }
    auto digest = metadata_entry_digest(application_snapshot.entries);
    if (!digest.has_value())
      return common::make_unexpected(digest.error());
    next_snapshot.part_set_checksum = *digest;
    application_snapshot.raft_snapshot = next_snapshot;

    auto installed = storage.install(application_snapshot);
    if (!installed.has_value())
      return common::make_unexpected(installed.error());
    auto compacted = runtime->execute_batch(
        {{group_id, CompactSnapshotOperation{application_snapshot.raft_snapshot}}});
    if (!compacted.has_value())
      return common::make_unexpected(fail(compacted.error()));
    if (compacted->size() != 1U || !compacted->front().status.is_ok()) {
      return common::make_unexpected(
          compacted->empty() ? unavailable("metadata snapshot compaction returned no result")
                             : compacted->front().status);
    }
    node = runtime->find_group(group_id);
    if (node == nullptr ||
        node->persistent_state().snapshot != application_snapshot.raft_snapshot) {
      return common::make_unexpected(
          fail(corruption("durable Raft metadata disagrees with metadata snapshot bytes")));
    }
    installed_snapshot = application_snapshot.raft_snapshot;
    return MetadataSnapshotCompactionReport{
        .snapshot = application_snapshot.raft_snapshot,
        .file_name = std::move(installed->file_name),
        .application_entries = application_snapshot.entries.size(),
        .application_snapshot_already_present = installed->already_present};
  }

  GroupId group_id;
  DurableMultiRaftRuntime* runtime;
  std::optional<MetadataSnapshotStorage> snapshot_storage;
  std::optional<SnapshotMetadata> installed_snapshot;
  MetadataStateMachine metadata;
  MetadataCommandCodecLimits codec_limits;
  SchemaDefinitionCodecLimits schema_codec_limits;
  common::Status failure;
};

DurableMetadataStateMachine::DurableMetadataStateMachine(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
DurableMetadataStateMachine::~DurableMetadataStateMachine() = default;
DurableMetadataStateMachine::DurableMetadataStateMachine(DurableMetadataStateMachine&&) noexcept =
    default;
DurableMetadataStateMachine&
DurableMetadataStateMachine::operator=(DurableMetadataStateMachine&&) noexcept = default;

common::Result<DurableMetadataStateMachine>
DurableMetadataStateMachine::recover(GroupId group_id, DurableMultiRaftRuntime& runtime,
                                     const MetadataLimits state_limits,
                                     const MetadataCommandCodecLimits codec_limits,
                                     const SchemaDefinitionCodecLimits schema_codec_limits) {
  return recover_impl(group_id, runtime, std::nullopt, state_limits, codec_limits,
                      schema_codec_limits);
}

common::Result<DurableMetadataStateMachine> DurableMetadataStateMachine::recover(
    GroupId group_id, DurableMultiRaftRuntime& runtime, MetadataSnapshotStorage snapshot_storage,
    const MetadataLimits state_limits, const MetadataCommandCodecLimits codec_limits,
    const SchemaDefinitionCodecLimits schema_codec_limits) {
  return recover_impl(group_id, runtime,
                      std::optional<MetadataSnapshotStorage>{std::move(snapshot_storage)},
                      state_limits, codec_limits, schema_codec_limits);
}

common::Result<DurableMetadataStateMachine>
DurableMetadataStateMachine::recover_impl(GroupId group_id, DurableMultiRaftRuntime& runtime,
                                          std::optional<MetadataSnapshotStorage> snapshot_storage,
                                          const MetadataLimits state_limits,
                                          const MetadataCommandCodecLimits codec_limits,
                                          const SchemaDefinitionCodecLimits schema_codec_limits) {
  if (group_id.is_nil())
    return common::make_unexpected(invalid("metadata Raft group identity is nil"));
  const RaftNode* const node = runtime.find_group(group_id);
  if (node == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "metadata Raft group does not exist"});
  }
  auto state = MetadataStateMachine::create(state_limits);
  if (!state.has_value())
    return common::make_unexpected(state.error());
  const PersistentState& persistent = node->persistent_state();
  const LogIndex snapshot_index = persistent.snapshot.last_included_index;
  if (persistent.applied_index < snapshot_index || persistent.commit_index < snapshot_index ||
      persistent.commit_index - snapshot_index > persistent.log.size()) {
    return common::make_unexpected(
        corruption("metadata applied or committed prefix disagrees with retained history"));
  }
  auto impl = std::make_unique<Impl>(group_id, runtime, std::move(snapshot_storage),
                                     std::move(*state), codec_limits, schema_codec_limits);
  if (snapshot_index != 0U) {
    std::optional<MetadataSnapshotStorage>& storage_owner = impl->snapshot_storage;
    if (!storage_owner.has_value()) {
      return common::make_unexpected(
          unsupported("metadata recovery requires its installed application snapshot"));
    }
    MetadataSnapshotStorage& storage = storage_owner.value();
    auto loaded = storage.load(snapshot_index);
    if (!loaded.has_value())
      return common::make_unexpected(loaded.error());
    if (loaded->snapshot.group_id != group_id ||
        loaded->snapshot.raft_snapshot != persistent.snapshot) {
      return common::make_unexpected(
          corruption("installed metadata snapshot disagrees with durable Raft state"));
    }
    common::Status restored = impl->apply_snapshot(std::move(loaded->snapshot));
    if (!restored.is_ok())
      return common::make_unexpected(restored);
    impl->installed_snapshot = persistent.snapshot;
  }
  const std::size_t suffix_count =
      static_cast<std::size_t>(persistent.commit_index - snapshot_index);
  const std::span<const LogEntry> committed_suffix{persistent.log.data(), suffix_count};
  auto recovered =
      impl->apply_entries(committed_suffix, persistent.applied_index < persistent.commit_index);
  if (!recovered.has_value())
    return common::make_unexpected(recovered.error());
  return DurableMetadataStateMachine{std::move(impl)};
}

common::Result<MetadataApplicationReport> DurableMetadataStateMachine::apply_committed() {
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  const RaftNode* const node = impl_->runtime->find_group(impl_->group_id);
  if (node == nullptr)
    return common::make_unexpected(impl_->fail(unavailable("metadata Raft group disappeared")));
  if (node->persistent_state().snapshot.last_included_index == 0U) {
    if (impl_->installed_snapshot.has_value()) {
      return common::make_unexpected(
          impl_->fail(corruption("metadata snapshot boundary moved backward")));
    }
  } else {
    const std::optional<SnapshotMetadata>& installed = impl_->installed_snapshot;
    if (!installed.has_value() || installed.value() != node->persistent_state().snapshot) {
      return common::make_unexpected(
          impl_->fail(unsupported("metadata application cannot cross a different snapshot")));
    }
  }
  return impl_->apply_entries(node->committed_unapplied(), true);
}

common::Result<MetadataSnapshotCompactionReport>
DurableMetadataStateMachine::compact_applied_prefix(const LogIndex last_included_index) {
  return impl_->compact_applied_prefix(last_included_index);
}

common::Result<MetadataSnapshotReclamationReport>
DurableMetadataStateMachine::reclaim_obsolete_snapshots() {
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  std::optional<MetadataSnapshotStorage>& storage_owner = impl_->snapshot_storage;
  if (!storage_owner.has_value()) {
    return common::make_unexpected(
        unsupported("metadata snapshot reclamation requires snapshot-storage ownership"));
  }
  MetadataSnapshotStorage& storage = storage_owner.value();
  const RaftNode* const node = impl_->runtime->find_group(impl_->group_id);
  if (node == nullptr)
    return common::make_unexpected(impl_->fail(unavailable("metadata Raft group disappeared")));
  const SnapshotMetadata& snapshot = node->persistent_state().snapshot;
  if (snapshot.last_included_index == 0U) {
    if (impl_->installed_snapshot.has_value()) {
      return common::make_unexpected(
          impl_->fail(corruption("metadata snapshot boundary moved backward")));
    }
    return storage.reclaim_obsolete(std::nullopt);
  }
  const std::optional<SnapshotMetadata>& installed = impl_->installed_snapshot;
  if (!installed.has_value() || installed.value() != snapshot) {
    return common::make_unexpected(
        impl_->fail(unsupported("metadata reclamation cannot cross a different snapshot")));
  }
  return storage.reclaim_obsolete(snapshot.last_included_index);
}

common::Result<QuorumSyncReceipt>
DurableMetadataStateMachine::prove_applied_quorum_sync(const LogIndex index) const {
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  if (index == 0U || impl_->metadata.applied_index() < index) {
    return common::make_unexpected(unavailable("QUORUM_SYNC metadata entry has not been applied"));
  }
  return impl_->runtime->prove_quorum_sync(impl_->group_id, index);
}

const MetadataStateMachine& DurableMetadataStateMachine::state() const noexcept {
  return impl_->metadata;
}
const GroupId& DurableMetadataStateMachine::group_id() const noexcept {
  return impl_->group_id;
}
std::optional<MetadataSnapshotCleanupMetrics>
DurableMetadataStateMachine::snapshot_cleanup_metrics() const noexcept {
  if (!impl_)
    return std::nullopt;
  return impl_->snapshot_storage.transform(
      [](const MetadataSnapshotStorage& storage) { return storage.cleanup_metrics(); });
}
bool DurableMetadataStateMachine::failed() const noexcept {
  return !impl_->failure.is_ok();
}
common::Status DurableMetadataStateMachine::failure_status() const {
  return impl_->failure;
}

} // namespace chronos::raft
