#ifndef CHRONOS_RAFT_TABLET_RECONFIGURATION_ACTION_LEDGER_HPP_
#define CHRONOS_RAFT_TABLET_RECONFIGURATION_ACTION_LEDGER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/tablet_reconfiguration_action_codec.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace chronos::raft {

struct TabletReconfigurationActionLedgerConfig {
  std::string directory_path;
  schema::TabletId tablet_id;
  TabletReconfigurationActionCodecLimits codec_limits{};
  std::uint16_t file_permissions{0600U};
};

struct PreparedTabletReconfigurationAction {
  TabletReconfigurationActionId id;
  std::string file_name;
  bool already_present{};
};

struct LoadedTabletReconfigurationAction {
  std::string file_name;
  TabletReconfigurationAction action;
  std::vector<std::byte> bytes;
};

[[nodiscard]] common::Result<std::string>
tablet_reconfiguration_action_file_name(const TabletReconfigurationActionId& id);

// Locked immutable pre-dispatch ledger for one tablet. prepare() exact-validates, file-syncs,
// no-replace installs, and directory-syncs an action before it may be dispatched. Same-ID retries
// must have identical bytes; completed files remain diagnostic evidence and are never overwritten.
class TabletReconfigurationActionLedger {
public:
  TabletReconfigurationActionLedger() = delete;
  ~TabletReconfigurationActionLedger();
  TabletReconfigurationActionLedger(const TabletReconfigurationActionLedger&) = delete;
  TabletReconfigurationActionLedger& operator=(const TabletReconfigurationActionLedger&) = delete;
  TabletReconfigurationActionLedger(TabletReconfigurationActionLedger&&) noexcept;
  TabletReconfigurationActionLedger& operator=(TabletReconfigurationActionLedger&&) noexcept;

  [[nodiscard]] static common::Result<TabletReconfigurationActionLedger>
  create(TabletReconfigurationActionLedgerConfig config);
  [[nodiscard]] static common::Result<TabletReconfigurationActionLedger>
  open_existing(TabletReconfigurationActionLedgerConfig config);

  [[nodiscard]] common::Result<PreparedTabletReconfigurationAction>
  prepare(const TabletReconfigurationAction& action);
  [[nodiscard]] common::Result<LoadedTabletReconfigurationAction>
  load(const TabletReconfigurationActionId& id) const;

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  class Impl;
  [[nodiscard]] static common::Result<TabletReconfigurationActionLedger>
  open(TabletReconfigurationActionLedgerConfig config, bool create_lock);
  explicit TabletReconfigurationActionLedger(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TABLET_RECONFIGURATION_ACTION_LEDGER_HPP_
