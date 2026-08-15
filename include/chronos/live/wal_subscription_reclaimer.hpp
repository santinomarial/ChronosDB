#ifndef CHRONOS_LIVE_WAL_SUBSCRIPTION_RECLAIMER_HPP_
#define CHRONOS_LIVE_WAL_SUBSCRIPTION_RECLAIMER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/live/subscription_retention.hpp"
#include "chronos/wal/wal_writer.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::live {

struct WalSubscriptionReclamationSource {
  schema::TabletId tablet_id;
  std::uint64_t placement_epoch{};
  wal::WalWriter* writer{};
};

struct WalSubscriptionSourceReclaimerConfig {
  std::vector<WalSubscriptionReclamationSource> sources;
  std::size_t maximum_sources{1024U};
};

// Thread-affine physical reclaimer for WAL-backed subscription sources. WalWriter pointers are
// borrowed, must outlive this owner, and remain exclusively serialized by the same caller. The
// complete request batch is identity-checked and mapped to verified physical checkpoints before
// the first writer is allowed to unlink a segment. Tablets sharing one WAL conservatively authorize
// only their minimum logical record sequence.
class WalSubscriptionSourceReclaimer final : public SubscriptionSourceReclaimer {
public:
  WalSubscriptionSourceReclaimer() = delete;
  ~WalSubscriptionSourceReclaimer() override;
  WalSubscriptionSourceReclaimer(const WalSubscriptionSourceReclaimer&) = delete;
  WalSubscriptionSourceReclaimer& operator=(const WalSubscriptionSourceReclaimer&) = delete;
  WalSubscriptionSourceReclaimer(WalSubscriptionSourceReclaimer&&) noexcept;
  WalSubscriptionSourceReclaimer& operator=(WalSubscriptionSourceReclaimer&&) noexcept;

  [[nodiscard]] static common::Result<WalSubscriptionSourceReclaimer>
  create(WalSubscriptionSourceReclaimerConfig config);

  [[nodiscard]] common::Status
  reclaim(std::span<const SubscriptionSourceReclamation> requests) override;

private:
  class Impl;
  explicit WalSubscriptionSourceReclaimer(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_WAL_SUBSCRIPTION_RECLAIMER_HPP_
