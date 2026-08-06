#ifndef CHRONOS_QUERY_RESOURCE_CONTEXT_HPP_
#define CHRONOS_QUERY_RESOURCE_CONTEXT_HPP_

#include "chronos/common/result.hpp"

#include <cstddef>
#include <memory>

namespace chronos::query {

namespace detail {
class QueryResourceState;
} // namespace detail

// Move-only credit against one query-wide memory limit. Destruction or release() returns the exact
// credit. Moving transfers that obligation; it does not reserve again.
class QueryMemoryReservation {
public:
  QueryMemoryReservation() noexcept = default;
  QueryMemoryReservation(const QueryMemoryReservation&) = delete;
  QueryMemoryReservation& operator=(const QueryMemoryReservation&) = delete;
  QueryMemoryReservation(QueryMemoryReservation&& other) noexcept;
  QueryMemoryReservation& operator=(QueryMemoryReservation&& other) noexcept;
  ~QueryMemoryReservation();

  [[nodiscard]] bool is_valid() const noexcept;
  [[nodiscard]] std::size_t bytes() const noexcept;
  void release() noexcept;

private:
  QueryMemoryReservation(std::shared_ptr<detail::QueryResourceState> state,
                         std::size_t bytes) noexcept;

  std::shared_ptr<detail::QueryResourceState> state_;
  std::size_t bytes_{};

  friend class QueryResourceContext;
};

// A copyable handle to one query's shared memory and cancellation state. Copies are intended for
// worker-task handoff. request_cancel() is idempotent; existing owners release reservations and
// snapshot pins by observing cancellation at explicit poll points and unwinding normally.
class QueryResourceContext {
public:
  QueryResourceContext() = delete;
  QueryResourceContext(const QueryResourceContext&) noexcept = default;
  QueryResourceContext& operator=(const QueryResourceContext&) noexcept = default;
  QueryResourceContext(QueryResourceContext&&) noexcept = default;
  QueryResourceContext& operator=(QueryResourceContext&&) noexcept = default;

  [[nodiscard]] static common::Result<QueryResourceContext>
  create(std::size_t maximum_memory_bytes);

  [[nodiscard]] common::Result<QueryMemoryReservation> reserve(std::size_t bytes) const;
  [[nodiscard]] bool owns(const QueryMemoryReservation& reservation) const noexcept;

  // Returns true only for the call that first changes the shared state to cancelled.
  [[nodiscard]] bool request_cancel() const noexcept;
  [[nodiscard]] bool is_cancelled() const noexcept;
  [[nodiscard]] common::Result<void> check_cancelled() const;

  [[nodiscard]] std::size_t maximum_memory_bytes() const noexcept;
  [[nodiscard]] std::size_t reserved_memory_bytes() const noexcept;
  [[nodiscard]] std::size_t available_memory_bytes() const noexcept;
  [[nodiscard]] std::size_t peak_reserved_memory_bytes() const noexcept;

private:
  explicit QueryResourceContext(std::shared_ptr<detail::QueryResourceState> state) noexcept;

  std::shared_ptr<detail::QueryResourceState> state_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_RESOURCE_CONTEXT_HPP_
