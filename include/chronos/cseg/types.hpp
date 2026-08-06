#ifndef CHRONOS_CSEG_TYPES_HPP_
#define CHRONOS_CSEG_TYPES_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"

#include <compare>

namespace chronos::cseg {

class PartId {
public:
  PartId() = delete;

  [[nodiscard]] static common::Result<PartId> from_uuid(common::Uuid value) {
    if (value.is_nil()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInvalidArgument, "part identity must be nonzero"});
    }
    return PartId{value};
  }

  [[nodiscard]] static common::Result<PartId> from_bytes(common::Uuid::Bytes bytes) {
    return from_uuid(common::Uuid{bytes});
  }

  [[nodiscard]] constexpr const common::Uuid& uuid() const noexcept {
    return value_;
  }
  [[nodiscard]] constexpr const common::Uuid::Bytes& bytes() const noexcept {
    return value_.bytes();
  }

  friend constexpr auto operator<=>(const PartId&, const PartId&) = default;

private:
  explicit constexpr PartId(common::Uuid value) noexcept : value_(value) {}

  common::Uuid value_;
};

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_TYPES_HPP_
