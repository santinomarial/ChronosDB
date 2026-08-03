#ifndef CHRONOS_SCHEMA_IDENTITY_HPP_
#define CHRONOS_SCHEMA_IDENTITY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"

#include <compare>
#include <cstdint>

namespace chronos::schema {

template <typename Tag> class Identifier {
public:
  Identifier() = delete;

  [[nodiscard]] static common::Result<Identifier> from_uuid(common::Uuid value) {
    if (value.is_nil()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInvalidArgument, "identifier must be nonzero"});
    }
    return Identifier{value};
  }

  [[nodiscard]] static common::Result<Identifier> from_bytes(common::Uuid::Bytes bytes) {
    return from_uuid(common::Uuid{bytes});
  }

  [[nodiscard]] constexpr const common::Uuid& uuid() const noexcept {
    return value_;
  }
  [[nodiscard]] constexpr const common::Uuid::Bytes& bytes() const noexcept {
    return value_.bytes();
  }

  friend constexpr auto operator<=>(const Identifier&, const Identifier&) = default;

private:
  explicit constexpr Identifier(common::Uuid value) noexcept : value_(value) {}

  common::Uuid value_;
};

struct TableIdTag;
struct ColumnIdTag;
struct SchemaIdTag;
struct TabletIdTag;

using TableId = Identifier<TableIdTag>;
using ColumnId = Identifier<ColumnIdTag>;
using SchemaId = Identifier<SchemaIdTag>;
using TabletId = Identifier<TabletIdTag>;

class SchemaVersion {
public:
  SchemaVersion() = delete;

  [[nodiscard]] static common::Result<SchemaVersion> from_value(std::uint64_t value);
  [[nodiscard]] static constexpr SchemaVersion initial() noexcept {
    return SchemaVersion{1};
  }

  [[nodiscard]] common::Result<SchemaVersion> next() const;
  [[nodiscard]] constexpr std::uint64_t value() const noexcept {
    return value_;
  }

  friend constexpr auto operator<=>(const SchemaVersion&, const SchemaVersion&) = default;

private:
  explicit constexpr SchemaVersion(std::uint64_t value) noexcept : value_(value) {}

  std::uint64_t value_;
};

} // namespace chronos::schema

#endif // CHRONOS_SCHEMA_IDENTITY_HPP_
