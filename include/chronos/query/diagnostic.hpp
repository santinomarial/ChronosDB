#ifndef CHRONOS_QUERY_DIAGNOSTIC_HPP_
#define CHRONOS_QUERY_DIAGNOSTIC_HPP_

#include "chronos/common/status.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>
#include <string>

namespace chronos::query {

enum class SqlDiagnosticCode : std::uint16_t {
  kInvalidByte = 1U,
  kUnterminatedQuotedIdentifier,
  kUnterminatedString,
  kUnterminatedComment,
  kInvalidBinaryLiteral,
  kInvalidNumber,
  kResourceLimit,
  kUnexpectedToken,
  kUnsupportedSyntax,
  kUnknownTable,
  kUnknownColumn,
  kAmbiguousColumn,
  kTypeMismatch,
  kDuplicateOutputName,
  kExecutionFailure,
};

struct SourceLocation {
  std::size_t byte_offset{};
  std::uint32_t line{1U};
  std::uint32_t column{1U};

  friend bool operator==(const SourceLocation&, const SourceLocation&) = default;
};

struct SourceSpan {
  SourceLocation begin;
  std::size_t byte_length{};

  friend bool operator==(const SourceSpan&, const SourceSpan&) = default;
};

class SqlDiagnostic {
public:
  SqlDiagnostic(SqlDiagnosticCode code, SourceSpan span, common::Status status) noexcept;

  [[nodiscard]] SqlDiagnosticCode code() const noexcept;
  [[nodiscard]] const SourceSpan& span() const noexcept;
  [[nodiscard]] const common::Status& status() const noexcept;

private:
  SqlDiagnosticCode code_;
  SourceSpan span_;
  common::Status status_;
};

template <typename Value> using SqlResult = std::expected<Value, SqlDiagnostic>;

} // namespace chronos::query

#endif // CHRONOS_QUERY_DIAGNOSTIC_HPP_
