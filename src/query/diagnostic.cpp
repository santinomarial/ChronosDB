#include "chronos/query/diagnostic.hpp"

#include <utility>

namespace chronos::query {

SqlDiagnostic::SqlDiagnostic(const SqlDiagnosticCode code, const SourceSpan span,
                             common::Status status) noexcept
    : code_(code), span_(span), status_(std::move(status)) {}

SqlDiagnosticCode SqlDiagnostic::code() const noexcept {
  return code_;
}

const SourceSpan& SqlDiagnostic::span() const noexcept {
  return span_;
}

const common::Status& SqlDiagnostic::status() const noexcept {
  return status_;
}

} // namespace chronos::query
