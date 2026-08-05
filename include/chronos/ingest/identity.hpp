#ifndef CHRONOS_INGEST_IDENTITY_HPP_
#define CHRONOS_INGEST_IDENTITY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"

#include <compare>

namespace chronos::ingest {

template <typename Tag> class RequestIdentifier {
public:
  RequestIdentifier() = delete;

  [[nodiscard]] static common::Result<RequestIdentifier> from_uuid(common::Uuid value) {
    if (value.is_nil()) {
      return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                     "request identifier must be nonzero"});
    }
    return RequestIdentifier{value};
  }

  [[nodiscard]] static common::Result<RequestIdentifier>
  from_bytes(common::Uuid::Bytes bytes) {
    return from_uuid(common::Uuid{bytes});
  }

  [[nodiscard]] constexpr const common::Uuid& uuid() const noexcept {
    return value_;
  }
  [[nodiscard]] constexpr const common::Uuid::Bytes& bytes() const noexcept {
    return value_.bytes();
  }

  friend constexpr auto operator<=>(const RequestIdentifier&, const RequestIdentifier&) = default;

private:
  explicit constexpr RequestIdentifier(common::Uuid value) noexcept : value_(value) {}

  common::Uuid value_;
};

struct ClientIdTag;
struct ClientBatchIdTag;

using ClientId = RequestIdentifier<ClientIdTag>;
using ClientBatchId = RequestIdentifier<ClientBatchIdTag>;

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_IDENTITY_HPP_
