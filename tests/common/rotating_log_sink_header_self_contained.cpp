#include "chronos/common/rotating_log_sink.hpp"

namespace {

[[maybe_unused]] void rotating_log_sink_header_self_contained() {
  chronos::common::RotatingJsonLogSinkConfig config;
  config.path = "chronos.log";
}

} // namespace
