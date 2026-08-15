#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace {

void benchmark_harness_iteration(benchmark::State& state) {
  for (auto iteration : state)
    benchmark::DoNotOptimize(iteration);
  state.SetLabel("local measurement only; harness iteration and optimization barrier");
}

void benchmark_crc32c(benchmark::State& state) {
  const auto size = static_cast<std::size_t>(state.range(0));
  std::vector<std::byte> buffer(size);
  for (std::size_t index = 0; index < buffer.size(); ++index) {
    buffer[index] = static_cast<std::byte>((index * 131U + 17U) & 0xffU);
  }

  for ([[maybe_unused]] auto iteration : state) {
    std::uint32_t checksum = chronos::common::crc32c(buffer);
    benchmark::DoNotOptimize(checksum);
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
  state.SetLabel("local measurement only; portable scalar CRC32C");
}

void benchmark_byte_reader_u64(benchmark::State& state) {
  const auto size = static_cast<std::size_t>(state.range(0));
  std::vector<std::byte> buffer(size);
  chronos::common::ByteWriter setup_writer{buffer};
  while (setup_writer.remaining() >= sizeof(std::uint64_t)) {
    const chronos::common::Status status = setup_writer.write_u64_le(0x0123456789abcdefULL);
    if (!status.is_ok()) {
      const std::string message = status.to_string();
      state.SkipWithError(message);
      return;
    }
  }

  for ([[maybe_unused]] auto iteration : state) {
    chronos::common::ByteReader reader{buffer};
    std::uint64_t aggregate = 0;
    while (!reader.empty()) {
      const auto value = reader.read_u64_le();
      if (!value.has_value()) {
        const std::string message = value.error().to_string();
        state.SkipWithError(message);
        return;
      }
      aggregate ^= *value;
    }
    benchmark::DoNotOptimize(aggregate);
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
  state.SetLabel("local measurement only; sequential little-endian u64 decode");
}

void benchmark_byte_writer_u64(benchmark::State& state) {
  const auto size = static_cast<std::size_t>(state.range(0));
  std::vector<std::byte> buffer(size);

  for ([[maybe_unused]] auto iteration : state) {
    chronos::common::ByteWriter writer{buffer};
    while (writer.remaining() >= sizeof(std::uint64_t)) {
      const chronos::common::Status status = writer.write_u64_le(0x0123456789abcdefULL);
      if (!status.is_ok()) {
        const std::string message = status.to_string();
        state.SkipWithError(message);
        return;
      }
    }
    benchmark::DoNotOptimize(buffer.data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
  state.SetLabel("local measurement only; sequential little-endian u64 encode");
}

// Google Benchmark intentionally registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_harness_iteration);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_crc32c)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_byte_reader_u64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_byte_writer_u64)->Arg(1024)->Arg(65536);

} // namespace
