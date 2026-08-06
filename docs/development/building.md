# Building ChronosDB

ChronosDB's implemented code currently consists of the Phase 1A build/version foundation, the Phase
1B portable common binary primitives, the logical schema/model foundation, the pure in-memory WAL
v1 physical codec, the minimal blocking
POSIX file/directory layer, the segmented WAL writer, locked physical recovery/reopen path, and the
bounded commit coordinator, subprocess crash harness, read-only `chronos-waldump` inspector, and
correctness-gated `chronos-walbench` measurement tool. Application-kind codecs and other engine
components described elsewhere remain planned. The reference production platform is
Linux x86-64; the common, WAL codec, POSIX I/O, writer, recovery, and inspection targets support
Linux and modern macOS, including Apple silicon. macOS correctness support is not a power-loss
durability claim.

## Prerequisites

- CMake 3.25 or newer
- Ninja 1.10 or newer
- a C++23 compiler and standard library that provide `std::expected`: GCC 13+, Clang 17+ paired
  with a capable libc++/libstdc++, or a current AppleClang
- Git, so CMake can fetch pinned test dependencies and optionally record revision metadata
- OpenSSL 3 and Zstandard 1.5.5 or newer production development packages
- Python only as required by CMake/GoogleTest test discovery
- clang-format 18 exactly, plus clang-tidy from a reasonably current LLVM release (17+ supported)

The first test or benchmark configuration needs network access to fetch its pinned dependency.
Subsequent configurations reuse CMake's build-tree dependency checkout. No dependency source is
vendored into this repository.

## Linux setup

On Ubuntu 24.04, install the distribution packages for `cmake`, `ninja-build`, `g++`, `clang`,
`libc++-dev`, `libc++abi-dev`, `libssl-dev`, `libzstd-dev`, `clang-format-18`, and `clang-tidy`. GCC and Clang are both
CI-supported. Ubuntu's Clang 18 defaults to libstdc++ 13, but that compiler/library pairing does not
expose the required C++23 `std::expected`; the supported Clang pairing uses libc++. Select the
compiler and standard library before the first configure:

```sh
CC=gcc CXX=g++ cmake --preset debug
CC=clang CXX=clang++ CXXFLAGS=-stdlib=libc++ cmake --preset debug
```

Configuration checks `std::expected` support and fails early with a direct diagnostic when it is
missing. Do not switch compilers, standard libraries, or standard-library flags inside an existing
build directory; use a fresh preset build directory.

## macOS setup

Install Xcode Command Line Tools and CMake/Ninja. Homebrew users can run:

```sh
xcode-select --install
brew install cmake ninja llvm llvm@18 openssl@3 zstd
```

Homebrew's versioned LLVM tools may not be on `PATH`. `scripts/format.sh` searches the standard
`llvm@18` prefix; otherwise set `CLANG_FORMAT=$(brew --prefix llvm@18)/bin/clang-format`. Set
`CLANG_TIDY` separately to the chosen current LLVM tidy executable. AppleClang builds the portable
common and WAL targets plus the macOS POSIX I/O backend. The backend uses `fsync` where Linux uses
`fdatasync`; this does not advertise a macOS power-loss envelope. Future server, direct-I/O, and
reactor components may require Linux and will be guarded by explicit platform checks rather than
weakened portable interfaces.

## Configure, build, and test

The presets are Ninja-based and create independent directories under `build/`:

```sh
cmake --preset dev
cmake --build --preset dev
ctest --preset dev
```

Equivalent wrappers can be run from any directory:

```sh
scripts/configure.sh dev
scripts/build.sh dev
scripts/test.sh dev
```

`debug` is the CI-oriented debug configuration. `release` enables the compiler's normal optimized
release mode; it does not add host-specific architecture flags.

```sh
cmake --preset release
cmake --build --preset release
ctest --preset release
```

The resulting proof executable reports build metadata. Git commit and dirty state are refreshed at
the start of every build, including a rebuild after source changes that do not require CMake to
reconfigure. The generated header is rewritten only when its content changes:

```sh
build/dev/chronosctl version
build/dev/chronosctl version --json
```

The read-only WAL inspector acquires the existing writer lock, verifies the complete physical log,
preflights every record, and then prints record metadata in deterministic order without dumping
payload contents:

```sh
build/dev/chronos-waldump <path-to-wal-directory>
```

It exits `0` for a clean WAL, `3` for an incomplete but potentially repairable final tail, `1` for
verification or lock failure, and `2` for invalid command-line use. It never repairs or creates a
missing `LOCK` file.

Install to a staging prefix with `cmake --install build/release --prefix <directory>`. This installs
`chronosctl`, `chronos-waldump`, `chronos-walbench`, the common, schema, POSIX I/O, and WAL libraries
and public headers, and a CMake package exporting `chronos::common`, `chronos::schema`,
`chronos::cseg`, `chronos::io`, and `chronos::wal` among the implemented library targets.

## Sanitizers

ASan and UBSan intentionally share one Debug preset; TSan is isolated because the runtimes are
incompatible and diagnose different failure classes:

```sh
cmake --preset asan-ubsan
cmake --build --preset asan-ubsan
ctest --preset asan-ubsan

cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan
```

The flags attach only to ChronosDB targets. A requested unsupported compiler fails configuration.
UBSan findings are configured as non-recovering so a test process cannot emit undefined-behavior
diagnostics and still exit successfully. Sanitizer support also depends on the compiler runtime and
host OS; Linux Clang is the CI reference.

## Microbenchmarks

The production-path WAL measurement harness has a dependency-free Release preset and a wrapper that
captures metadata and refuses unsafe output/source states:

```sh
scripts/benchmark-wal.sh /new/path/outside/the/repository/wal-run --mode LOCAL_SYNC
```

See [WAL benchmarks](../benchmarks/wal-benchmarks.md) before interpreting or publishing output.

The optional benchmark preset builds Release-mode CRC32C, ByteReader, ByteWriter, and harness
microbenchmarks:

```sh
cmake --preset benchmark
cmake --build --preset benchmark
build/benchmark/chronos_common_benchmarks
build/benchmark/chronos_cseg_benchmarks
```

The executable labels results as local measurements only. Record the command, compiler, host,
revision, and full output when using a run as evidence. A smoke run is neither a stable result nor a
database performance claim.

## Fuzz targets

The fuzz preset is optional and requires Clang with a linkable libFuzzer runtime. It builds the
ByteReader operation-sequence and WAL physical-codec targets with ASan and UBSan while leaving
ordinary tests out of that build tree:

```sh
cmake --preset fuzz
cmake --build --preset fuzz
build/fuzz/chronos_byte_reader_fuzz -runs=10000 -max_len=4096
build/fuzz/chronos_wal_codec_fuzz -runs=10000 -max_len=16777216
build/fuzz/chronos_cseg_metadata_codec_fuzz -runs=10000 -max_len=8388608
build/fuzz/chronos_cseg_plain_page_fuzz -runs=10000 -max_len=1048576
```

Apple's Command Line Tools compiler may omit the libFuzzer runtime even when it accepts Clang
sanitizer flags. Configuration detects that case and fails with a direct diagnostic. On a Homebrew
LLVM installation, select that compiler before a fresh configure:

```sh
CC="$(brew --prefix llvm)/bin/clang" CXX="$(brew --prefix llvm)/bin/clang++" cmake --fresh --preset fuzz
cmake --build --preset fuzz
```

If LeakSanitizer on macOS reports only exit-time allocations owned by the libFuzzer/symbolizer
runtime, first retain that output and ensure the normal ASan/UBSan test suite passes. A bounded fuzz
smoke may then be repeated with `ASAN_OPTIONS=detect_leaks=0`; do not use that workaround to dismiss
a leak whose stack enters ChronosDB code, and do not describe a smoke run as a fuzz campaign.

## Troubleshooting

- `Could not find Ninja`: install Ninja and rerun configure.
- a FetchContent clone fails: confirm Git/network access, then rerun the same configure command.
- `clang-tidy was not found`: install it or set `CLANG_TIDY` to its executable before running the
  lint script.
- `std::expected` is unavailable: select a standard library that implements the required C++23 API
  and use a fresh build tree; Ubuntu 24.04 Clang uses the libc++ command shown above.
- stale compiler or option results: remove only the affected `build/<preset>` directory and
  reconfigure. The scripts never delete build trees.
- no Git metadata: builds outside a checkout remain valid and report Git fields as unavailable.
- the fuzz preset reports a missing runtime: select a Clang distribution that includes libFuzzer;
  enabling the option with GCC or a runtime-less AppleClang is intentionally unsupported.
