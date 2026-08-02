# Building ChronosDB

ChronosDB's implemented code currently consists of the Phase 1A build/version foundation and the
Phase 1B portable common binary primitives. Engine components described elsewhere remain planned.
The reference production platform is Linux x86-64; this portable common code also supports modern
macOS, including Apple silicon.

## Prerequisites

- CMake 3.25 or newer
- Ninja 1.10 or newer
- a C++23 compiler and standard library that provide `std::expected`: GCC 13+, Clang 17+ paired
  with a capable libc++/libstdc++, or a current AppleClang
- Git, so CMake can fetch pinned test dependencies and optionally record revision metadata
- Python only as required by CMake/GoogleTest test discovery
- clang-format and clang-tidy from a reasonably current LLVM release (17+ recommended)

The first test or benchmark configuration needs network access to fetch its pinned dependency.
Subsequent configurations reuse CMake's build-tree dependency checkout. No dependency source is
vendored into this repository.

## Linux setup

On Ubuntu 24.04, install the distribution packages for `cmake`, `ninja-build`, `g++`, `clang`,
`libc++-dev`, `libc++abi-dev`, `clang-format`, and `clang-tidy`. GCC and Clang are both
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
brew install cmake ninja llvm
```

Homebrew's LLVM tools may not be on `PATH`. If so, set `CLANG_FORMAT` and `CLANG_TIDY` to the
corresponding executables under `$(brew --prefix llvm)/bin`. AppleClang builds the portable common
targets. Future server, direct-I/O, and reactor components may require Linux and will be guarded by
explicit platform checks rather than weakened portable interfaces.

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

Install to a staging prefix with `cmake --install build/release --prefix <directory>`. This installs
`chronosctl`, the common library and public headers, and a CMake package exporting
`chronos::common`.

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

The optional benchmark preset builds Release-mode CRC32C, ByteReader, ByteWriter, and harness
microbenchmarks:

```sh
cmake --preset benchmark
cmake --build --preset benchmark
build/benchmark/chronos_common_benchmarks
```

The executable labels results as local measurements only. Record the command, compiler, host,
revision, and full output when using a run as evidence. A smoke run is neither a stable result nor a
database performance claim.

## ByteReader fuzz target

The fuzz preset is optional and requires Clang with a linkable libFuzzer runtime. It builds the
ByteReader operation-sequence target with ASan and UBSan while leaving ordinary tests out of that
build tree:

```sh
cmake --preset fuzz
cmake --build --preset fuzz
build/fuzz/chronos_byte_reader_fuzz -runs=10000 -max_len=4096
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
