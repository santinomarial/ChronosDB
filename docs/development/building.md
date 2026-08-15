# Building ChronosDB

ChronosDB remains pre-alpha, but the repository now builds implemented storage, vector-query,
network, live-query, Raft, distributed-query, tiering, runtime, and service slices plus operator and
benchmark tools. Phase 9 and Phase 10 have accepted exit evidence; Phase 11 onward retain explicit
feature and qualification gaps in the [roadmap](../roadmap.md) and
[feature-completion review](../reviews/feature-completion-pass.md). The reference production
platform is Linux x86-64; implemented portable and POSIX targets also support modern macOS,
including Apple silicon. macOS correctness support is not a power-loss durability claim.

## Prerequisites

- CMake 3.25 or newer
- Ninja 1.10 or newer
- a C++23 compiler and standard library that provide `std::expected`: GCC 13+, Clang 17+ paired
  with a capable libc++/libstdc++, or a current AppleClang
- Git, so CMake can fetch pinned test dependencies and optionally record revision metadata
- OpenSSL 3, Zstandard 1.5.5 or newer, and libcurl 7.75 or newer production development packages;
  Apache Arrow and Parquet 25.x when optional interoperability is enabled
- Python only as required by CMake/GoogleTest test discovery
- clang-format 18 and clang-tidy 18 exactly

The first test or benchmark configuration needs network access to fetch its pinned dependency.
Subsequent configurations reuse CMake's build-tree dependency checkout. No dependency source is
vendored into this repository.

## Linux setup

On Ubuntu 24.04, install the distribution packages for `cmake`, `ninja-build`, `g++`, `clang`,
`libc++-dev`, `libc++abi-dev`, `libssl-dev`, `libzstd-dev`, `libcurl4-openssl-dev`,
`clang-format-18`, and
`clang-tidy-18`. GCC and Clang are both
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
brew install cmake ninja llvm llvm@18 openssl@3 zstd apache-arrow
```

Homebrew's versioned LLVM tools may not be on `PATH`. `scripts/format.sh` searches the standard
`llvm@18` prefix; otherwise set `CLANG_FORMAT=$(brew --prefix llvm@18)/bin/clang-format`. Set
`CLANG_TIDY=$(brew --prefix llvm@18)/bin/clang-tidy` when the versioned executable is not on `PATH`.
AppleClang builds the implemented portable targets plus the macOS POSIX I/O backend. The backend uses
`fsync` where Linux uses `fdatasync`; this does not advertise a macOS power-loss envelope. Future
direct-I/O work and Linux-only reactor capabilities such as epoll/io_uring execution remain guarded
by explicit platform checks rather than weakened portable interfaces.

Arrow IPC and Parquet file import/export are built explicitly and keep third-party types out of
public headers:

```sh
cmake --preset debug -DCHRONOS_ENABLE_ARROW_INTEROP=ON
cmake --build --preset debug --target chronos_interop_tests
ctest --preset debug -R ArrowParquet
```

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
build/dev/chronosd --help
```

`chronosd` packages the Linux-authoritative loopback native-protocol lifecycle. With no data
directory its startup banner says `data_plane=unconfigured` and data-plane requests fail explicitly.
Supplying `--data-dir PATH` opens or creates a recoverable single-node database and reports
`data_plane=configured`; native CREATE TABLE, single-local-tablet SQL INSERT VALUES, canonical
ingest, and the supported vector SELECT subset are dispatched. Paired `--subscription-sql` and
`--subscription-key-file` options serve one durable row-preserving plan. An already provisioned
replicated root can instead use `--replicated-groups`; multi-voter operation also requires the
complete authenticated peer/TLS bundle. See the exact supported modes and fail-closed gaps in the
[native server operations baseline](../operations/native-server.md).

The read-only WAL inspector acquires the existing writer lock, verifies the complete physical log,
preflights every record, and then prints record metadata in deterministic order without dumping
payload contents:

```sh
build/dev/chronos-waldump <path-to-wal-directory>
```

It exits `0` for a clean WAL, `3` for an incomplete but potentially repairable final tail, `1` for
verification or lock failure, and `2` for invalid command-line use. It never repairs or creates a
missing `LOCK` file.

The read-only CSEG inspector validates exactly one complete CSEG v1 candidate in memory, including
all schema-independent system-row, extrema, and ordering semantics. It prints metadata and storage
accounting without row values and never modifies the input:

```sh
build/dev/chronos-csegdump [--max-bytes N] [--descriptors] <path-to-cseg-file>
```

It exits `0` for valid bytes, `3` for an incomplete prefix, `4` for a recognized but unsupported
durable value, `1` for corruption, resource-limit, or I/O failure, and `2` for invalid command-line
use. Catalog schema binding requires the separate schema-aware library API.

Install to a staging prefix with `cmake --install build/release --prefix <directory>`. This installs
the operator and benchmark tools, all public headers, the common, schema, columnar, CSEG, POSIX I/O,
WAL, head, ingest, Manifest, query, network, runtime, live, Raft, cluster, tiering, and service
libraries, and a CMake package exporting their `chronos::` targets. Optional Arrow interoperability
is installed when enabled. The test suite builds and runs an external project against every
installed public target.

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

The fuzz preset is optional and requires Clang with a linkable libFuzzer runtime. It builds all
current common, WAL, columnar, ingest, CSEG, Manifest, and SQL decoder/parser targets with ASan and
UBSan while leaving ordinary tests out of that build tree. The deterministic smoke script copies the
checked-in binary and SQL seed corpora to temporary writable directories, runs every target with a
fixed seed, and retains crash artifacts under the build tree:

```sh
cmake --preset fuzz
cmake --build --preset fuzz
FUZZ_RUNS=10000 FUZZ_SEED=424242 FUZZ_MAX_LEN=4096 scripts/fuzz-smoke.sh build/fuzz
```

The smoke defaults to `-entropic=0` for reproducible behavior across the pinned CI runtime and newer
local libFuzzer releases; this changes scheduling, not sanitizer coverage or decoder assertions. Set
`FUZZ_ENTROPIC=1` for longer exploratory campaigns on a qualified runtime. Durable-format harnesses
also execute structurally valid in-memory fixtures before applying input-directed mutations, so an
empty or syntax-heavy corpus cannot prevent success-path coverage. CI runs 1,000 iterations per
target as a bounded regression smoke. Longer corpus-growing campaigns remain separate evidence and
must retain their exact settings and artifacts.

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
- `clang-tidy 18 was not found`: install it or set `CLANG_TIDY` to the exact 18.x executable before
  running the lint script.
- `std::expected` is unavailable: select a standard library that implements the required C++23 API
  and use a fresh build tree; Ubuntu 24.04 Clang uses the libc++ command shown above.
- stale compiler or option results: remove only the affected `build/<preset>` directory and
  reconfigure. The scripts never delete build trees.
- no Git metadata: builds outside a checkout remain valid and report Git fields as unavailable.
- the fuzz preset reports a missing runtime: select a Clang distribution that includes libFuzzer;
  enabling the option with GCC or a runtime-less AppleClang is intentionally unsupported.
