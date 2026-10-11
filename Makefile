.PHONY: all fmt fmt-check clippy test test-lib doc examples dupes dupes-cleanup check e2e check-all help

# cargo-dupes reads dupes.toml ONLY from the analysed path (no parent lookup)
# and silently passes when it finds none, so the gate refuses to run without it.
DUPES_CFG := dupes.toml
DUPES_VERSION := 0.2.1

# Default target: run all mandatory quality gates
all: check

# Auto-fix code formatting
fmt:
	cargo fmt --all

# Verify code formatting
fmt-check:
	cargo fmt --all -- --check

# Lint every target (lib, tests, examples) with warnings as errors
clippy:
	cargo clippy --all-targets --all-features -- -D warnings

# Every test target `cargo test` runs: unit tests in src/ and doctests.
# The gate covers exactly what `cargo test` covers.
test:
	cargo test --all-features

# Fast inner loop: library unit tests only (not part of `make check`)
test-lib:
	cargo test --lib --all-features

# Rustdoc must build without warnings (broken intra-doc links, missing docs)
doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

# The examples need a running Firefox to RUN, but must always BUILD.
# `clippy --all-targets` already compiles them; this target is a readable alias.
examples:
	cargo build --examples --all-features

# Check for code duplication (requires cargo-dupes)
dupes:
	@test -x "$$(command -v cargo-dupes)" || (echo "cargo-dupes not found. Installing..." && cargo install cargo-dupes --version $(DUPES_VERSION) --locked)
	@test -f "$(DUPES_CFG)" || (echo "ERROR: $(DUPES_CFG) missing; cargo-dupes would silently pass without thresholds" && exit 1)
	cargo dupes --path . check

# Show stale duplication suppressions
dupes-cleanup:
	@test -x "$$(command -v cargo-dupes)" || (echo "cargo-dupes not found. Installing..." && cargo install cargo-dupes --version $(DUPES_VERSION) --locked)
	cargo dupes --path . cleanup --dry-run

# All local quality gates, sequentially
check: fmt-check clippy test doc dupes

# Smoke-run the examples against a dedicated real Firefox (Linux and macOS).
# Not part of `make check`. See README > End-to-end smoke.
E2E_SCRIPT ?= tests/run_all_e2e.sh
e2e:
	@if [ -x "$(E2E_SCRIPT)" ]; then ./$(E2E_SCRIPT); else echo "e2e: $(E2E_SCRIPT) not present yet - skipped"; fi

# All local quality gates AND the E2E smoke
check-all: check e2e

help:
	@echo "Available targets:"
	@echo "  make (or make all) - Alias for 'make check'"
	@echo "  make check         - All local quality gates (fmt-check, clippy, test, doc, dupes)"
	@echo "  make test-lib      - Library unit tests only (fast inner loop, not a gate)"
	@echo "  make check-all     - Quality gates AND the E2E smoke against real Firefox"
	@echo "  make fmt           - Auto-fix formatting"
	@echo "  make fmt-check     - Verify formatting"
	@echo "  make clippy        - Lints with -D warnings on every target"
	@echo "  make test          - Unit tests and doctests"
	@echo "  make doc           - Rustdoc with -D warnings"
	@echo "  make examples      - Build the examples"
	@echo "  make dupes         - Duplication check (auto-installs cargo-dupes $(DUPES_VERSION))"
	@echo "  make dupes-cleanup - Show stale duplication suppressions"
	@echo "  make e2e           - Run the examples against a real Firefox (tests/run_all_e2e.sh)"
