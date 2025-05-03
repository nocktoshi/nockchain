# Set env variables
export RUST_BACKTRACE := full
export RUST_LOG := info,nockchain=debug,nockchain_libp2p_io=info,libp2p=info,libp2p_quic=info
export MINIMAL_LOG_FORMAT := true


## Build everything
.PHONY: build
build:
	cargo build --release

## Run all tests
.PHONY: test
test:
	cargo test --release

.PHONY: install-choo
install-choo: nuke-choo-data ## Install choo from this repo
	$(call show_env_vars)
	cargo install --locked --force --path crates/nockapp/apps/choo --bin choo

.PHONY: ensure-dirs
ensure-dirs:
	mkdir -p hoon
	mkdir -p assets

.PHONY: build-trivial-new
build-trivial-new: ensure-dirs
	$(call show_env_vars)
	echo '%trivial' > hoon/trivial.hoon
	choo --new --arbitrary hoon/trivial.hoon

HOON_TARGETS=assets/dumb.jam assets/wal.jam

.PHONY: nuke-choo-data
nuke-choo-data:
	rm -rf .data.choo
	rm -rf ~/.nockapp/choo

.PHONY: nuke-assets
nuke-assets:
	rm -f assets/*.jam

.PHONY: build-hoon-fresh
build-hoon-fresh: nuke-assets nuke-choo-data install-choo ensure-dirs build-trivial-new $(HOON_TARGETS)
	$(call show_env_vars)

.PHONY: build-hoon-new
build-hoon-all: ensure-dirs build-trivial-new $(HOON_TARGETS)
	$(call show_env_vars)

.PHONY: build-hoon
build-hoon: ensure-dirs $(HOON_TARGETS)
	$(call show_env_vars)

HOON_SRCS := $(find hoon -type file -name '*.hoon')

## Build dumb.jam with choo
assets/dumb.jam: hoon/apps/dumbnet/outer.hoon $(HOON_SRCS)
	$(call show_env_vars)
	RUST_LOG=trace choo hoon/apps/dumbnet/outer.hoon hoon
	mv out.jam assets/dumb.jam

## Build wal.jam with choo
assets/wal.jam: hoon/apps/wallet/wallet.hoon $(HOON_SRCS)
	$(call show_env_vars)
	RUST_LOG=trace choo hoon/apps/wallet/wallet.hoon hoon
	mv out.jam assets/wal.jam
