# Default target
all: binaries

binaries:
	cd go-libp2p && go build -linkshared -o gossipsub-bin
	cd rust-libp2p && cargo build
	# Build JS binary
	cd js-libp2p && npm install && npm run build

# Clean all generated shadow simulation files
clean:
	rm -rf shadow-outputs || true
	rm plots/* || true

test:
	# Testing partial messages
	@echo "Testing partial messages"
	@uv run run.py --node_count 32 --composition "rust-and-go" --scenario "partial-messages" && uv run checks/partial_messages.py latest --count 1

	@echo "Testing partial messages chain"
	@uv run run.py --node_count 8 --composition "rust-and-go" --scenario "partial-messages-chain" && uv run checks/partial_messages.py latest --count 16

	@echo "Testing fanout"
	uv run run.py --node_count 8 --composition "rust-and-go" --scenario "partial-messages-fanout" && uv run checks/partial_messages.py latest/
	uv run run.py --node_count 8 --seed 1 --composition "rust-and-go" --scenario "partial-messages-fanout" && uv run checks/partial_messages.py latest/
	uv run run.py --node_count 8 --seed 2 --composition "rust-and-go" --scenario "partial-messages-fanout" && uv run checks/partial_messages.py latest/
	uv run run.py --node_count 8 --seed 3 --composition "rust-and-go" --scenario "partial-messages-fanout" && uv run checks/partial_messages.py latest/

test-go:
	# Testing partial messages
	@echo "Testing partial messages"
	@uv run run.py --node_count 8 --composition "all-go" --scenario "partial-messages" && uv run checks/partial_messages.py latest --count 1

	@echo "Testing partial messages chain"
	@uv run run.py --node_count 8 --composition "all-go" --scenario "partial-messages-chain" && uv run checks/partial_messages.py latest --count 16

	@echo "Testing fanout"
	@uv run run.py --node_count 2 --composition "all-go" --scenario "partial-messages-fanout" && uv run checks/partial_messages.py latest/


test-rust-only:
	# Testing partial messages
	@echo "Testing partial messages"
	@uv run run.py --node_count 8 --composition "all-rust" --scenario "partial-messages" && uv run checks/partial_messages.py latest --count 1

	@echo "Testing partial messages chain"
	@uv run run.py --node_count 8 --composition "all-rust" --scenario "partial-messages-chain" && uv run checks/partial_messages.py latest --count 16

	@echo "Testing fanout"
	@uv run run.py --node_count 2 --composition "all-rust" --scenario "partial-messages-fanout" && uv run checks/partial_messages.py latest/


test-js:
	# Testing JS partial messages
	@echo "Testing JS partial messages"
	@uv run run.py --node_count 2 --composition "all-js" --scenario "partial-messages" && uv run checks/partial_messages.py latest --count 1

	@echo "Testing JS partial messages chain"
	@uv run run.py --node_count 8 --composition "all-js" --scenario "partial-messages-chain" && uv run checks/partial_messages.py latest --count 16

test-js-and-go:
	# Testing JS and Go interop
	@echo "Testing JS and Go partial messages"
	@uv run run.py --node_count 4 --composition "js-and-go" --scenario "partial-messages" && uv run checks/partial_messages.py latest --count 1

test-js-and-rust:
	# Testing JS and Rust interop
	@echo "Testing JS and Rust partial messages"
	@uv run run.py --node_count 4 --composition "js-and-rust" --scenario "partial-messages" && uv run checks/partial_messages.py latest --count 1

test-all-mixed:
	# Testing all implementations together (Go, Rust, JS)
	@echo "Testing all mixed partial messages"
	@uv run run.py --node_count 6 --composition "all-mixed" --scenario "partial-messages" && uv run checks/partial_messages.py latest --count 1

.PHONY: binaries all clean test test-go test-rust-only test-js test-js-and-go test-js-and-rust test-all-mixed
