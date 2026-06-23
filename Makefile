.PHONY: maelstrom-test maelstrom-test-quick maelstrom-test-stress tla-check

MAELSTROM_TEST_RUNNER := ./tests/maelstrom-tests/run_tests.sh
TLA_TEST_RUNNER := ./specs/tla/run_tlc.sh

# Run the complete Maelstrom correctness suite.
maelstrom-test:
	$(MAELSTROM_TEST_RUNNER) all

# Run the shortest adapter and protocol smoke test.
maelstrom-test-quick:
	$(MAELSTROM_TEST_RUNNER) smoke

# Run the extended partition and convergence scenario.
maelstrom-test-stress:
	$(MAELSTROM_TEST_RUNNER) convergence

# Run the bounded TLA+ model checks.
tla-check:
	$(TLA_TEST_RUNNER)
