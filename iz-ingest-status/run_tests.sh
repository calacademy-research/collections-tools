#!/bin/bash
# Run all tests for the IZ Ingest Status tool.
# Usage: ./run_tests.sh [optional pytest args]
#
# Examples:
#   ./run_tests.sh                    # Run all tests
#   ./run_tests.sh -v                 # Verbose output
#   ./run_tests.sh tests/test_classify.py  # Run one file
#   ./run_tests.sh -k "test_casiz"    # Run tests matching pattern

set -e

cd "$(dirname "$0")"

if [ -d "venv" ]; then
    source venv/bin/activate
fi

python -m pytest "$@"
