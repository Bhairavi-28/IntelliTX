#!/usr/bin/env bash
# C++ tests + Python/MySQL/dashboard tests. Requires .env (or exported MYSQL_* variables) and MySQL running.
set -euo pipefail
cd "$(dirname "$0")/.."
./build/itx_tests
export MYSQL_DATABASE="${MYSQL_TEST_DATABASE:-intellitx_test}"   # tests recreate the schema: never point at real data
python3 -m pytest -q
