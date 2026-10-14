#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Only disposable containers, random loopback ports, and a fresh SQL schema.
pg_name="quip-ingestion-pg-$$"
redis_name="quip-ingestion-redis-$$"
cleanup() {
  docker rm -f "$pg_name" "$redis_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker run --rm -d --name "$pg_name" -e POSTGRES_PASSWORD=ingestion-test \
  -e POSTGRES_DB=ingestion_test -p 127.0.0.1::5432 postgres:16-alpine >/dev/null
docker run --rm -d --name "$redis_name" -p 127.0.0.1::6379 redis:7-alpine >/dev/null
pg_addr=$(docker port "$pg_name" 5432/tcp)
redis_addr=$(docker port "$redis_name" 6379/tcp)
for attempt in {1..30}; do
  if docker exec "$pg_name" pg_isready -U postgres -d ingestion_test >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
export INGESTION_TEST_POSTGRES="host=127.0.0.1 port=${pg_addr##*:} user=postgres password=ingestion-test dbname=ingestion_test sslmode=disable"
export INGESTION_TEST_REDIS="$redis_addr"
export NETWORK_NODE="ingestion_test_$$"
go test ./internal/dao/ingestiontest -count=1 -timeout=60s "$@"
