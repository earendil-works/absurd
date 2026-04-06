#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-with-sys-time}"
MUTATIONS="${MUTATIONS:-15}"
SLEEP_BETWEEN="${SLEEP_BETWEEN:-2}"
BASE_EPOCH="${BASE_EPOCH:-1711972800}" # 2024-04-01T12:00:00Z
IMAGE="${IMAGE:-postgres:16-alpine}"

if [[ "$MODE" != "with-sys-time" && "$MODE" != "without-sys-time" ]]; then
  echo "unknown mode: $MODE" >&2
  exit 2
fi

name="clock-repro-${MODE}-${GITHUB_RUN_ID:-local}-${RANDOM}"

cleanup() {
  set +e
  if [[ -n "${CID:-}" ]]; then
    docker rm -f "$CID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "$MODE" == "with-sys-time" ]]; then
  CID="$(docker run -d --name "$name" --cap-add SYS_TIME "$IMAGE" sh -lc 'sleep 600')"
else
  CID="$(docker run -d --name "$name" "$IMAGE" sh -lc 'sleep 600')"
fi
echo "mode=$MODE container=$CID image=$IMAGE"

docker exec "$CID" sh -lc "date -u +'%Y-%m-%dT%H:%M:%SZ'" | sed 's/^/[container-time] /'
date -u +'%Y-%m-%dT%H:%M:%SZ' | sed 's/^/[host-time] /'

for i in $(seq 1 "$MUTATIONS"); do
  ts=$((BASE_EPOCH + (i - 1) * 7200))

  date -u +'%Y-%m-%dT%H:%M:%SZ' | sed "s/^/[heartbeat $i][host-time] /"
  if curl -fsS --max-time 5 https://api.github.com/meta >/dev/null; then
    echo "[heartbeat $i][network] ok"
  else
    echo "[heartbeat $i][network] failed"
  fi

  if [[ "$MODE" == "with-sys-time" ]]; then
    echo "[mutate $i] date -u -s @$ts"
    if ! out="$(docker exec -u root "$CID" sh -lc "date -u -s @$ts" 2>&1)"; then
      echo "[mutate $i] failed: $out"
      exit 1
    fi
    echo "[mutate $i] ok: $out"
  else
    out="$(docker exec "$CID" sh -lc "date -u +'%Y-%m-%dT%H:%M:%SZ'" 2>&1)"
    echo "[baseline $i] container time: $out"
  fi

  sleep "$SLEEP_BETWEEN"
done

echo "completed mode=$MODE"
