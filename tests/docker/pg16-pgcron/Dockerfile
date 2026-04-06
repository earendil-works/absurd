FROM postgres:16-alpine

ARG PG_CRON_VERSION=v1.6.7

RUN set -eux; \
    apk add --no-cache \
      libfaketime; \
    apk add --no-cache --virtual .build-deps \
      build-base \
      clang19 \
      git \
      llvm19 \
      postgresql16-dev; \
    git clone --depth 1 --branch "${PG_CRON_VERSION}" https://github.com/citusdata/pg_cron.git /tmp/pg_cron; \
    make -C /tmp/pg_cron; \
    make -C /tmp/pg_cron install; \
    rm -rf /tmp/pg_cron; \
    apk del .build-deps

# Keep startup fake time far in the past so test jumps are always forward.
# pg_cron minute schedules can stall after backward jumps.
RUN printf '%s\n' '-50y' > /tmp/absurd-faketime.ts

RUN mv /usr/local/bin/postgres /usr/local/bin/postgres.real && \
    cat > /usr/local/bin/postgres <<'EOF' && \
    chmod +x /usr/local/bin/postgres
#!/bin/sh
enable_faketime=1
for arg in "$@"; do
  case "$arg" in
    *listen_addresses*)
      # During docker-entrypoint temporary server startup, keep real time.
      enable_faketime=0
      break
      ;;
  esac
done

if [ "$enable_faketime" = "1" ]; then
  export LD_PRELOAD=/usr/lib/faketime/libfaketime.so.1
  export FAKETIME_TIMESTAMP_FILE=/tmp/absurd-faketime.ts
  export FAKETIME_NO_CACHE=1
  export FAKETIME_DONT_FAKE_MONOTONIC=1
fi

exec /usr/local/bin/postgres.real "$@"
EOF
