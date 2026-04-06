FROM postgres:16-alpine

ARG PG_CRON_VERSION=v1.6.7

RUN set -eux; \
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
