FROM debian:12.13-slim AS build

ENV DEBIAN_FRONTEND=noninteractive

RUN \
  apt-get update -y \
  && apt-get install -y make curl build-essential

RUN \
  apt-get update && \
  curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
  apt-get install -y --no-install-recommends nodejs

RUN \
  apt-get update && \
  apt-get install -y --no-install-recommends ca-certificates && \
  echo "deb http://deb.debian.org/debian bookworm-backports main" \
    > /etc/apt/sources.list.d/bookworm-backports.list && \
  apt-get update && \
  apt-get install -y --no-install-recommends -t bookworm-backports golang-go golang-1.23-go

COPY . .

RUN make build

FROM debian:12.13-slim

COPY --from=build bin/habitat /bin/habitat

EXPOSE 7890
ENTRYPOINT [ "/bin/habitat" ]
CMD [ "run" ]
