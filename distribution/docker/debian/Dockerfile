FROM docker.io/debian:trixie-slim AS builder

WORKDIR /sol

COPY sol_*.deb ./
RUN dpkg -i sol_*_"$(dpkg --print-architecture)".deb

RUN mkdir -p /var/lib/sol

FROM docker.io/debian:trixie-slim

# https://github.com/opencontainers/image-spec/blob/main/annotations.md
LABEL org.opencontainers.image.url="https://github.com/cboudereau/sol"
LABEL org.opencontainers.image.source="https://github.com/cboudereau/sol"
LABEL org.opencontainers.image.documentation="https://github.com/cboudereau/sol"

# we want the latest versions of these
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata systemd && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/bin/sol /usr/bin/sol
COPY --from=builder /usr/share/sol /usr/share/sol
COPY --from=builder /usr/share/doc/sol /usr/share/doc/sol
COPY --from=builder /etc/sol /etc/sol
COPY --from=builder /var/lib/sol /var/lib/sol

# Smoke test
RUN ["sol", "--version"]

ENTRYPOINT ["/usr/bin/sol"]
