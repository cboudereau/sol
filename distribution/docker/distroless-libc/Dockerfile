FROM docker.io/debian:trixie-slim AS builder

WORKDIR /sol

COPY sol_*.deb ./
RUN dpkg -i sol_*_"$(dpkg --print-architecture)".deb

RUN mkdir -p /var/lib/sol

# distroless doesn't use static tags
# hadolint ignore=DL3007
FROM gcr.io/distroless/cc-debian12:latest

# https://github.com/opencontainers/image-spec/blob/main/annotations.md
LABEL org.opencontainers.image.url="https://github.com/cboudereau/sol"
LABEL org.opencontainers.image.source="https://github.com/cboudereau/sol"
LABEL org.opencontainers.image.documentation="https://github.com/cboudereau/sol"

COPY --from=builder /usr/lib/*-linux-gnu/libz.so.1 /usr/lib/
COPY --from=builder /usr/bin/sol /usr/bin/sol
COPY --from=builder /usr/share/doc/sol /usr/share/doc/sol
COPY --from=builder /usr/share/sol /usr/share/sol
COPY --from=builder /etc/sol /etc/sol
COPY --from=builder /var/lib/sol /var/lib/sol

# Smoke test
RUN ["sol", "--version"]

ENTRYPOINT ["/usr/bin/sol"]
