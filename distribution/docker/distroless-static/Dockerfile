FROM docker.io/alpine:3.23 AS builder

WORKDIR /sol

COPY sol-*-unknown-linux-musl*.tar.gz ./
RUN tar -xvf sol-0*-"$(cat /etc/apk/arch)"-unknown-linux-musl*.tar.gz --strip-components=2

RUN mkdir -p /var/lib/sol

# distroless doesn't use static tags
# hadolint ignore=DL3007
FROM gcr.io/distroless/static:latest

# https://github.com/opencontainers/image-spec/blob/main/annotations.md
LABEL org.opencontainers.image.url="https://github.com/cboudereau/sol"
LABEL org.opencontainers.image.source="https://github.com/cboudereau/sol"
LABEL org.opencontainers.image.documentation="https://github.com/cboudereau/sol"

COPY --from=builder /sol/bin/* /usr/local/bin/
COPY --from=builder /sol/config/sol.yaml /etc/sol/sol.yaml
COPY --from=builder /var/lib/sol /var/lib/sol

# Smoke test
RUN ["sol", "--version"]

ENTRYPOINT ["/usr/local/bin/sol"]
