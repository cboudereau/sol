FROM docker.io/alpine:3.23 AS builder

WORKDIR /sol

ARG TARGETPLATFORM

COPY sol-*-unknown-linux-musl*.tar.gz ./

# special case for arm v6 builds, /etc/apk/arch reports armhf which conflicts with the armv7 package
RUN ARCH=$(if [ "$TARGETPLATFORM" = "linux/arm/v6" ]; then echo "arm"; else cat /etc/apk/arch; fi) \
    && tar -xvf sol-0*-"$ARCH"-unknown-linux-musl*.tar.gz --strip-components=2

RUN mkdir -p /var/lib/sol

FROM docker.io/alpine:3.23

# https://github.com/opencontainers/image-spec/blob/main/annotations.md
LABEL org.opencontainers.image.url="https://github.com/cboudereau/sol"
LABEL org.opencontainers.image.source="https://github.com/cboudereau/sol"
LABEL org.opencontainers.image.documentation="https://github.com/cboudereau/sol"

# we want the latest versions of these
# hadolint ignore=DL3018
RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /sol/bin/* /usr/local/bin/
COPY --from=builder /sol/config/sol.yaml /etc/sol/sol.yaml
COPY --from=builder /var/lib/sol /var/lib/sol

# Smoke test
RUN ["sol", "--version"]

ENTRYPOINT ["/usr/local/bin/sol"]
