ARG RUST_VERSION=1.92
ARG DEBIAN_RELEASE=trixie

FROM docker.io/rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS builder
RUN apt-get update && apt-get -y --no-install-recommends install \
    build-essential git clang cmake libclang-dev \
    libsasl2-dev libssl-dev libxxhash-dev zlib1g-dev zlib1g

WORKDIR /sol

COPY . .
RUN scripts/environment/install-protoc.sh
RUN --mount=type=cache,target=/sol/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --bin sol && \
    cp target/release/sol .

FROM docker.io/debian:${DEBIAN_RELEASE}-slim
RUN apt-get update && apt-get -y --no-install-recommends install zlib1g ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /sol/sol /usr/bin/sol
RUN mkdir -p /var/lib/sol

RUN ["sol", "--version"]

ENTRYPOINT ["/usr/bin/sol"]
