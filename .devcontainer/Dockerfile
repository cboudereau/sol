FROM rust:1.92-bookworm

# System dependencies for Sol (from docs/DEV_ENVIRONMENT.md)
RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    cmake \
    curl \
    git \
    ca-certificates \
    libclang-dev \
    libsasl2-dev \
    libssl-dev \
    libtool \
    pkg-config \
    protobuf-compiler \
    libprotobuf-dev \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Clippy + rustfmt
RUN rustup component add clippy rustfmt

# Non-root user for devcontainer
RUN useradd -m -s /bin/bash vscode \
    && echo "vscode ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/vscode

# Install Claude Code CLI
# WORKDIR /tmp avoids installer scanning the entire filesystem
USER vscode
WORKDIR /tmp
RUN curl -fsSL https://claude.ai/install.sh | bash
ENV PATH="/home/vscode/.local/bin:${PATH}"
VOLUME /workspace
WORKDIR /workspace

ENTRYPOINT claude
