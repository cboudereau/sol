FROM debian:trixie-slim
ARG DUCKDB_VERSION=v1.3.0
RUN apt-get update && apt-get install -y --no-install-recommends wget unzip ca-certificates \
    && wget -qO /tmp/duckdb.zip "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip" \
    && unzip /tmp/duckdb.zip -d /usr/local/bin \
    && rm /tmp/duckdb.zip \
    && apt-get purge -y wget unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["duckdb"]
