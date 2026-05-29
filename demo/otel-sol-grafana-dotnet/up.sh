#!/bin/bash
set -eux

PROFILES="${1:+--profile $1}"
docker compose $PROFILES down --remove-orphans -v --rmi local && docker compose $PROFILES up
