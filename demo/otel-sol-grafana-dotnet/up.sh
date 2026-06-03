#!/bin/bash
set -eux

# docker compose down --remove-orphans -v --rmi local && docker compose up
docker compose down --rmi local && docker compose up
