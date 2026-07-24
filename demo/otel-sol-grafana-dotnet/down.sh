#!/bin/bash
set -eux

docker compose down --remove-orphans -v --rmi local
