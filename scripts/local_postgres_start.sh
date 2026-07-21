#!/usr/bin/env bash
set -euo pipefail

PG_BIN="/opt/homebrew/opt/postgresql@18/bin"
PGDATA="/opt/homebrew/var/postgresql@18"
PGLOG="${PGDATA}/server.log"

"${PG_BIN}/pg_ctl" -D "${PGDATA}" -l "${PGLOG}" start
