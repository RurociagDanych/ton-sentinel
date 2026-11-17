#!/bin/sh
# Creates the git-ignored Grafana admin password file (mode 0600) if absent. Never prints it.
set -eu
d="$(dirname "$0")/../../.cache/secrets"; f="$d/grafana_admin"
mkdir -p "$d"
if [ ! -s "$f" ]; then (umask 077; head -c 24 /dev/urandom | base64 | tr -d '\n=+/' > "$f"); fi
chmod 600 "$f"
