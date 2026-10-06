#!/bin/bash
set -e

echo "=== Akiraka starting (Akari + enricher) ==="

if [ -z "$NS_USER_AGENT" ]; then
    echo "[ERROR] NS_USER_AGENT environment variable is required (your main nation name)."
    exit 1
fi

# Run as the host user rather than root, when asked to.
#
# Unraid runs containers as root by default, which means the store, its WAL and
# its shm end up owned by root. The array user - uid 1000 by default - then
# cannot manage them, and the share UI shows files nobody can move. On Windows
# this is invisible because Docker's filesystem synthesises permissions; on Linux
# they are real. PUID/PGID is the convention every Linux container already uses,
# so the same image works either way.
#
# Root is kept as the default rather than switching the default to non-root,
# because changing a UID inside a mounted volume means finding every existing
# file and chowning it, and a store that suddenly cannot be opened is a much
# worse outcome than root-owned files.
#
# Dropping privilege needs care in two places: the data directory is created and
# chowned before the switch, and the config directory is left alone because it is
# a read-only mount on a host the array user may not own.
PUID="${PUID:-0}"
PGID="${PGID:-0}"
RUN_AS_ROOT=0

if [ "$PUID" != "0" ] || [ "$PGID" != "0" ]; then
    echo "Running as uid=$PUID gid=$PGID (PUID/PGID)"

    # The group is needed even when only the uid is given, which is the common
    # case on Unraid where both are set but plenty of setups set one.
    if ! getent group "$PGID" >/dev/null 2>&1; then
        groupadd -g "$PGID" akiraka 2>/dev/null || true
    fi
    if ! getent passwd "$PUID" >/dev/null 2>&1; then
        useradd -u "$PUID" -g "$PGID" -M -s /usr/sbin/nologin akiraka 2>/dev/null || true
    fi
    RUN_AS_ROOT=1
fi

  CONFIG_PATH=${CONFIG_PATH:-/app/config/akari.toml}
  AKARI_LOG=${AKARI_LOG:-/data/akari_events.jsonl}
  CTE_LOG=${CTE_LOG:-/data/events.enriched.cte.jsonl}
  EVENT_STORE=${EVENT_STORE:-/data/events.db}
  QUERY_CONFIG=${QUERY_CONFIG:-/app/config/endpoints.json}

mkdir -p /data /app/config

# chown before the switch rather than after: once the process is non-root it
# cannot give back ownership, so a pre-existing root-owned store would be
# unreadable and fail at open rather than at startup, which is a worse place to
# find out.
if [ "$RUN_AS_ROOT" = "1" ]; then
    # Only the data directory. The config directory is usually a read-only mount
    # and chowning it would fail on a host the user does not own.
    chown -R "$PUID:$PGID" /data 2>/dev/null || \
        echo "[WARN] could not chown /data to $PUID:$PGID - it may be owned by another user."
fi

# If akari.toml doesn't exist, create default config
if [ ! -f "$CONFIG_PATH" ]; then
    echo "Creating default akari.toml at $CONFIG_PATH..."
    cat <<EOF > "$CONFIG_PATH"
[input]
url = "https://www.nationstates.net/api/all"
workers = 2

[output.console]
enabled = false

[output.file]
enabled = true
path = "$AKARI_LOG"
EOF
fi

# Akari writes the file under [output.file]; Akiraka tails $AKARI_LOG. They have to
# agree, and when the config above is generated they do by construction - but the
# config is a host mount a user can edit, and then nothing links them.
#
# Both ways of getting it wrong fail silently, which is why they are checked rather
# than documented. Akiraka does not treat a missing input file as an error; it waits
# for it to appear, prints "Waiting for ... to appear" and tries again. So either
# akari writing to another path, or writing nothing at all, leaves Akiraka waiting
# on a file that never arrives - while the container reports Up and the store never
# grows. Nothing else here would notice.
if [ -f "$CONFIG_PATH" ]; then
    output_file_section=$(sed -n '/^\[output\.file\]/,/^\[/p' "$CONFIG_PATH" 2>/dev/null)

    configured_enabled=$(printf '%s\n' "$output_file_section" \
        | sed -n 's/^[[:space:]]*enabled[[:space:]]*=[[:space:]]*\([A-Za-z]*\).*/\1/p' | head -n 1)
    configured_log=$(printf '%s\n' "$output_file_section" \
        | sed -n 's/^[[:space:]]*path[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)

    if [ "$configured_enabled" = "false" ]; then
        echo "[ERROR] [output.file] is disabled in $CONFIG_PATH, so Akari writes nothing"
        echo "        for Akiraka to tail, and Akiraka would wait for ever. Set it to true."
        exit 1
    fi

    if [ -n "$configured_log" ] && [ "$configured_log" != "$AKARI_LOG" ]; then
        echo "[ERROR] $CONFIG_PATH writes to '$configured_log' but Akiraka tails '$AKARI_LOG'."
        echo "        Akiraka would wait for a file nothing writes, silently. Either set"
        echo "        AKARI_LOG='$configured_log', or change path under [output.file] to '$AKARI_LOG'."
        exit 1
    fi
fi

# Akari rotates its output file. Its own fallbacks are the unbounded ones - a
# 500-line segment, and no limit on how many are kept - so an unset maxfiles is
# why a log directory fills up. The baked config sets sane values; these
# overrides exist because that config is not mounted, so a host has no way to
# change them short of a rebuild (see docker-compose.yml).
#
# A segment has to be big enough that Akari cannot refill a fresh one inside
# Akiraka's 500ms poll. Rotation is noticed by the file shrinking, so a segment
# small enough to be replaced and refilled past the read offset between two polls
# has its start skipped. That needs a whole segment written in under half a
# second, which the floor below is far above at any plausible NationStates rate.
AKARI_THRESHOLD_MIN="${AKARI_THRESHOLD_MIN:-100}"

is_positive_int() {
    case "$1" in
        ''|*[!0-9]*) return 1 ;;
        *) [ "$1" -gt 0 ] ;;
    esac
}

# A malformed value is worse than an unset one. Akari fails the whole TOML parse
# on it and falls back to defaults, which disable file output - silently, because
# its logging is compiled out - so Akiraka would wait for a file nothing writes
# while the container reports healthy. Drop anything that is not a positive
# integer rather than write it.
if [ -n "$AKARI_THRESHOLD" ] && ! is_positive_int "$AKARI_THRESHOLD"; then
    echo "[rotation] AKARI_THRESHOLD='$AKARI_THRESHOLD' is not a positive integer; ignoring it."
    AKARI_THRESHOLD=""
fi
if [ -n "$AKARI_MAXFILES" ] && ! is_positive_int "$AKARI_MAXFILES"; then
    echo "[rotation] AKARI_MAXFILES='$AKARI_MAXFILES' is not a positive integer; ignoring it."
    AKARI_MAXFILES=""
fi

if [ -n "$AKARI_THRESHOLD" ] && [ "$AKARI_THRESHOLD" -lt "$AKARI_THRESHOLD_MIN" ]; then
    echo "[rotation] AKARI_THRESHOLD=$AKARI_THRESHOLD is below the minimum of $AKARI_THRESHOLD_MIN" \
         "(${AKARI_THRESHOLD_MIN}000 lines per segment); using $AKARI_THRESHOLD_MIN."
    AKARI_THRESHOLD="$AKARI_THRESHOLD_MIN"
fi

if [ -n "$AKARI_THRESHOLD" ] || [ -n "$AKARI_MAXFILES" ]; then
    awk -v th="$AKARI_THRESHOLD" -v mf="$AKARI_MAXFILES" '
        BEGIN { in_section = 0; saw_th = 0; saw_mf = 0 }
        /^\[/ {
            if (in_section) emit_missing()
            in_section = ($0 ~ /^\[output\.file\]/)
            if (in_section) { saw_th = 0; saw_mf = 0 }
        }
        in_section && /^[[:space:]]*threshold[[:space:]]*=/ {
            saw_th = 1
            if (th != "") print "threshold = " th
            next
        }
        in_section && /^[[:space:]]*maxfiles[[:space:]]*=/ {
            saw_mf = 1
            if (mf != "") print "maxfiles = " mf
            next
        }
        { print }
        END { if (in_section) emit_missing() }
        function emit_missing() {
            if (th != "" && !saw_th) print "threshold = " th
            if (mf != "" && !saw_mf) print "maxfiles = " mf
        }
    ' "$CONFIG_PATH" > "$CONFIG_PATH.rotation" \
        && cat "$CONFIG_PATH.rotation" > "$CONFIG_PATH" \
        && rm -f "$CONFIG_PATH.rotation"
    echo "[rotation] Akari log: threshold=${AKARI_THRESHOLD:-unchanged} maxfiles=${AKARI_MAXFILES:-unchanged}"
fi

echo "Starting Akari (SSE client)..."
if [ "$RUN_AS_ROOT" = "1" ]; then
    setpriv --reuid="$PUID" --regid="$PGID" --clear-groups \
        akari --config "$CONFIG_PATH" &
else
    akari --config "$CONFIG_PATH" &
fi
AKARI_PID=$!

  echo "Starting Akiraka enricher (tailing $AKARI_LOG -> store $EVENT_STORE)..."
  # The store is the record, and this process is its only writer. No enriched JSONL
  # is written; the query service reads the store, and endpoints cover the subsets
  # that used to need their own files.
  #
  # The cessation sidecar is still written to $CTE_LOG, for reading those events
  # straight off disk. The same rows are in the store, so an endpoint over those
  # categories serves them too.
  if [ "$RUN_AS_ROOT" = "1" ]; then
    setpriv --reuid="$PUID" --regid="$PGID" --clear-groups \
      node /app/dist/index.js --tail "$AKARI_LOG" --store "$EVENT_STORE" --cte-out "$CTE_LOG" &
  else
    node /app/dist/index.js --tail "$AKARI_LOG" --store "$EVENT_STORE" --cte-out "$CTE_LOG" &
  fi
  AKIRAKA_PID=$!

# The query service reads the store over HTTP, in this container but as its own
# process: it holds a read-only connection and a crash in it must not stop the
# collector. It restarts on its own because on a first start it can come up
# before the enricher has created the store, which the query treats as fatal.
run_query() {
    if [ "$RUN_AS_ROOT" = "1" ]; then
        setpriv --reuid="$PUID" --regid="$PGID" --clear-groups \
            node /app/dist/query.js --config "$QUERY_CONFIG"
    else
        node /app/dist/query.js --config "$QUERY_CONFIG"
    fi
}
echo "Starting query service (reading $EVENT_STORE)..."
(
    while true; do
        run_query || true
        echo "[query] exited; restarting in 2s"
        sleep 2
    done
) &
QUERY_PID=$!

cleanup() {
    echo "Shutting down..."
    kill -TERM "$AKARI_PID" 2>/dev/null || true
    kill -TERM "$AKIRAKA_PID" 2>/dev/null || true
    kill -TERM "$QUERY_PID" 2>/dev/null || true
    wait "$AKARI_PID" "$AKIRAKA_PID" 2>/dev/null || true
    exit 0
}

trap cleanup INT TERM

# Wait for either process to exit
wait -n "$AKARI_PID" "$AKIRAKA_PID"
