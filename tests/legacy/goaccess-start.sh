#!/bin/sh
set -eu

state=${GOACCESS_STATE:-/state}
runtime=${GOACCESS_RUNTIME:-/runtime}
config=${GOACCESS_CONFIG:-/config/goaccess.conf}
child=""

health() {
    test -s "$runtime/goaccess.pid" || return 1
    kill -0 "$(cat "$runtime/goaccess.pid")" 2>/dev/null || return 1
    awk '$2 == "0100007F:1ED2" && $4 == "0A" { found=1 } END { exit !found }' /proc/net/tcp || return 1
    test -s "$state/www/current/index.html"
}

if [ "${1:-}" = --health ]; then
    health
    exit $?
fi

stop_child() {
    [ -n "$child" ] || return 0
    kill -INT "$child" 2>/dev/null || true
    deadline=$(($(date +%s) + 90))
    while kill -0 "$child" 2>/dev/null; do
        if [ "$(date +%s)" -ge "$deadline" ]; then
            kill -TERM "$child" 2>/dev/null || true
            sleep 5
            kill -KILL "$child" 2>/dev/null || true
            break
        fi
        sleep 1
    done
    wait "$child" || true
    child=""
    rm -f "$runtime/goaccess.pid"
}

finish() {
    trap - INT TERM
    stop_child
    exit 0
}
trap finish INT TERM

mkdir -p "$state/db" "$state/www" "$state/logs"
generation=""
while :; do
    desired=$(cat "$runtime/generation" 2>/dev/null || true)
    if [ -z "$desired" ]; then
        sleep 1
        continue
    fi
    case "$desired" in *[!a-f0-9]*) exit 1 ;; esac
    if [ "$desired" != "$generation" ]; then
        stop_child
        generation=$desired
        run_id=$(cat /proc/sys/kernel/random/uuid)
        mkdir -p "$state/db/$run_id" "$state/www/$run_id"
        goaccess --no-global-config --config-file="$config" \
            --jobs="${GOACCESS_JOBS:-6}" --chunk-size="${GOACCESS_CHUNK_SIZE:-8192}" \
            --db-path="$state/db/$run_id" --pid-file="$runtime/goaccess-child.pid" \
            --unknowns-log="$state/logs/unknowns.txt" \
            --invalid-requests="$state/logs/invalid-requests.log" \
            --output="$state/www/$run_id/index.html" \
            "$runtime/generations/$generation/access.log" &
        child=$!
        printf '%s\n' "$child" > "$runtime/goaccess.pid"
        printf '%s\n' "$generation" > "$runtime/consumer.tmp"
        mv -f "$runtime/consumer.tmp" "$runtime/consumer"
        published=false
    fi
    if ! kill -0 "$child" 2>/dev/null; then
        wait "$child" || true
        exit 1
    fi
    if [ "$published" = false ] && [ -f "$state/www/$run_id/index.html" ] && \
        tail -c 64 "$state/www/$run_id/index.html" | grep -q '</html>'; then
        previous=$(readlink "$state/www/current" || true)
        rm -f "$state/www/current.tmp"
        ln -s "$run_id" "$state/www/current.tmp"
        mv -Tf "$state/www/current.tmp" "$state/www/current"
        published=true
        for directory in "$state/db/"* "$state/www/"*; do
            [ -d "$directory" ] && [ ! -L "$directory" ] || continue
            name=$(basename "$directory")
            if [ "$name" != "$run_id" ] && [ "$name" != "$previous" ]; then
                rm -rf "$directory"
            fi
        done
    fi
    for log in "$state/logs/unknowns.txt" "$state/logs/invalid-requests.log"; do
        if [ -f "$log" ] && [ "$(wc -c < "$log")" -gt "${GOACCESS_LOG_MAX_BYTES:-8388608}" ]; then
            : > "$log"
        fi
    done
    sleep 1
done
