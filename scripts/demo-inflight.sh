#!/usr/bin/env bash
# In-flight live demo: ground -> simulated satellite link -> gateway -> cabin.
#
#   ffmpeg test source --udp--> ground (ingest --live)
#     ground relay <-- linksim (3 Mbps, 300 ms each way, cut twice) <-- gateway
#     gateway <-- mDNS --> cabin <-- playcheck (headless player)
#
# The ground node requires a token; the gateway holds it (--peer addr@token)
# and the cabin, which dials only the gateway, has none.
#
# The ground node signs what it ingests. It gets a fresh publisher key and the
# gateway and cabin pair it. Every key dir is a temp dir under the run dir.
#
# The link is cut for 30 s, then for 5 minutes. The player must not stall
# through the short cut, must stall in the long one, and must end up about
# two minutes (--delay) behind live. Prints DEMO PASS or DEMO FAIL: <reason>.
#
# Takes about 17 minutes. Everything is written under a fresh `mktemp -d`.
#
# Environment:
#   DEMO_SCALE=0.5     scale the delay, timeline and checks (quick trial runs;
#                      segments stay 10 s, so scaled runs are not expected to pass)
#   DEMO_CABIN_PEER=1  fallback if mDNS can't work on this machine: the cabin
#                      gets an explicit --peer to the gateway instead
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$(mktemp -d)"
SCALE="${DEMO_SCALE:-1}"
SRC_UDP="udp://127.0.0.1:5001" # 5000 is held by AirPlay on macOS
export RUST_LOG="${RUST_LOG:-cosmosd::peer=debug}"

say() { echo "demo: $*"; }
fail() { echo "DEMO FAIL: $*"; exit 1; }
# Scale a number of seconds by DEMO_SCALE, rounded.
sc() { awk -v s="$SCALE" -v x="$1" 'BEGIN { printf "%d", x * s + 0.5 }'; }

DELAY=$(sc 120)
CUT1=$(sc 240); RESTORE1=$(sc 270); CUT2=$(sc 420); RESTORE2=$(sc 720)
DURATION=$(sc 960)
# The 30 s cut can only show once the 120 s buffer drains (by about 400 s);
# the 5 min cut stalls from about 530 s. So: no stall in [240, 500).
QUIET_FROM=$(sc 240); QUIET_TO=$(sc 500)
STALL_FROM=$(sc 420); STALL_TO=$(sc 780)
LAG_MIN=$(sc 110); LAG_MAX=$(sc 180)

command -v ffmpeg >/dev/null || fail "ffmpeg not found (brew install ffmpeg)"
# cargo may be installed but not on PATH (Homebrew's keg-only rustup, rustup.rs).
if ! command -v cargo >/dev/null; then
    for d in /opt/homebrew/opt/rustup/bin "$HOME/.cargo/bin"; do
        if [ -x "$d/cargo" ]; then export PATH="$d:$PATH"; break; fi
    done
fi
command -v cargo >/dev/null || fail "cargo not found (install Rust: brew install rustup)"

# === Cleanup ===

PIDS=()
INGEST_PID=""
cleanup() {
    trap - EXIT INT TERM
    # ingest stops its ffmpeg child and removes its tempdir on SIGINT or SIGTERM.
    if [ -n "$INGEST_PID" ] && kill -0 "$INGEST_PID" 2>/dev/null; then
        kill -INT "$INGEST_PID" 2>/dev/null || true
        for _ in 1 2 3 4 5; do kill -0 "$INGEST_PID" 2>/dev/null || break; sleep 1; done
    fi
    # bash 3.2 with `set -u` treats an empty array as unbound: guard it for
    # failures before anything was started.
    for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    say "logs in $DIR"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# Wait up to 30 s for an HTTP endpoint to answer.
wait_http() {
    for _ in $(seq 60); do
        curl -fs "$1" >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    fail "$1 did not come up (see $DIR)"
}

# === 1. Build ===

say "building"
(cd "$ROOT" && cargo build --release --bins --examples)
BIN="$ROOT/target/release"
COSMOSD="$BIN/cosmosd"

# === 2. Live test source ===

VF=()
if ffmpeg -hide_banner -filters 2>/dev/null | grep -q drawtext; then
    VF=(-vf "drawtext=text='%{localtime\:%T}':fontsize=96:fontcolor=white:box=1:boxcolor=black@0.6:x=40:y=40")
else
    say "this ffmpeg has no drawtext filter: the test pattern runs without the burned-in clock"
fi
ffmpeg -hide_banner -loglevel warning -nostdin \
    -re -f lavfi -i "testsrc2=size=1280x720:rate=30" \
    -re -f lavfi -i "sine=frequency=440:sample_rate=48000" \
    ${VF[@]+"${VF[@]}"} \
    -c:v libx264 -preset veryfast -tune zerolatency -g 60 -b:v 3000k \
    -c:a aac -b:a 128k \
    -f mpegts "$SRC_UDP?pkt_size=1316" >"$DIR/source.log" 2>&1 &
PIDS+=($!)

# === 3. Keys: the ground node publishes, the others pair it ===

for node in ground gateway cabin; do mkdir -p "$DIR/keys/$node"; done
"$COSMOSD" keys --key-dir "$DIR/keys/ground" init
PAIRING="$("$COSMOSD" keys --key-dir "$DIR/keys/ground" show | head -n 1)"
for node in gateway cabin; do
    "$COSMOSD" keys --key-dir "$DIR/keys/$node" pair "$PAIRING"
done

# === 4. Ground node + live ingest ===

"$COSMOSD" serve --port 17777 --relay-port 17778 --node-id ground --no-discovery \
    --relay-token demo-secret --key-dir "$DIR/keys/ground" \
    --delay "$DELAY" --store "$DIR/ground" >"$DIR/ground.log" 2>&1 &
PIDS+=($!)
wait_http http://127.0.0.1:17777/health
"$COSMOSD" ingest --live "$SRC_UDP" --content-id demo --daemon http://127.0.0.1:17777 \
    >"$DIR/ingest.log" 2>&1 &
INGEST_PID=$!

# === 5. Simulated satellite link ===

"$BIN/examples/linksim" --listen 127.0.0.1:18778 --upstream 127.0.0.1:17778 \
    --rate-kbps 3000 --latency-ms 300 >"$DIR/linksim.log" 2>&1 &
LINKSIM_PID=$!
PIDS+=($LINKSIM_PID)

# === 6. Gateway (dials the ground through the link; mDNS on) ===

"$COSMOSD" serve --port 27777 --relay-port 27778 --node-id gateway --peer 127.0.0.1:18778@demo-secret \
    --key-dir "$DIR/keys/gateway" --delay "$DELAY" --store "$DIR/gateway" >"$DIR/gateway.log" 2>&1 &
PIDS+=($!)

# === 7. Cabin (finds the gateway by mDNS, no --peer) ===

CABIN_PEER=()
if [ -n "${DEMO_CABIN_PEER:-}" ]; then
    say "DEMO_CABIN_PEER set: cabin uses --peer 127.0.0.1:27778 instead of mDNS"
    CABIN_PEER=(--peer 127.0.0.1:27778)
fi
"$COSMOSD" serve --port 37777 --relay-port 37778 --node-id cabin --bind 0.0.0.0 \
    ${CABIN_PEER[@]+"${CABIN_PEER[@]}"} --key-dir "$DIR/keys/cabin" \
    --delay "$DELAY" --store "$DIR/cabin" >"$DIR/cabin.log" 2>&1 &
PIDS+=($!)
wait_http http://127.0.0.1:27777/health
wait_http http://127.0.0.1:37777/health

# === 8. Player ===

say "logs and play.log in $DIR"
say "watch: ffplay http://127.0.0.1:37777/play/demo (or VLC / Safari); playback starts after ${DELAY}s"
say "timeline: cut ${CUT1}s, restore ${RESTORE1}s, cut ${CUT2}s, restore ${RESTORE2}s, end ${DURATION}s"
"$BIN/examples/playcheck" --url http://127.0.0.1:37777/play/demo --duration-secs "$DURATION" \
    > >(tee "$DIR/play.log") 2>&1 &
PLAY_PID=$!
PIDS+=($PLAY_PID)
SECONDS=0

# === 9. Timeline (seconds since playcheck started) ===

at() { while [ "$SECONDS" -lt "$1" ]; do sleep 1; done; }

at 30
say "cabin /peers (the gateway should be listed, learned by mDNS):"
curl -s http://127.0.0.1:37777/peers || true
echo

at "$CUT1";     say "t=$SECONDS cut the link";     kill -USR1 "$LINKSIM_PID"
at "$RESTORE1"; say "t=$SECONDS restore the link"; kill -USR2 "$LINKSIM_PID"
at "$CUT2";     say "t=$SECONDS cut the link";     kill -USR1 "$LINKSIM_PID"
at "$RESTORE2"; say "t=$SECONDS restore the link"; kill -USR2 "$LINKSIM_PID"

PLAY_STATUS=0
wait "$PLAY_PID" || PLAY_STATUS=$?
sleep 1 # let tee flush

# === 10. Checks ===

LOG="$DIR/play.log"
[ "$PLAY_STATUS" -eq 0 ] || fail "playcheck exited with status $PLAY_STATUS"
grep -q '^summary ' "$LOG" || fail "no summary line in play.log"
# Anchored so `stall start seq=` can't count as playback starting.
grep -qE '^t=[0-9.]+ start seq=' "$LOG" || fail "playback never started"

# Event lines: t=<secs> <event...> seq=<n> lag=<secs>
# Count stall starts with lo <= t <= hi, or lo <= t < hi when $3 is "open".
stall_starts() {
    awk -v lo="$1" -v hi="$2" -v open="${3:-}" '$2 == "stall" && $3 == "start" {
        t = substr($1, 3) + 0
        if (t >= lo && (open == "open" ? t < hi : t <= hi)) n++ } END { print n + 0 }' "$LOG"
}
QUIET=$(stall_starts "$QUIET_FROM" "$QUIET_TO" open)
STALLED=$(stall_starts "$STALL_FROM" "$STALL_TO")
FINAL_LAG=$(awk '/^summary / { for (i = 2; i <= NF; i++) if ($i ~ /^final_lag=/) print substr($i, 11) }' "$LOG")

[ "$QUIET" -eq 0 ] || fail "$QUIET stall(s) started in [${QUIET_FROM}s, ${QUIET_TO}s) (the 30 s cut)"
[ "$STALLED" -ge 1 ] || fail "no stall started between ${STALL_FROM}s and ${STALL_TO}s (the 5 min cut)"
awk -v l="$FINAL_LAG" -v lo="$LAG_MIN" -v hi="$LAG_MAX" 'BEGIN { exit !(l >= lo && l <= hi) }' \
    || fail "final_lag=${FINAL_LAG}s, want ${LAG_MIN}..${LAG_MAX}s"
echo "DEMO PASS"
