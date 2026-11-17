#!/usr/bin/env bash
# Interop check: a stock BPv7 node (µD3TN) carries a cosmosd bundle.
#
#   cosmosd dev-bundle --out in.bundle
#   in.bundle --MTCP--> µD3TN --(holds it until a contact opens)--MTCP--> receiver
#   cosmosd dev-bundle --check out.bundle
#
# µD3TN gets a contact to the bundle's destination node that opens a few
# seconds in the future, so it must store the bundle before it forwards it.
# The receiver is a small MTCP listener in this script. The forwarded bytes
# must decode with cosmosd's own decoder, which needs block 192 intact, and
# must keep the publisher, content ID, kind, sequence and payload hash.
#
# µD3TN has no TCPCLv4, so the convergence layer is MTCP. µD3TN is cloned at
# a pinned release into a fresh temp dir, built with its documented
# `make posix` target, and removed again at the end. Set UD3TN_BIN to a
# prebuilt ud3tn binary to skip the clone and build.
#
# Prints INTEROP PASS and exits 0 only on a verified receipt. Otherwise
# prints INTEROP FAIL: <reason> and exits 1. Every run uses fresh temp dirs
# and fresh loopback ports, and kills what it starts.
#
# Needs git, make, a C compiler and the SQLite headers (macOS has them,
# Debian needs libsqlite3-dev), python3 and cargo.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UD3TN_REPO="https://gitlab.com/d3tn/ud3tn.git"
UD3TN_TAG="v0.15.2"
UD3TN_COMMIT="6b18b267ce79ac0c8d8f7dbce5389f2acef0ca40"
NODE_ID="dtn://interop-node/"
# Seconds until the contact to the destination opens. µD3TN stores the
# bundle for this long.
HOLD_SECS=3

say() { echo "interop: $*"; }
fail() { echo "INTEROP FAIL: $*"; exit 1; }

command -v python3 >/dev/null || fail "python3 not found"
# cargo may be installed but not on PATH (Homebrew's keg-only rustup, rustup.rs).
if ! command -v cargo >/dev/null; then
    for d in /opt/homebrew/opt/rustup/bin "$HOME/.cargo/bin"; do
        if [ -x "$d/cargo" ]; then export PATH="$d:$PATH"; break; fi
    done
fi
command -v cargo >/dev/null || fail "cargo not found"

# === Cleanup ===

# The run dir holds the AAP unix sockets. Their paths must stay under the
# 104 byte limit, so it comes from plain mktemp -d.
RUN="$(mktemp -d)"
SRC_DIR=""
PIDS=()
cleanup() {
    for pid in "${PIDS[@]+"${PIDS[@]}"}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${PIDS[@]+"${PIDS[@]}"}"; do
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$RUN"
    if [ -n "$SRC_DIR" ]; then rm -rf "$SRC_DIR"; fi
}
trap cleanup EXIT
trap 'fail "interrupted"' INT TERM
trap 'fail "unexpected error at line $LINENO"' ERR

# On failure, show the end of µD3TN's log before the run dir goes.
fail_with_log() {
    if [ -f "$RUN/ud3tn.log" ]; then
        echo "--- last lines of the µD3TN log ---"
        tail -n 20 "$RUN/ud3tn.log"
        echo "---"
    fi
    fail "$*"
}

# === Helper ===

# MTCP framing is one CBOR byte string per bundle. AAP v1 is µD3TN's
# simple local application protocol, used here only to send the contact
# config. Runs with -I from the run dir, never from the µD3TN tree.
HELPER=$(cat <<'PY'
import os, socket, struct, sys

def recv_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed early")
        buf += chunk
    return buf

def bstr_header(n):
    if n < 24:
        return bytes([0x40 | n])
    for code, size in ((24, 1), (25, 2), (26, 4), (27, 8)):
        if n < 1 << (8 * size):
            return bytes([0x40 | code]) + n.to_bytes(size, "big")

def free_ports(count):
    socks = [socket.socket() for _ in range(int(count))]
    for s in socks:
        s.bind(("127.0.0.1", 0))
    print(" ".join(str(s.getsockname()[1]) for s in socks))

def mtcp_send(port, path):
    data = open(path, "rb").read()
    with socket.create_connection(("127.0.0.1", int(port)), timeout=10) as s:
        s.sendall(bstr_header(len(data)) + data)

def mtcp_recv(port, out, ready, timeout):
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", int(port)))
    srv.listen(1)
    srv.settimeout(float(timeout))
    open(ready, "w").close()
    conn, _ = srv.accept()
    conn.settimeout(float(timeout))
    head = recv_exact(conn, 1)[0]
    if head >> 5 != 2:
        raise ValueError("MTCP frame is not a CBOR byte string")
    info = head & 0x1F
    if info < 24:
        n = info
    else:
        n = int.from_bytes(recv_exact(conn, {24: 1, 25: 2, 26: 4, 27: 8}[info]), "big")
    data = recv_exact(conn, n)
    open(out + ".part", "wb").write(data)
    os.rename(out + ".part", out)

def aap_send(sock_path, dest, payload):
    s = socket.socket(socket.AF_UNIX)
    s.settimeout(10)
    s.connect(sock_path)
    if recv_exact(s, 1)[0] != 0x17:
        raise ValueError("no AAP WELCOME")
    recv_exact(s, struct.unpack(">H", recv_exact(s, 2))[0])
    agent = b"interop-config"
    s.sendall(b"\x12" + struct.pack(">H", len(agent)) + agent)
    if recv_exact(s, 1)[0] != 0x10:
        raise ValueError("AAP REGISTER refused")
    d, p = dest.encode(), payload.encode()
    s.sendall(b"\x13" + struct.pack(">H", len(d)) + d + struct.pack(">Q", len(p)) + p)
    if recv_exact(s, 1)[0] != 0x15:
        raise ValueError("AAP SENDBUNDLE not confirmed")
    recv_exact(s, 8)
    s.close()

cmd, args = sys.argv[1], sys.argv[2:]
{"ports": free_ports, "send": mtcp_send, "recv": mtcp_recv, "aap": aap_send}[cmd](*args)
PY
)
helper() { (cd "$RUN" && exec python3 -I -c "$HELPER" "$@"); }

# Wait up to $1 seconds for command "${@:2}" to succeed.
wait_for() {
    local secs=$1; shift
    local i
    for ((i = 0; i < secs * 10; i++)); do
        if "$@"; then return 0; fi
        sleep 0.1
    done
    return 1
}

# === Build ===

say "building cosmosd"
(cd "$ROOT" && cargo build --quiet) >"$RUN/cargo.log" 2>&1 \
    || { tail -n 20 "$RUN/cargo.log"; fail "cargo build failed"; }
COSMOSD="$ROOT/target/debug/cosmosd"

if [ -n "${UD3TN_BIN:-}" ]; then
    [ -x "$UD3TN_BIN" ] || fail "UD3TN_BIN=$UD3TN_BIN is not an executable"
    UD3TN="$UD3TN_BIN"
    UD3TN_VERSION="prebuilt binary $UD3TN_BIN"
else
    command -v git >/dev/null || fail "git not found"
    command -v make >/dev/null || fail "make not found"
    SRC_DIR="$(mktemp -d)"
    say "cloning µD3TN $UD3TN_TAG into $SRC_DIR"
    git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$UD3TN_TAG" \
        --recurse-submodules --shallow-submodules "$UD3TN_REPO" "$SRC_DIR/ud3tn" >"$RUN/clone.log" 2>&1 \
        || { tail -n 20 "$RUN/clone.log"; fail "could not clone µD3TN"; }
    got="$(git -C "$SRC_DIR/ud3tn" rev-parse HEAD)"
    [ "$got" = "$UD3TN_COMMIT" ] || fail "µD3TN $UD3TN_TAG is commit $got, expected $UD3TN_COMMIT"
    say "building µD3TN (make posix, without the optional JSON config support)"
    make -C "$SRC_DIR/ud3tn" posix -j8 DISABLE_JSON=1 >"$RUN/build.log" 2>&1 \
        || { tail -n 20 "$RUN/build.log"; fail "µD3TN build failed"; }
    UD3TN="$SRC_DIR/ud3tn/build/posix/ud3tn"
    UD3TN_VERSION="$UD3TN_TAG (commit $UD3TN_COMMIT)"
fi

# === Sample bundle ===

"$COSMOSD" dev-bundle --out "$RUN/in.bundle" >/dev/null || fail "cosmosd dev-bundle failed"
"$COSMOSD" dev-bundle --check "$RUN/in.bundle" >"$RUN/in.txt" || fail "cosmosd cannot decode its own sample"
PUBLISHER="$(sed -n 's/^publisher=//p' "$RUN/in.txt")"
CONTENT_ID="$(sed -n 's/^content_id=//p' "$RUN/in.txt")"
DEST="dtn://$PUBLISHER/$CONTENT_ID"
say "sample bundle: $(wc -c <"$RUN/in.bundle" | tr -d ' ') bytes to $DEST"

# === Run µD3TN ===

read -r IN_PORT OUT_PORT <<<"$(helper ports 2)"
"$UD3TN" -e "$NODE_ID" -c "sqlite:file::memory:?cache=shared;mtcp:127.0.0.1,$IN_PORT" \
    -s "$RUN/aap.sock" -S "$RUN/aap2.sock" -L 4 >"$RUN/ud3tn.log" 2>&1 &
PIDS+=($!)
wait_for 10 grep -q "CLA mtcp is now listening" "$RUN/ud3tn.log" \
    || fail_with_log "µD3TN did not start its MTCP listener"
wait_for 10 test -S "$RUN/aap.sock" || fail_with_log "µD3TN did not open its AAP socket"
say "µD3TN $UD3TN_VERSION is up as $NODE_ID, MTCP in on 127.0.0.1:$IN_PORT"

# The receiver stands in for the next node. It takes one bundle.
helper recv "$OUT_PORT" "$RUN/out.bundle" "$RUN/recv.ready" 30 2>"$RUN/recv.err" &
PIDS+=($!)
wait_for 10 test -f "$RUN/recv.ready" || fail "MTCP receiver did not start"

# Contact to the destination node, opening HOLD_SECS from now (DTN time is
# seconds since 2000-01-01). Old µD3TN config format, see its
# doc/contacts_data_format.md.
now=$(($(date +%s) - 946684800))
config="1(dtn://$PUBLISHER/):(mtcp:127.0.0.1:$OUT_PORT)::[{$((now + HOLD_SECS)),$((now + 600)),100000}];"
helper aap "$RUN/aap.sock" "${NODE_ID}config" "$config" || fail_with_log "could not configure the contact"
say "contact to dtn://$PUBLISHER/ opens in $HOLD_SECS s"

helper send "$IN_PORT" "$RUN/in.bundle" || fail_with_log "could not send the bundle to µD3TN"
say "sent the sample bundle to µD3TN over MTCP"

# === Check ===

wait_for 10 grep -q "Received new bundle .* to \"$DEST\"" "$RUN/ud3tn.log" \
    || fail_with_log "µD3TN did not report receiving the bundle"
say "µD3TN log: $(grep -o "Received new bundle .* to \"$DEST\" via CLA mtcp" "$RUN/ud3tn.log" | head -n 1)"

wait_for 30 test -f "$RUN/out.bundle" || fail_with_log "µD3TN did not forward the bundle"
grep -q "TX of bundle .* to \"dtn://$PUBLISHER/\" via \"mtcp:127.0.0.1:$OUT_PORT\" OK" "$RUN/ud3tn.log" \
    || fail_with_log "µD3TN did not report a successful forward"
say "µD3TN log: $(grep -o "TX of bundle .* OK" "$RUN/ud3tn.log" | head -n 1)"
say "receiver got $(wc -c <"$RUN/out.bundle" | tr -d ' ') bytes back"

"$COSMOSD" dev-bundle --check "$RUN/out.bundle" >"$RUN/out.txt" 2>"$RUN/check.err" \
    || fail "cosmosd cannot decode the bundle µD3TN forwarded: $(tr '\n' ' ' <"$RUN/check.err")"
# The first five lines are the identity: publisher, content ID, kind,
# sequence and payload hash.
if ! diff <(head -n 5 "$RUN/in.txt") <(head -n 5 "$RUN/out.txt") >/dev/null; then
    fail "the forwarded bundle changed identity: $(diff <(head -n 5 "$RUN/in.txt") <(head -n 5 "$RUN/out.txt") | tr '\n' ' ')"
fi
# A real store shows in the age block: µD3TN adds the time it held the bundle.
age_in="$(sed -n 's/^age_ms=//p' "$RUN/in.txt")"
age_out="$(sed -n 's/^age_ms=//p' "$RUN/out.txt")"
[ "$age_out" -ge $((age_in + 1000)) ] \
    || fail "bundle age went from $age_in ms to $age_out ms, so µD3TN did not hold it for the contact"
say "decoded by cosmosd, before -> after:"
paste -d' ' <(sed 's/^/  /' "$RUN/in.txt") <(sed 's/^[^=]*=/-> /' "$RUN/out.txt")

echo "INTEROP PASS: µD3TN $UD3TN_VERSION received, stored and forwarded a cosmosd bundle with block 192 intact"
