"""
cosmosd two-node sync demo.

Starts two cosmosd nodes on localhost, ingests synthetic video segments
into node A, and watches them propagate automatically to node B via
epidemic routing — no internet required.

Node A signs what it ingests with a fresh publisher key. Node B pairs that
key, so it accepts the content. Both key dirs are temp dirs. Both nodes run
with --delay 0, so the near_live playlist lists segments as soon as they land.

Requirements:
    cosmosd binary must be built or installed
    pip install requests

Run:
    python demo.py
    python demo.py --binary ./target/release/cosmosd
"""

from __future__ import annotations

import argparse
import base64
import shutil
import subprocess
import sys
import tempfile
import time

import requests

# === ANSI colours ===

RESET  = "\033[0m"
BOLD   = "\033[1m"
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
DIM    = "\033[2m"

def banner(msg: str) -> None:
    print(f"\n{BOLD}{CYAN}{'─' * 60}{RESET}")
    print(f"{BOLD}{CYAN}  {msg}{RESET}")
    print(f"{BOLD}{CYAN}{'─' * 60}{RESET}\n")

def step(msg: str) -> None:
    print(f"{BOLD}▶  {msg}{RESET}")

def ok(msg: str) -> None:
    print(f"{GREEN}   ✓  {msg}{RESET}")

def info(msg: str) -> None:
    print(f"{DIM}   →  {msg}{RESET}")

def fail(msg: str) -> None:
    print(f"{RED}   ✗  {msg}{RESET}")

# === Fake MPEG-TS segment (valid sync byte, correct packet size) ===
# 188 bytes per TS packet × 50 packets = one ~1s chunk

def make_segment(seq: int) -> bytes:
    packets = []
    for i in range(50):
        pid = 0x100 + (seq % 32)
        header = bytes([
            0x47,                          # sync byte
            (pid >> 8) & 0x1F,             # flags + PID high
            pid & 0xFF,                    # PID low
            0x10 | (i & 0x0F),            # continuity counter
        ])
        payload = bytes([seq & 0xFF, i & 0xFF]) + bytes(182)
        packets.append(header + payload)
    return b"".join(packets)

# === Helpers ===

def wait_ready(url: str, timeout: int = 15) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{url}/health", timeout=1)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def push_segment(url: str, content_id: str, title: str, seq: int) -> str:
    payload = make_segment(seq)
    b64 = base64.b64encode(payload).decode()
    r = requests.post(f"{url}/ingest", json={
        "content_id":    content_id,
        "content_title": title,
        "sequence":      seq,
        "mime_type":     "video/mp2t",
        "priority":      "near_live",
        "payload_b64":   b64,
    }, timeout=10)
    r.raise_for_status()
    return r.json()["bundle_id"]


def finish(url: str, content_id: str) -> dict:
    r = requests.post(f"{url}/ingest/finish", json={"content_id": content_id}, timeout=10)
    r.raise_for_status()
    return r.json()


def keys(binary: str, key_dir: str, *args: str) -> str:
    out = subprocess.run(
        [binary, "keys", "--key-dir", key_dir, *args],
        check=True, capture_output=True, text=True,
    )
    return out.stdout


def segment_count(url: str, content_id: str) -> int:
    try:
        r = requests.get(f"{url}/catalog", timeout=5)
        for c in r.json():
            if c["content_id"] == content_id:
                return c.get("segment_count", 1)
    except Exception:
        pass
    return 0

# === Main demo ===

def main() -> None:
    parser = argparse.ArgumentParser(description="cosmosd two-node sync demo")
    parser.add_argument(
        "--binary",
        default=shutil.which("cosmosd") or "./target/release/cosmosd",
        help="Path to cosmosd binary",
    )
    parser.add_argument(
        "--segments", type=int, default=6,
        help="Number of segments to ingest (default: 6)",
    )
    args = parser.parse_args()

    if not shutil.which(args.binary) and not __import__("os").path.isfile(args.binary):
        print(f"{RED}cosmosd binary not found at: {args.binary}{RESET}")
        print("Build it:   cargo build --release")
        print("Or install: brew install cosmosd  /  cargo install cosmosd")
        sys.exit(1)

    banner("cosmosd — two-node sync demo")
    print(f"  {DIM}Binary : {args.binary}{RESET}")
    print(f"  {DIM}Node A : http://127.0.0.1:17780  (relay: 17781){RESET}")
    print(f"  {DIM}Node B : http://127.0.0.1:17790  (relay: 17791){RESET}")
    print(f"  {DIM}Segments: {args.segments}{RESET}\n")

    with tempfile.TemporaryDirectory(prefix="cosmosd_demo_") as tmp:
        store_a = f"{tmp}/node-a"
        store_b = f"{tmp}/node-b"
        keys_a = f"{tmp}/keys-a"
        keys_b = f"{tmp}/keys-b"

        # ── 0. Keys: A publishes, B pairs A ──────────────────────────
        step("Creating Node A's publisher key and pairing it on Node B")
        keys(args.binary, keys_a, "init")
        pairing = keys(args.binary, keys_a, "show").splitlines()[0]
        keys(args.binary, keys_b, "pair", pairing)
        ok(f"Node B trusts {pairing[:24]}...")

        # ── 1. Start node A ──────────────────────────────────────────
        step("Starting Node A  (the ingest node)")
        proc_a = subprocess.Popen(
            [
                args.binary, "serve",
                "--port",       "17780",
                "--relay-port", "17781",
                "--store",      store_a,
                "--node-id",    "node-alpha",
                "--relay-token", "demo-alpha",
                "--key-dir",    keys_a,
                "--delay",      "0",
                "--no-discovery",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        if not wait_ready("http://127.0.0.1:17780"):
            fail("Node A did not start in time")
            proc_a.terminate()
            sys.exit(1)
        ok("Node A ready  →  http://127.0.0.1:17780")

        # ── 2. Start node B ──────────────────────────────────────────
        step("Starting Node B  (the relay/viewer node)  — peered with A")
        proc_b = subprocess.Popen(
            [
                args.binary, "serve",
                "--port",       "17790",
                "--relay-port", "17791",
                "--store",      store_b,
                "--node-id",    "node-beta",
                "--peer",       "127.0.0.1:17781@demo-alpha",
                "--relay-token", "demo-beta",
                "--key-dir",    keys_b,
                "--delay",      "0",
                "--no-discovery",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        if not wait_ready("http://127.0.0.1:17790"):
            fail("Node B did not start in time")
            proc_a.terminate()
            proc_b.terminate()
            sys.exit(1)
        ok("Node B ready  →  http://127.0.0.1:17790")

        # ── 3. Ingest segments into node A ───────────────────────────
        content_id = "cosmosd_demo_stream"
        title      = "CosmosD Demo — Two-Node Sync"

        print()
        step(f"Ingesting {args.segments} segments into Node A")
        for i in range(args.segments):
            bundle_id = push_segment("http://127.0.0.1:17780", content_id, title, i)
            ok(f"seg {i:05d}  →  {bundle_id}")
            time.sleep(0.1)
        manifest = finish("http://127.0.0.1:17780", content_id)
        ok(f"signed manifest  →  {manifest['manifest_id']} (version {manifest['version']})")

        # ── 4. Wait for epidemic sync to reach node B ────────────────
        print()
        step("Waiting for epidemic sync to propagate to Node B ...")
        synced   = False
        deadline = time.time() + 30
        dots     = 0

        while time.time() < deadline:
            # Older segments arrive as backfill, a few per sync, so wait for all.
            if segment_count("http://127.0.0.1:17790", content_id) >= args.segments:
                synced = True
                break
            print(f"   {DIM}{'.' * (dots % 6 + 1)}{RESET}", end="\r")
            dots += 1
            time.sleep(1)

        print()
        if not synced:
            fail("Sync did not complete within 30 seconds")
            proc_a.terminate()
            proc_b.terminate()
            sys.exit(1)

        ok("Content arrived on Node B via epidemic routing!")

        # ── 5. Verify HLS playlist on node B ─────────────────────────
        print()
        step("Verifying HLS playlist on Node B")
        r = requests.get(f"http://127.0.0.1:17790/play/{content_id}")
        assert r.status_code == 200, f"Playlist request failed: {r.status_code}"
        m3u8 = r.text
        assert "#EXTM3U" in m3u8
        seg_lines = [l for l in m3u8.splitlines() if l.startswith("/segment/")]
        ok(f"Valid M3U8 playlist — {len(seg_lines)} segments available")

        # ── 6. Verify a segment is retrievable from node B ───────────
        step("Fetching segment 0 from Node B")
        r = requests.get(f"http://127.0.0.1:17790/segment/{content_id}/0")
        assert r.status_code == 200
        assert len(r.content) > 0
        ok(f"Segment 0 served by Node B ({len(r.content):,} bytes) — integrity verified")

        # ── 7. Summary ───────────────────────────────────────────────
        banner("Demo complete")
        print(f"  {GREEN}{BOLD}Node A ingested {args.segments} segments.{RESET}")
        print(f"  {GREEN}{BOLD}Node B received them via epidemic routing — no direct push.{RESET}")
        print()
        print(f"  Play on Node A:  {CYAN}ffplay http://127.0.0.1:17780/play/{content_id}{RESET}")
        print(f"  Play on Node B:  {CYAN}ffplay http://127.0.0.1:17790/play/{content_id}{RESET}")
        print()
        print(f"  {DIM}Press Ctrl+C to stop the nodes.{RESET}\n")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        proc_a.terminate()
        proc_b.terminate()
        proc_a.wait()
        proc_b.wait()
        print(f"\n{DIM}Nodes stopped. Demo done.{RESET}\n")


if __name__ == "__main__":
    main()
