#!/usr/bin/env python3
"""
Probe RTP/UDP destinations from a registry CSV and record the observed source IP/port.

Usage:
  sudo python scripts/map_sources.py --registry example_registry.csv --iface eth0 --out mapped_sources.csv

Notes:
- Requires tcpdump on PATH and permission to capture on the given interface (often needs sudo).
- Assumes ipAddress values look like:
    * rtp://238.10.36.60:6000
    * 238.10.36.60:6000
    * 238.10.36.60   (port defaults to 6000)
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import time
from pathlib import Path
from typing import Tuple


def parse_host_port(addr: str) -> Tuple[str, int]:
    raw = addr.strip()
    if "://" in raw:
        raw = raw.split("://", 1)[1]
    host, sep, port = raw.rpartition(":")
    if sep:
        return host, int(port)
    return raw, 6000


def sniff_source(
    iface: str,
    dst_host: str,
    dst_port: int,
    packets: int = 10,
    timeout: int = 10,
    join_analyzeduration: int = 0,
    join_probesize: int = 32768,
    verbose: bool = False,
) -> Tuple[str, int] | Tuple[None, None]:
    """
    Run a short tcpdump capture for the destination host/port and return the first source IP/port seen.
    """
    flt = f"udp and dst host {dst_host} and dst port {dst_port}"
    try:
        tcpdump_cmd = ["tcpdump", "-i", iface, "-nnvvv", "-c", str(packets), flt]
        if verbose:
            print(f"  tcpdump: {' '.join(tcpdump_cmd)}")
        tcpdump_proc = subprocess.Popen(
            tcpdump_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise SystemExit("tcpdump not found on PATH") from exc
    # Kick off a short ffprobe to issue an IGMP join so packets flow while tcpdump listens.
    join_proc = None
    try:
        join_cmd = [
            "ffprobe",
            "-v",
            "error",
            "-probesize",
            str(join_probesize),
            "-analyzeduration",
            str(join_analyzeduration),
            "-timeout",
            str(timeout * 1_000_000),
            "-i",
            f"udp://@{dst_host}:{dst_port}",
        ]
        if verbose:
            print(f"  ffprobe join: {' '.join(join_cmd)}")
        join_proc = subprocess.Popen(
            join_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        join_proc = None
    try:
        stdout, stderr = tcpdump_proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        tcpdump_proc.kill()
        stdout, stderr = tcpdump_proc.communicate()
    # Ensure join proc is terminated
    if join_proc and join_proc.poll() is None:
        try:
            join_proc.terminate()
        except Exception:
            pass
    pkt_bytes = stdout or b""
    stderr_bytes = stderr or b""
    if verbose:
        print(f"  tcpdump bytes={len(pkt_bytes)}, stderr bytes={len(stderr_bytes)}")
    # Try parsing pcap bytes directly for robustness.
    # Parse text output (more reliable across environments)
    output_lines = (pkt_bytes or b"").decode(errors="ignore").splitlines()
    if not output_lines:
        output_lines = stderr_bytes.decode(errors="ignore").splitlines()

    if verbose and output_lines:
        preview = output_lines[:3] + (["..."] if len(output_lines) > 3 else [])
        for ln in preview:
            print(f"  tcpdump line: {ln}")

    ip_re = re.compile(r"IP[^>]*?(\d+\.\d+\.\d+\.\d+)\.(\d+)\s*>\s*(\d+\.\d+\.\d+\.\d+)\.(\d+)")
    for ln in output_lines:
        m = ip_re.search(ln)
        if m:
            src_ip, src_port, _, _ = m.groups()
            return src_ip, int(src_port)
    # fallback to near-summary lines
    if len(output_lines) >= 4:
        m = ip_re.search(output_lines[-4])
        if m:
            src_ip, src_port, _, _ = m.groups()
            return src_ip, int(src_port)
    return None, None


def main() -> None:
    parser = argparse.ArgumentParser(description="Map registry destinations to observed source IP/port via tcpdump.")
    parser.add_argument("--registry", required=True, help="Path to the registry CSV (expects ipAddress column).")
    parser.add_argument("--iface", required=True, help="Interface to listen on (e.g., eth0).")
    parser.add_argument("--out", default="registry_with_sources.csv", help="Output CSV path.")
    parser.add_argument("--packets", type=int, default=10, help="Packets to capture per entry (default: 10).")
    parser.add_argument("--timeout", type=int, default=10, help="Seconds before giving up per entry (default: 10).")
    parser.add_argument("--join-analyzeduration", type=int, default=1_000_000, help="ffprobe analyzeduration (microseconds) for the multicast join (default: 1_000_000 = 1s). Use 0 for legacy behavior.")
    parser.add_argument("--join-probesize", type=int, default=65536, help="ffprobe probesize for the multicast join (default: 65536).")
    parser.add_argument("--verbose", action="store_true", help="Print detailed progress and command output.")
    args = parser.parse_args()

    reg_path = Path(args.registry)
    if not reg_path.exists():
        raise SystemExit(f"Registry not found: {reg_path}")

    entries = reg_path.read_text().splitlines()
    with reg_path.open("r", newline="") as f_in, Path(args.out).open("w", newline="") as f_out:
        reader = csv.DictReader(f_in)
        fieldnames = reader.fieldnames or []
        if "ipAddress" not in fieldnames:
            raise SystemExit("Registry is missing ipAddress column")
        extra_cols = [c for c in ("sourceIp", "sourcePort") if c not in fieldnames]
        writer = csv.DictWriter(f_out, fieldnames=fieldnames + extra_cols)
        writer.writeheader()
        total = sum(1 for _ in reader)
        f_in.seek(0)
        reader = csv.DictReader(f_in)
        idx = 0
        for row in reader:
            idx += 1
            dst = row.get("ipAddress", "").strip()
            if not dst:
                writer.writerow(row)
                print(f"[{idx}/{total}] {row.get('channelName','(unknown)')}: no ipAddress, skipped")
                continue
            host, port = parse_host_port(dst)
            print(f"[{idx}/{total}] {row.get('channelName','(unknown)')} -> {host}:{port} ...", end="", flush=True)
            src_ip, src_port = sniff_source(
                args.iface,
                host,
                port,
                packets=args.packets,
                timeout=args.timeout,
                join_analyzeduration=args.join_analyzeduration,
                join_probesize=args.join_probesize,
                verbose=args.verbose,
            )
            if src_ip:
                row["sourceIp"] = src_ip
            if src_port:
                row["sourcePort"] = src_port
            writer.writerow(row)
            status = f" source {src_ip}:{src_port}" if src_ip else " no packets"
            print(status)


if __name__ == "__main__":
    main()
