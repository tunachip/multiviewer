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


def sniff_source(iface: str, dst_host: str, dst_port: int, packets: int = 5, timeout: int = 5) -> Tuple[str, int] | Tuple[None, None]:
    """
    Run a short tcpdump capture for the destination host/port and return the first source IP/port seen.
    """
    flt = f"udp and dst host {dst_host} and dst port {dst_port}"
    try:
        proc = subprocess.run(
            ["tcpdump", "-i", iface, "-nn", "-c", str(packets), flt],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise SystemExit("tcpdump not found on PATH") from exc
    except subprocess.TimeoutExpired:
        return None, None
    output = (proc.stdout or b"").decode(errors="ignore").splitlines()
    ip_re = re.compile(r"IP\s+([\d\.]+)\.(\d+)\s+>\s+([\d\.]+)\.(\d+)")
    for line in output:
        m = ip_re.search(line)
        if m:
            src_ip, src_port, _, _ = m.groups()
            return src_ip, int(src_port)
    return None, None


def main() -> None:
    parser = argparse.ArgumentParser(description="Map registry destinations to observed source IP/port via tcpdump.")
    parser.add_argument("--registry", required=True, help="Path to the registry CSV (expects ipAddress column).")
    parser.add_argument("--iface", required=True, help="Interface to listen on (e.g., eth0).")
    parser.add_argument("--out", default="registry_with_sources.csv", help="Output CSV path.")
    parser.add_argument("--packets", type=int, default=5, help="Packets to capture per entry (default: 5).")
    parser.add_argument("--timeout", type=int, default=5, help="Seconds before giving up per entry (default: 5).")
    args = parser.parse_args()

    reg_path = Path(args.registry)
    if not reg_path.exists():
        raise SystemExit(f"Registry not found: {reg_path}")

    with reg_path.open("r", newline="") as f_in, Path(args.out).open("w", newline="") as f_out:
        reader = csv.DictReader(f_in)
        fieldnames = reader.fieldnames or []
        if "ipAddress" not in fieldnames:
            raise SystemExit("Registry is missing ipAddress column")
        extra_cols = [c for c in ("sourceIp", "sourcePort") if c not in fieldnames]
        writer = csv.DictWriter(f_out, fieldnames=fieldnames + extra_cols)
        writer.writeheader()
        for row in reader:
            dst = row.get("ipAddress", "").strip()
            if not dst:
                writer.writerow(row)
                continue
            host, port = parse_host_port(dst)
            src_ip, src_port = sniff_source(args.iface, host, port, packets=args.packets, timeout=args.timeout)
            if src_ip:
                row["sourceIp"] = src_ip
            if src_port:
                row["sourcePort"] = src_port
            writer.writerow(row)


if __name__ == "__main__":
    main()
