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
            ["tcpdump", "-i", iface, "-nn", "-c", str(packets), "-w", "-", flt],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise SystemExit("tcpdump not found on PATH") from exc
    except subprocess.TimeoutExpired:
        return None, None
    # Try parsing pcap bytes directly for robustness.
    pkt_bytes = proc.stdout or b""
    if pkt_bytes:
        try:
            import struct

            # pcap global header is 24 bytes.
            if len(pkt_bytes) >= 24 + 16:
                magic = pkt_bytes[0:4]
                little = magic in (b"\xd4\xc3\xb2\xa1", b"\x4d\x3c\xb2\xa1")
                endian = "<" if little else ">"
                # First packet header starts at 24.
                pkt_off = 24
                if len(pkt_bytes) >= pkt_off + 16:
                    incl_len = struct.unpack(endian + "I", pkt_bytes[pkt_off + 8 : pkt_off + 12])[0]
                    data_off = pkt_off + 16
                    pkt = pkt_bytes[data_off : data_off + incl_len]
                    # Ethernet header 14 bytes; check IPv4 (0x0800)
                    if len(pkt) >= 14 + 20:
                        eth_type = struct.unpack(endian + "H", pkt[12:14])[0]
                        if eth_type == 0x0800:
                            ip_header = pkt[14:]
                            ihl = (ip_header[0] & 0x0F) * 4
                            src_ip = ".".join(str(b) for b in ip_header[12:16])
                            # UDP header after IP
                            udp_off = 14 + ihl
                            if len(pkt) >= udp_off + 4:
                                udp_hdr = pkt[udp_off : udp_off + 4]
                                src_port = struct.unpack(endian + "H", udp_hdr[0:2])[0]
                                return src_ip, src_port
        except Exception:
            pass

    # Fallback to text parsing from stderr if tcpdump emitted summary.
    output = (proc.stderr or b"").decode(errors="ignore").splitlines()
    def pick_line(lines):
        for ln in lines:
            if ">" in ln and "UDP" in ln and (" IP " in ln or ln.startswith("IP")):
                return ln
        return None

    line = pick_line(output)
    if not line and len(output) >= 4:
        line = output[-4]
    if line and ">" in line:
        try:
            lhs = line.split(">", 1)[0].strip()
            src_token = lhs.split()[-1]
            if "." in src_token:
                ip_part, port_part = src_token.rsplit(".", 1)
                parts = ip_part.split(".")
                if len(parts) > 4:
                    ip_part = ".".join(parts[:4])
                return ip_part, int(port_part)
        except Exception:
            pass
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
            src_ip, src_port = sniff_source(args.iface, host, port, packets=args.packets, timeout=args.timeout)
            if src_ip:
                row["sourceIp"] = src_ip
            if src_port:
                row["sourcePort"] = src_port
            writer.writerow(row)
            status = f" source {src_ip}:{src_port}" if src_ip else " no packets"
            print(status)


if __name__ == "__main__":
    main()
