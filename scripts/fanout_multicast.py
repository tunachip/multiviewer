#!/usr/bin/env python3
"""
Discover programs in a multicast transport stream and emit a local fan-out plan.

What it does:
- Runs ffprobe on a multicast group/port to list program_ids (and service names if present).
- Generates a registry CSV pointing channels at local UDP ports (one per program).
- Prints an ffmpeg command that joins the multicast once and remuxes each program to a local UDP port.
- Optional: runs the ffmpeg fan-out immediately (`--execute`) and keeps it running.

Example:
  python scripts/fanout_multicast.py \\
    --group 238.10.36.60 --port 6000 \\
    --base-port 7000 \\
    --registry local_registry.csv \\
    --execute

This will:
  - probe 238.10.36.60:6000 for programs
  - write local_registry.csv mapping each program to udp://127.0.0.1:<7000+idx>
  - start ffmpeg to copy each program to those local ports

Prereqs: ffprobe/ffmpeg on PATH.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


def ffprobe_programs(url: str) -> List[dict]:
    """
    Return list of program dicts with program_id and service_name (if present).
    """
    cmd = [
        "ffprobe",
        "-v",
        "warning",
        "-print_format",
        "json",
        "-show_programs",
        url,
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError as exc:
        raise SystemExit("ffprobe not found on PATH") from exc
    if res.returncode != 0:
        raise SystemExit(f"ffprobe failed: {res.stderr.strip() or res.stdout.strip()}")
    data = json.loads(res.stdout or "{}")
    return data.get("programs", [])


def build_ffmpeg_command(source_url: str, mappings: List[Tuple[int, int]]) -> List[str]:
    """
    Build a single ffmpeg command that maps each program_id to a local UDP port.
    mappings: list of (program_id, port)
    """
    cmd: List[str] = [
        "ffmpeg",
        "-y",
        "-fflags",
        "nobuffer",
        "-flags",
        "low_delay",
        "-i",
        source_url,
    ]
    for prog_id, port in mappings:
        cmd.extend(
            [
                "-map",
                f"0:p:{prog_id}",
                "-c",
                "copy",
                "-f",
                "mpegts",
                f"udp://127.0.0.1:{port}",
            ]
        )
    return cmd


def write_registry(path: Path, mappings: List[Tuple[int, int]], names: List[str]) -> None:
    """
    Write a registry CSV with channelName/direction/ipAddress.
    """
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["channelName", "direction", "ipAddress"])
        for (prog_id, port), name in zip(mappings, names):
            writer.writerow([name, "", f"udp://127.0.0.1:{port}"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Fan out multicast TS programs to local UDP and emit a registry.")
    parser.add_argument("--group", required=True, help="Multicast group IP (e.g., 238.10.36.60).")
    parser.add_argument("--port", type=int, required=True, help="Multicast UDP port (e.g., 6000).")
    parser.add_argument("--base-port", type=int, default=7000, help="Starting local UDP port for outputs (default 7000).")
    parser.add_argument("--registry", type=Path, default=Path("local_registry.csv"), help="Output registry CSV path.")
    parser.add_argument("--execute", action="store_true", help="Run the ffmpeg fan-out after generating the registry.")
    args = parser.parse_args()

    source_url = f"udp://@{args.group}:{args.port}"
    programs = ffprobe_programs(source_url)
    if not programs:
        raise SystemExit("No programs found in multicast stream.")

    mappings: List[Tuple[int, int]] = []
    names: List[str] = []
    for idx, prog in enumerate(programs):
        prog_id = prog.get("program_id")
        if prog_id is None:
            continue
        local_port = args.base_port + idx
        service_name = (prog.get("tags") or {}).get("service_name") or f"PROGRAM_{prog_id}"
        mappings.append((prog_id, local_port))
        names.append(service_name)

    if not mappings:
        raise SystemExit("Found programs but could not read program_id values.")

    write_registry(args.registry, mappings, names)
    print(f"Wrote registry with {len(mappings)} entries to {args.registry}")

    cmd = build_ffmpeg_command(source_url, mappings)
    print("\nFan-out command (joins multicast once and remuxes locally):")
    print(" ".join(cmd))

    if args.execute:
        print("\nExecuting fan-out... (Ctrl+C to stop)")
        try:
            proc = subprocess.Popen(cmd)
            proc.wait()
        except KeyboardInterrupt:
            proc.terminate()
        finally:
            sys.exit(proc.returncode if 'proc' in locals() else 0)


if __name__ == "__main__":
    main()
