from __future__ import annotations

import csv
import json
import socket
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass
class StreamMeta:
    program_id: int
    service_name: str
    width: Optional[int]
    height: Optional[int]
    fps: Optional[float]
    local_url: str


def _parse_rate(rate_str: str | None) -> Optional[float]:
    if not rate_str or rate_str == "0/0":
        return None
    if "/" in rate_str:
        num, den = rate_str.split("/", 1)
        try:
            num_f = float(num)
            den_f = float(den)
            if den_f == 0:
                return None
            return num_f / den_f
        except Exception:
            return None
    try:
        return float(rate_str)
    except Exception:
        return None


def _is_multicast(host: str) -> bool:
    try:
        first = int(host.split(".")[0])
        return 224 <= first <= 239
    except Exception:
        return False


def _parse_host_port(addr: str) -> Tuple[str, int]:
    raw = addr.strip()
    if "://" in raw:
        raw = raw.split("://", 1)[1]
    host, sep, port = raw.rpartition(":")
    if sep:
        return host, int(port)
    return raw, 6000


def _load_cache(path: Path) -> Dict[Tuple[str, int, int], StreamMeta]:
    cache: Dict[Tuple[str, int, int], StreamMeta] = {}
    if not path.exists():
        return cache
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                src = row.get("source", "")
                program_id = int(row.get("programId", ""))
                local_url = row.get("localUrl", "")
                width = int(row["videoWidth"]) if row.get("videoWidth") else None
                height = int(row["videoHeight"]) if row.get("videoHeight") else None
                fps = float(row["fps"]) if row.get("fps") else None
                service = row.get("serviceName", "") or f"PROGRAM_{program_id}"
            except Exception:
                continue
            host, port = _parse_host_port(src)
            cache[(host, port, program_id)] = StreamMeta(
                program_id=program_id,
                service_name=service,
                width=width,
                height=height,
                fps=fps,
                local_url=local_url,
            )
    return cache


def _save_cache(path: Path, metas: Iterable[StreamMeta], source_url: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for meta in metas:
        rows.append(
            {
                "source": source_url,
                "programId": meta.program_id,
                "localUrl": meta.local_url,
                "serviceName": meta.service_name,
                "videoWidth": meta.width or "",
                "videoHeight": meta.height or "",
                "fps": meta.fps or "",
            }
        )
    existing = []
    if path.exists():
        with path.open("r", newline="") as f:
            existing = list(csv.DictReader(f))
    with path.open("w", newline="") as f:
        fieldnames = ["source", "programId", "localUrl", "serviceName", "videoWidth", "videoHeight", "fps"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        # keep entries not matching this source
        for row in existing:
            if row.get("source") != source_url:
                writer.writerow(row)
        for row in rows:
            writer.writerow(row)


def probe_programs(url: str) -> List[dict]:
    cmd = [
        "ffprobe",
        "-v",
        "warning",
        "-print_format",
        "json",
        "-show_programs",
        "-show_streams",
        url,
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError as exc:
        raise RuntimeError("ffprobe not found on PATH") from exc
    if res.returncode != 0:
        raise RuntimeError(f"ffprobe failed for {url}: {res.stderr.strip() or res.stdout.strip()}")
    data = json.loads(res.stdout or "{}")
    return data.get("programs", [])


def _extract_meta(program: dict) -> Tuple[Optional[int], Optional[int], Optional[float], str]:
    width = None
    height = None
    fps = None
    service_name = (program.get("tags") or {}).get("service_name") or ""
    for stream in program.get("streams", []):
        if stream.get("codec_type") == "video":
            width = stream.get("width") or width
            height = stream.get("height") or height
            fps = _parse_rate(stream.get("avg_frame_rate") or stream.get("r_frame_rate")) or fps
    return width, height, fps, service_name


def ensure_fanouts_for_dataframe(
    df,
    cache_path: Path,
    base_port: int = 7000,
) -> Tuple:
    """
    For multicast sources with programId set, ensure a single ffmpeg fan-out per source.
    Returns (updated_df, [subprocess.Popen], cache_path).
    """
    import polars as pl

    cache = _load_cache(cache_path)
    procs: List[subprocess.Popen] = []
    next_port = base_port
    updated_rows = []

    # Group by source ip/port
    for key, group in df.group_by("ipAddress", maintain_order=True):
        src_raw = key[0] if isinstance(key, tuple) else key["ipAddress"][0]
        host, port = _parse_host_port(str(src_raw))
        # Only handle multicast + rows with programId set
        if not _is_multicast(host) or group["programId"].is_null().all():
            updated_rows.append(group)
            continue

        # Build mappings for this source
        program_ids = [int(p) for p in group["programId"].to_list() if p is not None]
        mappings: Dict[int, StreamMeta] = {}
        missing_ids: List[int] = []
        for pid in program_ids:
            cached = cache.get((host, port, pid))
            if cached and cached.local_url:
                mappings[pid] = cached
            else:
                missing_ids.append(pid)

        # Probe once if needed
        programs_meta: Dict[int, StreamMeta] = {}
        if missing_ids:
            url = f"udp://@{host}:{port}"
            programs = probe_programs(url)
            for prog in programs:
                pid = prog.get("program_id")
                if pid in program_ids:
                    w, h, fps, svc = _extract_meta(prog)
                    local_port = next_port
                    next_port += 1
                    meta = StreamMeta(
                        program_id=pid,
                        service_name=svc or f"PROGRAM_{pid}",
                        width=w,
                        height=h,
                        fps=fps,
                        local_url=f"udp://127.0.0.1:{local_port}",
                    )
                    programs_meta[pid] = meta
        # Merge cached + newly probed
        for pid in program_ids:
            if pid in mappings:
                continue
            if pid in programs_meta:
                mappings[pid] = programs_meta[pid]
            else:
                # Unknown program, leave unchanged
                continue

        # If nothing mapped, keep rows unchanged
        if not mappings:
            updated_rows.append(group)
            continue

        # Start fan-out process for this source using all mapped PIDs
        url = f"udp://@{host}:{port}"
        cmd = ["ffmpeg", "-y", "-fflags", "nobuffer", "-flags", "low_delay", "-i", url]
        for meta in mappings.values():
            cmd.extend(
                ["-map", f"0:p:{meta.program_id}", "-c", "copy", "-f", "mpegts", meta.local_url]
            )
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            procs.append(proc)
        except FileNotFoundError as exc:
            raise RuntimeError("ffmpeg not found on PATH, required for multicast fan-out.") from exc

        # Update rows to point to local_url and populate metadata
        updated = group.with_columns(
            pl.col("programId"),
            pl.col("channelName"),
        ).to_dicts()
        for row in updated:
            pid = row.get("programId")
            meta = mappings.get(pid)
            if meta:
                row["ipAddress"] = meta.local_url
                row["videoWidth"] = meta.width
                row["videoHeight"] = meta.height
                row["fps"] = meta.fps
        updated_rows.append(pl.DataFrame(updated))

        # Persist cache for this source
        _save_cache(cache_path, mappings.values(), source_url=url)

    new_df = updated_rows[0]
    for part in updated_rows[1:]:
        new_df = new_df.vstack(part)
    return new_df, procs, cache_path
