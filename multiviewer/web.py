from __future__ import annotations

import secrets
import subprocess
import threading
from pathlib import Path
from typing import Dict, List
import shutil
import time
import sys
import glob

import polars as pl
from flask import Flask, jsonify, render_template, request, send_file, send_from_directory

from .registry import load_registry
import csv

app = Flask(__name__, template_folder="templates")

# Basic in-memory process registry.
processes: Dict[str, subprocess.Popen] = {}
sdp_paths: Dict[str, Path] = {}
hls_paths: Dict[str, Path] = {}
session_timers: Dict[str, threading.Timer] = {}
sessions_by_target_ip: Dict[str, str] = {}
session_meta: Dict[str, Dict] = {}
registry_df: pl.DataFrame | None = None
registry_path: Path | None = None
registry_lock = threading.Lock()


def init_registry(path: str) -> None:
    global registry_df, registry_path
    with registry_lock:
        registry_df = load_registry(path)
        registry_path = Path(path)


def get_request_ip() -> str:
    fwd = request.headers.get("X-Forwarded-For", "")
    return fwd.split(",")[0].strip() if fwd else request.remote_addr or ""


def stop_session(session_id: str) -> None:
    proc = processes.pop(session_id, None)
    timer = session_timers.pop(session_id, None)
    if timer:
        timer.cancel()
    # Remove target-ip mapping if present
    for ip, sid in list(sessions_by_target_ip.items()):
        if sid == session_id:
            sessions_by_target_ip.pop(ip, None)
    if proc:
        try:
            proc.terminate()
        except Exception:
            pass
    sdp = sdp_paths.pop(session_id, None)
    hls_dir = hls_paths.pop(session_id, None)
    if hls_dir and hls_dir.exists():
        shutil.rmtree(hls_dir, ignore_errors=True)
    if sdp and sdp.exists():
        try:
            sdp.unlink()
        except Exception:
            pass
    meta = session_meta.pop(session_id, None)


@app.errorhandler(Exception)
def handle_error(exc):
    # Return JSON errors for API routes; otherwise fall back to default handling.
    if request.path.startswith("/api/"):
        return jsonify({"error": str(exc)}), 500
    raise exc


def build_live_command(
    ip:               str | None,
    port:             int | None,
    channels:         List[str] | None,
    sdp_path:         Path | None,
    width:            int = 1280,
    height:           int = 720,
    font_size:        int = 32,
    encoder:          str = "libx264",
    fps:              int = 30,
    hls_dir:          Path | None = None,
    hls_segment_time: float = 1.0,
    hls_list_size:    int = 6,
    bitrate_kbps:     int | None = None,
    registry_override: Path | None = None,
) -> List[str]:
    cmd = [
        sys.executable, "-m", "multiviewer.live",
        "--registry",  str(registry_override or registry_path),
        "--width",     str(width),
        "--height",    str(height),
        "--font-size", str(font_size),
        "--no-window",
    ]
    if ip and port:
        cmd.extend([
            "--rtp-out",    f"{ip}:{port}",
            "--rtp-fps",    str(fps),
            "--rtp-encoder", encoder,
            "--rtp-ffmpeg-arg", "-vf format=yuv420p",
        ])
        if bitrate_kbps:
            cmd.extend(["--rtp-bitrate-kbps", str(bitrate_kbps)])
        if sdp_path:
            cmd.extend(["--rtp-sdp-file", str(sdp_path)])
    if hls_dir:
        cmd.extend([
            "--hls-dir",          str(hls_dir),
            "--hls-segment-time", str(hls_segment_time),
            "--hls-list-size",    str(hls_list_size),
        ])
        if bitrate_kbps:
            cmd.extend(["--hls-bitrate-kbps", str(bitrate_kbps)])
    if channels:
        for ch in channels:
            cmd.extend(["--channel", ch])
    return cmd


def build_session_registry(layout_text: str, base_registry: pl.DataFrame, session_id: str) -> Path:
    """
    Create a temporary registry CSV with row/col positions based on a session layout text.
    layout_text: lines of comma-separated channel names; blanks are empty tiles.
    """
    lines = [line.strip() for line in layout_text.strip().splitlines() if line.strip() != ""]
    entries = []
    for r, line in enumerate(lines):
        cols = [c.strip() for c in line.split(",")]
        for c, name in enumerate(cols):
            if name == "":
                entries.append({"channelName": f"EMPTY_{r}_{c}", "direction": "", "ipAddress": "", "row": r, "col": c, "isEmpty": True, "rotation": 0, "trim": ""})
                continue
            match = base_registry.filter(pl.col("channelName") == name)
            if match.is_empty():
                # Unknown channel, treat as empty tile.
                entries.append({"channelName": f"EMPTY_{r}_{c}", "direction": "", "ipAddress": "", "row": r, "col": c, "isEmpty": True, "rotation": 0, "trim": ""})
                continue
            row = match.to_dicts()[0]
            row.update({"row": r, "col": c, "isEmpty": False, "rotation": row.get("rotation", 0) or 0, "trim": row.get("trim", "") or ""})
            entries.append(row)
    if not entries:
        raise ValueError("Session layout produced no entries.")
    # Write to temp CSV
    tmp_path = Path(f"/tmp/session_{session_id}.csv")
    fieldnames = ["channelName", "direction", "ipAddress", "row", "col", "isEmpty", "rotation", "trim"]
    # include any extra columns from registry to keep compatibility
    extra_cols = [c for c in base_registry.columns if c not in fieldnames]
    fieldnames.extend(extra_cols)
    with tmp_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for e in entries:
            for col in extra_cols:
                e.setdefault(col, "")
            writer.writerow(e)
    return tmp_path


def load_preset_layout(name: str) -> str:
    path = Path("sessions") / name
    if not path.exists():
        raise FileNotFoundError(f"Preset not found: {path}")
    return path.read_text()


@app.route("/")
def index():
    presets = [Path(p).name for p in glob.glob("sessions/*.csv")]
    return render_template("index.html", presets=presets)


@app.route("/api/channels")
def list_channels():
    if registry_df is None:
        return jsonify({"error": "registry not initialized"}), 500
    names = registry_df.get_column("channelName").to_list()
    return jsonify({"channels": names})


@app.route("/api/me")
def my_ip():
    # Prefer X-Forwarded-For if behind a proxy, otherwise use remote_addr.
    return jsonify({"ip": get_request_ip()})


@app.route("/api/sessions")
def list_sessions():
    now = time.time()
    items = []
    for sid, meta in session_meta.items():
        expires_at = meta.get("started_at", now) + meta.get("duration", 0)
        items.append(
            {
                "id": sid,
                "ip": meta.get("ip"),
                "port": meta.get("port"),
                "channels": meta.get("channels"),
                "mode": meta.get("mode"),
                "started_at": meta.get("started_at"),
                "expires_at": expires_at,
            }
        )
    return jsonify({"sessions": items})


@app.route("/api/stop/<session_id>", methods=["POST"])
def stop_session_api(session_id: str):
    if session_id not in processes:
        return jsonify({"error": "session not found"}), 404
    stop_session(session_id)
    return jsonify({"stopped": session_id})


@app.route("/api/start", methods=["POST"])
def start_stream():
    if registry_df is None or registry_path is None:
        return jsonify({"error": "registry not initialized"}), 500
    data      = request.get_json(force=True, silent=True) or {}
    ip        = data.get("ip")
    port      = int(data.get("port", 5004))
    channels  = data.get("channels") or None
    session_layout = data.get("sessionLayout")
    preset_name    = data.get("preset")
    width     = int(data.get("width", 1280))
    height    = int(data.get("height", 720))
    font_size = int(data.get("fontSize", 32))
    encoder    = data.get("encoder", "libx264")
    fps        = int(data.get("fps", 30))
    use_hls    = bool(data.get("hls", False))
    duration   = int(data.get("duration", 300))  # seconds
    bitrate    = data.get("bitrateKbps", "6500")
    bitrate    = int(bitrate) if bitrate not in ("", None) else 6500
    hls_segment = float(data.get("hlsSegment", 1.0))
    hls_list_size = int(data.get("hlsListSize", 6))

    session_id = secrets.token_hex(6)

    # Enforce one session per target IP (when provided).
    if ip:
        existing = sessions_by_target_ip.get(ip)
        if existing:
            stop_session(existing)

    # Override layout if a preset is provided.
    if preset_name:
        session_layout = load_preset_layout(preset_name)

    try:
        if use_hls:
            hls_dir = Path(f"/tmp/hls_{session_id}")
            # If a session layout is provided, build a temporary registry file with positions.
            registry_path_to_use = registry_path
            if session_layout:
                tmp_registry = build_session_registry(session_layout, registry_df, session_id)
                registry_path_to_use = tmp_registry
            cmd = build_live_command(
                None,
                None,
                channels,
                None,
                width=width,
                height=height,
                font_size=font_size,
                encoder=encoder,
                fps=fps,
                hls_dir=hls_dir,
                hls_segment_time=hls_segment,
                hls_list_size=hls_list_size,
                bitrate_kbps=bitrate,
                registry_override=registry_path_to_use,
            )
            proc = subprocess.Popen(cmd)
            processes[session_id] = proc
            hls_paths[session_id] = hls_dir
            session_meta[session_id] = {
                "mode": "hls",
                "ip": None,
                "port": None,
                "channels": channels,
                "started_at": time.time(),
                "duration": duration,
            }
            t = threading.Timer(duration, stop_session, args=[session_id])
            t.daemon = True
            t.start()
            session_timers[session_id] = t

            playlist = hls_dir / "index.m3u8"
            for _ in range(50):  # ~5s max
                if playlist.exists():
                    break
                time.sleep(0.1)
            if not playlist.exists():
                stop_session(session_id)
                return jsonify({"error": "Failed to start HLS (playlist not created)"}), 500

            return jsonify({"session": session_id, "hls_url": f"/hls/{session_id}/index.m3u8", "cmd": cmd})
        else:
            if not ip:
                return jsonify({"error": "ip is required"}), 400
            sdp_path = Path(f"/tmp/mosaic_{session_id}.sdp")
            registry_path_to_use = registry_path
            if session_layout:
                tmp_registry = build_session_registry(session_layout, registry_df, session_id)
                registry_path_to_use = tmp_registry
            cmd = build_live_command(
                ip,
                port,
                channels,
                sdp_path,
                width=width,
                height=height,
                font_size=font_size,
                encoder=encoder,
                fps=fps,
                bitrate_kbps=bitrate,
                registry_override=registry_path_to_use,
            )

            proc = subprocess.Popen(cmd)
            processes[session_id] = proc
            sdp_paths[session_id] = sdp_path
            sessions_by_target_ip[ip] = session_id
            session_meta[session_id] = {
                "mode": "rtp",
                "ip": ip,
                "port": port,
                "channels": channels,
                "started_at": time.time(),
                "duration": duration,
            }
            t = threading.Timer(duration, stop_session, args=[session_id])
            t.daemon = True
            t.start()
            session_timers[session_id] = t

            return jsonify({"session": session_id, "sdp_url": f"/sdp/{session_id}", "cmd": cmd})
    except Exception as exc:
        stop_session(session_id)
        return jsonify({"error": f"Failed to start: {exc}"}), 500


@app.route("/sdp/<session_id>")
def fetch_sdp(session_id: str):
    path = sdp_paths.get(session_id)
    if not path or not path.exists():
        return jsonify({"error": "SDP not found"}), 404
    return send_file(path, mimetype="application/sdp", as_attachment=True, download_name="mosaic.sdp")


@app.route("/hls/<session_id>/<path:filename>")
def serve_hls(session_id: str, filename: str):
    base = hls_paths.get(session_id)
    if not base or not base.exists():
        return jsonify({"error": "HLS session not found"}), 404
    return send_from_directory(base, filename)


def run(host: str = "0.0.0.0", port: int = 8080, registry: str = "example_registry.csv"):
    init_registry(registry)
    app.run(host=host, port=port)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Web UI to launch RTP multiviewer streams.")
    parser.add_argument("--host",     default="0.0.0.0")
    parser.add_argument("--port",     type=int, default=8080)
    parser.add_argument("--registry", default="example_registry.csv")
    args = parser.parse_args()
    run(host=args.host, port=args.port, registry=args.registry)


if __name__ == "__main__":
    main()
