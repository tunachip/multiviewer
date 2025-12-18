from __future__ import annotations

import secrets
import subprocess
import threading
from pathlib import Path
from typing import Dict, List
import shutil
import time

import polars as pl
from flask import Flask, jsonify, request, send_file, send_from_directory

from .registry import load_registry
from .hls import start_hls_writer

app = Flask(__name__)

# Basic in-memory process registry.
processes: Dict[str, subprocess.Popen] = {}
sdp_paths: Dict[str, Path] = {}
hls_paths: Dict[str, Path] = {}
session_timers: Dict[str, threading.Timer] = {}
sessions_by_target_ip: Dict[str, str] = {}
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


def build_live_command(
    ip: str | None,
    port: int | None,
    channels: List[str] | None,
    sdp_path: Path | None,
    width: int = 1280,
    height: int = 720,
    font_size: int = 32,
    encoder: str = "mpeg4",
    fps: int = 30,
    hls_dir: Path | None = None,
    hls_segment_time: float = 1.0,
    hls_list_size: int = 6,
) -> List[str]:
    cmd = [
        "python",
        "-m",
        "multiviewer.live",
        "--registry",
        str(registry_path),
        "--width",
        str(width),
        "--height",
        str(height),
        "--font-size",
        str(font_size),
        "--no-window",
    ]
    if ip and port:
        cmd.extend(
            [
                "--rtp-out",
                f"{ip}:{port}",
                "--rtp-fps",
                str(fps),
                "--rtp-encoder",
                encoder,
                "--rtp-ffmpeg-arg",
                "-vf format=yuv420p",
            ]
        )
        if sdp_path:
            cmd.extend(["--rtp-sdp-file", str(sdp_path)])
    if hls_dir:
        cmd.extend(
            [
                "--hls-dir",
                str(hls_dir),
                "--hls-segment-time",
                str(hls_segment_time),
                "--hls-list-size",
                str(hls_list_size),
            ]
        )
    if channels:
        for ch in channels:
            cmd.extend(["--channel", ch])
    return cmd


@app.route("/")
def index():
    return INDEX_HTML


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


@app.route("/api/start", methods=["POST"])
def start_stream():
    if registry_df is None or registry_path is None:
        return jsonify({"error": "registry not initialized"}), 500
    data = request.get_json(force=True, silent=True) or {}
    ip = data.get("ip")
    port = int(data.get("port", 5004))
    channels = data.get("channels") or None
    width = int(data.get("width", 1280))
    height = int(data.get("height", 720))
    font_size = int(data.get("fontSize", 32))
    encoder = data.get("encoder", "mpeg4")
    fps = int(data.get("fps", 30))
    use_hls = bool(data.get("hls", False))
    duration = int(data.get("duration", 300))  # seconds

    session_id = secrets.token_hex(6)

    # Enforce one session per target IP (when provided).
    if ip:
        existing = sessions_by_target_ip.get(ip)
        if existing:
            stop_session(existing)

    if use_hls:
        hls_dir = Path(f"/tmp/hls_{session_id}")
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
        )
        proc = subprocess.Popen(cmd)
        processes[session_id] = proc
        hls_paths[session_id] = hls_dir
        # schedule timeout
        t = threading.Timer(duration, stop_session, args=[session_id])
        t.daemon = True
        t.start()
        session_timers[session_id] = t

        # Wait briefly for playlist to appear to avoid browser 404 loops.
        playlist = hls_dir / "index.m3u8"
        for _ in range(50):  # ~5s max
            if playlist.exists():
                break
            import time

            time.sleep(0.1)
        if not playlist.exists():
            try:
                proc.terminate()
            except Exception:
                pass
            return jsonify({"error": "Failed to start HLS (playlist not created)"}, ), 500

        return jsonify({"session": session_id, "hls_url": f"/hls/{session_id}/index.m3u8", "cmd": cmd})
    else:
        if not ip:
            return jsonify({"error": "ip is required"}), 400
        sdp_path = Path(f"/tmp/mosaic_{session_id}.sdp")
        cmd = build_live_command(
            ip, port, channels, sdp_path, width=width, height=height, font_size=font_size, encoder=encoder, fps=fps
        )

        proc = subprocess.Popen(cmd)
        processes[session_id] = proc
        sdp_paths[session_id] = sdp_path
        sessions_by_target_ip[ip] = session_id
        t = threading.Timer(duration, stop_session, args=[session_id])
        t.daemon = True
        t.start()
        session_timers[session_id] = t

        return jsonify({"session": session_id, "sdp_url": f"/sdp/{session_id}", "cmd": cmd})


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
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--registry", default="example_registry.csv")
    args = parser.parse_args()
    run(host=args.host, port=args.port, registry=args.registry)


INDEX_HTML = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Multiviewer RTP Launcher</title>
  <style>
    :root { color-scheme: dark; }
    body { margin: 0; font-family: sans-serif; background: #0f0f10; color: #f1f1f1; }
    .layout { display: flex; min-height: 100vh; }
    .sidebar { width: 320px; max-width: 90vw; background: #16161a; border-right: 1px solid #23232a; padding: 16px; box-sizing: border-box; transition: transform 0.2s ease; }
    .sidebar.collapsed { transform: translateX(-100%); }
    .toggle { position: absolute; top: 12px; left: 12px; z-index: 2; background: #2d7cf6; color: #fff; border: none; border-radius: 4px; padding: 8px 12px; cursor: pointer; }
    h2 { margin-top: 0; }
    label { display: block; margin-top: 10px; font-size: 14px; color: #cfd1d5; }
    input, select { width: 100%; padding: 8px; border-radius: 6px; border: 1px solid #2b2c33; background: #1f1f26; color: #f1f1f1; box-sizing: border-box; }
    .channels { height: 200px; }
    button.primary { margin-top: 16px; width: 100%; padding: 12px 16px; border: none; border-radius: 6px; background: #2d7cf6; color: #fff; cursor: pointer; font-size: 15px; }
    button.primary:hover { background: #1f65c9; }
    .status { margin-top: 12px; min-height: 22px; }
    .content { flex: 1; display: flex; align-items: center; justify-content: center; padding: 16px; box-sizing: border-box; }
    .player-wrap { width: 100%; max-width: 100%; }
    video { width: 100%; height: auto; max-height: 90vh; background: #000; border: 1px solid #23232a; border-radius: 8px; }
  </style>
</head>
<body>
  <button class="toggle" onclick="toggleSidebar()">Settings</button>
  <div class="layout">
    <div id="sidebar" class="sidebar">
      <h2>Start Stream</h2>
      <label>Your IP address (receiver)</label>
      <input id="ip" placeholder="e.g. 192.168.1.50">
      <label>Port</label>
      <input id="port" value="5004">
      <label>Duration (seconds, default 300)</label>
      <input id="duration" value="300">
      <label>Channels (Ctrl/Cmd+click to select multiple)</label>
      <select id="channels" class="channels" multiple></select>
      <label><input type="checkbox" id="hls"> Play in browser (HLS)</label>
      <button class="primary" onclick="start()">Start Stream</button>
      <div class="status" id="status"></div>
    </div>
    <div class="content">
      <div class="player-wrap">
        <video id="player" controls style="display:none"></video>
      </div>
    </div>
  </div>
  <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
  <script>
    let hlsPlayer = null;
    function toggleSidebar() {
      const sb = document.getElementById('sidebar');
      sb.classList.toggle('collapsed');
    }
    async function loadChannels() {
      const res = await fetch('/api/channels');
      const data = await res.json();
      const sel = document.getElementById('channels');
      sel.innerHTML = '';
      (data.channels || []).forEach(name => {
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = name;
        sel.appendChild(opt);
      });
    }

    async function loadIp() {
      try {
        const res = await fetch('/api/me');
        const data = await res.json();
        if (data.ip) {
          document.getElementById('ip').value = data.ip;
        }
      } catch (e) {
        // ignore
      }
    }
    async function start() {
      const ip = document.getElementById('ip').value.trim();
      const port = document.getElementById('port').value.trim() || '5004';
      const duration = document.getElementById('duration').value.trim() || '300';
      const sel = document.getElementById('channels');
      const channels = Array.from(sel.selectedOptions).map(o => o.value);
      const hls = document.getElementById('hls').checked;
      const payload = { ip, port, channels, hls, duration };
      const res = await fetch('/api/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      const data = await res.json();
      const status = document.getElementById('status');
      const player = document.getElementById('player');
      player.style.display = 'none';
      if (hlsPlayer) {
        hlsPlayer.destroy();
        hlsPlayer = null;
      }
      if (!res.ok) {
        status.textContent = data.error || 'Failed to start.';
        status.style.color = '#f66';
        return;
      }
      if (data.hls_url) {
        const hlsUrl = new URL(data.hls_url, window.location.origin).href;
        status.innerHTML = 'Started HLS. <a href="'+hlsUrl+'" style="color:#6cf">Open playlist</a>';
        status.style.color = '#6cf';
        if (Hls.isSupported()) {
          hlsPlayer = new Hls({ liveDurationInfinity: true });
          hlsPlayer.loadSource(hlsUrl);
          hlsPlayer.attachMedia(player);
          player.style.display = 'block';
          player.play();
        } else if (player.canPlayType('application/vnd.apple.mpegurl')) {
          player.src = hlsUrl;
          player.style.display = 'block';
          player.play();
        } else {
          status.innerHTML += ' (Browser cannot play HLS natively)';
        }
      } else {
        const sdpUrl = new URL(data.sdp_url, window.location.origin).href;
        const vlcUrl = 'vlc://' + sdpUrl;
        status.innerHTML = 'Started. <a href="'+sdpUrl+'" style="color:#6cf">Download SDP</a> or '
          + '<a href="'+vlcUrl+'" style="color:#9cf">Open in VLC</a> (may prompt)';
        status.style.color = '#6cf';
      }
    }
    loadChannels();
    loadIp();
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
