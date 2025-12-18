from __future__ import annotations

import secrets
import subprocess
import threading
from pathlib import Path
from typing import Dict, List

import polars as pl
from flask import Flask, jsonify, request, send_file, send_from_directory

from .registry import load_registry
from .hls import start_hls_writer

app = Flask(__name__)

# Basic in-memory process registry.
processes: Dict[str, subprocess.Popen] = {}
sdp_paths: Dict[str, Path] = {}
hls_paths: Dict[str, Path] = {}
registry_df: pl.DataFrame | None = None
registry_path: Path | None = None
registry_lock = threading.Lock()


def init_registry(path: str) -> None:
    global registry_df, registry_path
    with registry_lock:
        registry_df = load_registry(path)
        registry_path = Path(path)


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
    fwd = request.headers.get("X-Forwarded-For", "")
    ip = fwd.split(",")[0].strip() if fwd else request.remote_addr
    return jsonify({"ip": ip})


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

    session_id = secrets.token_hex(6)

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
    body { font-family: sans-serif; margin: 20px; background: #111; color: #eee; }
    .panel { background: #1b1b1b; padding: 16px; border-radius: 8px; max-width: 640px; }
    label { display: block; margin-top: 10px; }
    input, select { width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #333; background: #222; color: #eee; }
    .channels { height: 200px; }
    button { margin-top: 16px; padding: 10px 16px; border: none; border-radius: 4px; background: #2d7cf6; color: #fff; cursor: pointer; }
    button:hover { background: #1f65c9; }
    .status { margin-top: 12px; }
    video { width: 100%; max-width: 640px; margin-top: 12px; background: #000; }
  </style>
</head>
<body>
  <div class="panel">
    <h2>Start RTP Stream</h2>
    <label>Your IP address (receiver)</label>
    <input id="ip" placeholder="e.g. 192.168.1.50">
    <label>Port</label>
    <input id="port" value="5004">
    <label>Channels (Ctrl/Cmd+click to select multiple)</label>
    <select id="channels" class="channels" multiple></select>
    <label><input type="checkbox" id="hls"> Play in browser (HLS)</label>
    <button onclick="start()">Start Stream</button>
    <div class="status" id="status"></div>
    <video id="player" controls style="display:none"></video>
  </div>
  <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
  <script>
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
      const sel = document.getElementById('channels');
      const channels = Array.from(sel.selectedOptions).map(o => o.value);
      const hls = document.getElementById('hls').checked;
      const payload = { ip, port, channels, hls };
      const res = await fetch('/api/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      const data = await res.json();
      const status = document.getElementById('status');
      const player = document.getElementById('player');
      player.style.display = 'none';
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
          const hlsPlayer = new Hls();
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
