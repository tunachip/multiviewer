from __future__ import annotations

import secrets
import subprocess
import threading
from pathlib import Path
from typing import Dict, List

import polars as pl
from flask import Flask, jsonify, request, send_file

from .registry import load_registry

app = Flask(__name__)

# Basic in-memory process registry.
processes: Dict[str, subprocess.Popen] = {}
sdp_paths: Dict[str, Path] = {}
registry_df: pl.DataFrame | None = None
registry_path: Path | None = None
registry_lock = threading.Lock()


def init_registry(path: str) -> None:
    global registry_df, registry_path
    with registry_lock:
        registry_df = load_registry(path)
        registry_path = Path(path)


def build_live_command(
    ip: str,
    port: int,
    channels: List[str] | None,
    sdp_path: Path,
    width: int = 1280,
    height: int = 720,
    font_size: int = 32,
    encoder: str = "mpeg4",
    fps: int = 30,
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
        "--rtp-out",
        f"{ip}:{port}",
        "--rtp-fps",
        str(fps),
        "--rtp-encoder",
        encoder,
        "--rtp-sdp-file",
        str(sdp_path),
        "--rtp-ffmpeg-arg",
        "-vf format=yuv420p",
        "--no-window",
    ]
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


@app.route("/api/start", methods=["POST"])
def start_stream():
    if registry_df is None or registry_path is None:
        return jsonify({"error": "registry not initialized"}), 500
    data = request.get_json(force=True, silent=True) or {}
    ip = data.get("ip")
    if not ip:
        return jsonify({"error": "ip is required"}), 400
    port = int(data.get("port", 5004))
    channels = data.get("channels") or None
    width = int(data.get("width", 1280))
    height = int(data.get("height", 720))
    font_size = int(data.get("fontSize", 32))
    encoder = data.get("encoder", "mpeg4")
    fps = int(data.get("fps", 30))

    session_id = secrets.token_hex(6)
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
    <button onclick="start()">Start Stream</button>
    <div class="status" id="status"></div>
  </div>
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
    async function start() {
      const ip = document.getElementById('ip').value.trim();
      const port = document.getElementById('port').value.trim() || '5004';
      const sel = document.getElementById('channels');
      const channels = Array.from(sel.selectedOptions).map(o => o.value);
      const payload = { ip, port, channels };
      const res = await fetch('/api/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      const data = await res.json();
      const status = document.getElementById('status');
      if (!res.ok) {
        status.textContent = data.error || 'Failed to start.';
        status.style.color = '#f66';
        return;
      }
      const sdpUrl = new URL(data.sdp_url, window.location.origin).href;
      const vlcUrl = 'vlc://' + sdpUrl;
      status.innerHTML = 'Started. <a href="'+sdpUrl+'" style="color:#6cf">Download SDP</a> or '
        + '<a href="'+vlcUrl+'" style="color:#9cf">Open in VLC</a> (may prompt)';
      status.style.color = '#6cf';
    }
    loadChannels();
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
