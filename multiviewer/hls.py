from __future__ import annotations

import os
import subprocess
import threading
from pathlib import Path
from typing import Iterable, Optional

import numpy as np


def start_hls_writer(
    output_dir: Path,
    width: int,
    height: int,
    fps: int,
    segment_time: float = 1.0,
    list_size: int = 6,
    encoder: str = "libx264",
    extra_args: Optional[Iterable[str]] = None,
) -> subprocess.Popen:
    """
    Launch ffmpeg to consume raw BGR frames on stdin and produce HLS segments and playlist.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    playlist = output_dir / "index.m3u8"
    segment_pattern = output_dir / "seg_%03d.ts"
    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "bgr24",
        "-s",
        f"{width}x{height}",
        "-r",
        str(fps),
        "-i",
        "-",
        "-an",
        "-c:v",
        encoder,
        "-preset",
        "veryfast",
        "-tune",
        "zerolatency",
        "-pix_fmt",
        "yuv420p",
        "-f",
        "hls",
        "-hls_time",
        str(segment_time),
        "-hls_list_size",
        str(list_size),
        "-hls_flags",
        "delete_segments+append_list",
        "-hls_segment_filename",
        str(segment_pattern),
        str(playlist),
    ]
    if extra_args:
        cmd[1:1] = list(extra_args)  # insert after ffmpeg for any global args
    try:
        return subprocess.Popen(cmd, stdin=subprocess.PIPE)
    except FileNotFoundError as exc:
        raise RuntimeError("ffmpeg not found on PATH, required for HLS output.") from exc
