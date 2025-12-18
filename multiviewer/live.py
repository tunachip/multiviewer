from __future__ import annotations

import argparse
import signal
import subprocess
import threading
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import av
import cv2
import numpy as np
import polars as pl

from .layout import assign_grid, assign_grid_with_positions, apply_transforms
from .registry import load_registry
from .render import create_placeholder_grid_image
from .hls import start_hls_writer


def _rtp_url(ip_address: str) -> str:
    if "://" in ip_address:
        return ip_address
    # If no explicit port is provided, default to 6000.
    _, sep, maybe_port = ip_address.rpartition(":")
    has_port = sep == ":" and maybe_port.isdigit()
    if has_port:
        return f"rtp://{ip_address}"
    return f"rtp://{ip_address}:6000"


def _fit_frame(frame: np.ndarray, target_w: int, target_h: int) -> np.ndarray:
    """
    Resize frame to fit inside target while preserving aspect via letterboxing.
    """
    src_h, src_w = frame.shape[:2]
    scale = min(target_w / src_w, target_h / src_h)
    new_w, new_h = int(src_w * scale), int(src_h * scale)
    resized = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_AREA)

    # Letterbox into exact target size.
    result = np.zeros((target_h, target_w, 3), dtype=np.uint8)
    x_off = (target_w - new_w) // 2
    y_off = (target_h - new_h) // 2
    result[y_off:y_off + new_h, x_off:x_off + new_w] = resized
    return result


def _failure_frame(w: int, h: int, message: str) -> np.ndarray:
    """
    Create a red failure overlay frame.
    """
    frame = np.zeros((h, w, 3), dtype=np.uint8)
    cv2.putText(
        frame,
        message,
        (10, h // 2),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.7,
        (0, 0, 255),
        2,
        cv2.LINE_AA,
    )
    return frame


def _apply_trim(img: np.ndarray, trim_expr: str) -> np.ndarray:
    """
    Trim the image based on x:y:w:h. w/h can be percentages (e.g., 50%).
    Applied after rotation.
    """
    if not trim_expr:
        return img
    parts = trim_expr.split(":")
    if len(parts) != 4:
        return img
    try:
        x = int(parts[0])
        y = int(parts[1])
        w_raw, h_raw = parts[2], parts[3]
        img_h, img_w = img.shape[:2]

        def parse_size(val: str, total: int) -> int:
            val = val.strip()
            if val.endswith("%"):
                pct = float(val.strip("%"))
                return max(1, int(total * (pct / 100.0)))
            return int(val)

        w = parse_size(w_raw, img_w)
        h = parse_size(h_raw, img_h)
        x = max(0, x)
        y = max(0, y)
        w = min(w, img_w - x)
        h = min(h, img_h - y)
        return img[y : y + h, x : x + w]
    except Exception:
        return img


def stream_worker(
    name:           str,
    url:            str,
    target_w:       int,
    target_h:       int,
    rotation:       int,
    trim_expr:      str,
    slots:          Dict[str, np.ndarray],
    lock:           threading.Lock,
    stop_event:     threading.Event,
    max_failures:   int = 3,
    format_options: Optional[Dict[str, str]] = None,
) -> None:
    attempts = 0
    while not stop_event.is_set() and attempts < max_failures:
        try:
            with av.open(url, options=format_options or {}) as container:
                for frame in container.decode(video=0):
                    if stop_event.is_set():
                        break
                    img = frame.to_ndarray(format="bgr24")
                    if rotation:
                        k = (rotation // 90) % 4
                        if k:
                            img = np.rot90(img, k=k)
                    if trim_expr:
                        img = _apply_trim(img, trim_expr)
                    fitted = _fit_frame(img, target_w, target_h)
                    with lock:
                        slots[name] = fitted
        except Exception:
            attempts += 1
            continue

    if stop_event.is_set():
        return

    fail_img = _failure_frame(target_w, target_h, f"{url} failed")
    with lock:
        slots[name] = fail_img


def parse_ffmpeg_options(opt_list: Optional[Iterable[str]]) -> Dict[str, str]:
    """
    Parse key=value pairs into a dict for av.open(options=...).
    """
    opts: Dict[str, str] = {}
    if not opt_list:
        return opts
    for raw in opt_list:
        if "=" not in raw:
            raise ValueError(f"Invalid ffmpeg option '{raw}', expected key=value.")
        key, val = raw.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid ffmpeg option '{raw}', missing key.")
        opts[key] = val
    return opts


def parse_ffmpeg_arg_list(arg_list: Optional[Iterable[str]]) -> List[str]:
    """
    Flatten a list of ffmpeg args; each entry can contain multiple tokens.
    """
    import shlex

    if not arg_list:
        return []
    flattened: List[str] = []
    for raw in arg_list:
        flattened.extend(shlex.split(raw))
    return flattened


def start_rtp_writer(
    out_target: str,
    width: int,
    height: int,
    fps: int,
    encoder: str = "libx264",
    bitrate_kbps: Optional[int] = None,
    extra_args: Optional[Iterable[str]] = None,
    sdp_file: Optional[str] = None,
) -> subprocess.Popen:
    """
    Launch an ffmpeg process that consumes raw BGR frames on stdin and
    sends H.264 over RTP to the given target.
    """
    url = out_target if "://" in out_target else f"rtp://{out_target}"
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
    ]
    if bitrate_kbps:
        cmd.extend(["-b:v", f"{bitrate_kbps}k"])
    if sdp_file:
        cmd.extend(["-sdp_file", sdp_file])
    if extra_args:
        cmd.extend(extra_args)
    cmd.extend(["-f", "rtp", url])
    try:
        return subprocess.Popen(cmd, stdin=subprocess.PIPE)
    except FileNotFoundError as exc:
        raise RuntimeError("ffmpeg not found on PATH, required for RTP output.") from exc


def compositor_loop(
    df,
    backdrop_bgr: np.ndarray,
    slots:        Dict[str, np.ndarray],
    lock:         threading.Lock,
    stop_event:   threading.Event,
    window_name:  str = "Multiviewer",
    rtp_proc:     Optional[subprocess.Popen] = None,
    hls_proc:     Optional[subprocess.Popen] = None,
    show_window:  bool = True,
) -> None:
    height, width = backdrop_bgr.shape[:2]
    if show_window:
        try:
            cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)
            cv2.resizeWindow(window_name, width, height)
        except cv2.error as exc:
            msg = (
                "OpenCV build does not include GUI support (cvNamedWindow failed). "
                "Install the non-headless opencv-python package and ensure GUI libs "
                "like GTK/Qt are available, or run with --no-window."
            )
            raise RuntimeError(msg) from exc

    while not stop_event.is_set():
        frame = backdrop_bgr.copy()
        with lock:
            for row in df.iter_rows(named=True):
                name = row["channelName"]
                slot = slots.get(name)
                if slot is None:
                    continue
                x, y = int(row["x"]), int(row["y"])
                h, w, _ = slot.shape
                frame[y : y + h, x : x + w] = slot
                # Overlay channel name on a semi-transparent black bar at the bottom of the cell.
                label = str(name)
                if label:
                    bar_h = max(24, h // 12)
                    overlay = frame.copy()
                    cv2.rectangle(overlay, (x, y + h - bar_h), (x + w, y + h), (0, 0, 0), thickness=-1)
                    alpha = 0.8
                    frame[y : y + h, x : x + w] = cv2.addWeighted(
                        overlay[y : y + h, x : x + w], alpha,
                        frame[y : y + h, x : x + w], 1 - alpha,
                        0
                    )
                    cv2.putText(
                        frame,
                        label,
                        (x + 8, y + h - bar_h//3),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.6,
                        (255, 255, 255),
                        1,
                        cv2.LINE_AA,
                    )

        if rtp_proc and rtp_proc.stdin:
            try:
                rtp_proc.stdin.write(frame.tobytes())
                rtp_proc.stdin.flush()
            except BrokenPipeError:
                stop_event.set()
            except Exception:
                stop_event.set()
        if hls_proc and hls_proc.stdin:
            try:
                hls_proc.stdin.write(frame.tobytes())
                hls_proc.stdin.flush()
            except BrokenPipeError:
                stop_event.set()
            except Exception:
                stop_event.set()

        if show_window:
            cv2.imshow(window_name, frame)
            key = cv2.waitKey(1) & 0xFF
            if key in (ord("q"), 27):
                stop_event.set()
                break
        else:
            # Headless mode: small sleep to avoid busy loop.
            cv2.waitKey(1)

    if show_window:
        cv2.destroyAllWindows()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a live RTP multiviewer.")
    parser.add_argument("--registry", required=True, help="Path to the channel registry CSV.")
    parser.add_argument("--width",        type=int, default=1920, help="Screen width in pixels.")
    parser.add_argument("--height",       type=int, default=1080, help="Screen height in pixels.")
    parser.add_argument("--padding",      type=int, default=8,    help="Padding between cells in pixels.")
    parser.add_argument("--font-size",    type=int, default=28,   help="Font size for labels.")
    parser.add_argument("--font",         type=str, default=None, help="Optional TTF font path.")
    parser.add_argument("--max-failures", type=int, default=3,    help="Retries per stream before marking failed.")
    parser.add_argument( "--channel", action="append", dest="channels", default=None,
                        help="Channel name to include (can be specified multiple times). Default: all.")
    parser.add_argument( "--ffmpeg-opt", action="append", dest="ffmpeg_opts", default=None,
                        help="Extra ffmpeg input option key=value (e.g. rtpflags=send_bye). Can be repeated.")
    parser.add_argument( "--rtp-out", type=str, default=None,
                        help="RTP destination for the composed mosaic (e.g. 192.168.1.50:6910).")
    parser.add_argument( "--rtp-fps", type=int, default=30,
                        help="Framerate for RTP output when --rtp-out is set.")
    parser.add_argument( "--rtp-ffmpeg-arg", action="append", dest="rtp_ffmpeg_args", default=None,
                        help="Extra ffmpeg args for RTP output (passed verbatim), repeatable. "
                        "Quote groups to keep pairs together, e.g. --rtp-ffmpeg-arg \"-sdp_file mosaic.sdp\"")
    parser.add_argument( "--rtp-encoder", type=str, default="libx264",
                        help="Video encoder for RTP/HLS output (default: libx264).")
    parser.add_argument( "--rtp-bitrate-kbps", type=int, default=None,
                        help="Target video bitrate in kbps for RTP/HLS output.")
    parser.add_argument( "--rtp-sdp-file", type=str, default=None,
                        help="Write an SDP file for the RTP output (e.g., mosaic.sdp).")
    parser.add_argument( "--hls-dir", type=str, default=None,
                        help="Directory to write HLS output (index.m3u8, segments). If set, HLS is produced.")
    parser.add_argument( "--hls-segment-time", type=float, default=1.0,
                        help="HLS segment duration in seconds (default: 1.0).")
    parser.add_argument( "--hls-list-size", type=int, default=6,
                        help="Number of segments to keep in the HLS playlist (default: 6).")
    parser.add_argument( "--hls-bitrate-kbps", type=int, default=None,
                        help="Target video bitrate in kbps for HLS output (overrides rtp-bitrate-kbps if set).")
    parser.add_argument( "--no-window", action="store_true",
                        help="Run headless (no local window); useful for servers where only RTP output is needed.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = load_registry(args.registry)
    if args.channels:
        wanted = set(args.channels)
        df = df.filter(pl.col("channelName").is_in(list(wanted)))
        if df.is_empty():
            raise SystemExit("No matching channels found for selection.")
    df = apply_transforms(df)
    if {"row", "col"} <= set(df.columns):
        df = assign_grid_with_positions(df, args.width, args.height, padding=args.padding)
    else:
        df = assign_grid(df, args.width, args.height, padding=args.padding)
    ffmpeg_opts = parse_ffmpeg_options(args.ffmpeg_opts)

    # Build backdrop and shared slots.
    backdrop = create_placeholder_grid_image(
        df,
        args.width,
        args.height,
        font_size=args.font_size,
        font_path=args.font,
    )
    # Convert to BGR for OpenCV.
    backdrop_bgr = np.array(backdrop.convert("RGB"))[:, :, ::-1]

    slots: Dict[str, np.ndarray] = {}
    lock = threading.Lock()
    stop_event = threading.Event()
    rtp_proc: Optional[subprocess.Popen] = None
    hls_proc: Optional[subprocess.Popen] = None
    rtp_ffmpeg_args = parse_ffmpeg_arg_list(args.rtp_ffmpeg_args)
    if args.rtp_out:
        rtp_proc = start_rtp_writer(
            args.rtp_out,
            args.width,
            args.height,
            args.rtp_fps,
            encoder=args.rtp_encoder,
            bitrate_kbps=args.rtp_bitrate_kbps,
            extra_args=rtp_ffmpeg_args,
            sdp_file=args.rtp_sdp_file,
        )
    if args.hls_dir:
        hls_proc = start_hls_writer(
            Path(args.hls_dir),
            args.width,
            args.height,
            args.rtp_fps,
            segment_time=args.hls_segment_time,
            list_size=args.hls_list_size,
            encoder=args.rtp_encoder,
            bitrate_kbps=args.hls_bitrate_kbps or args.rtp_bitrate_kbps,
        )

    # Handle Ctrl+C cleanly.
    def _handle_sigint(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sigint)

    # Launch stream workers.
    threads = []
    for row in df.iter_rows(named=True):
        url = _rtp_url(str(row["ipAddress"]))
        if row.get("isEmpty") or not str(row["ipAddress"]).strip():
            continue
        t = threading.Thread(
            target=stream_worker,
            args=(
                str(row["channelName"]),
                url,
                int(row["w"]),
                int(row["h"]),
                int(row["rotation"]) if "rotation" in row else 0,
                str(row["trim"]) if "trim" in row else "",
                slots,
                lock,
                stop_event,
            ),
            kwargs={
                "max_failures": args.max_failures,
                "format_options": ffmpeg_opts
            },
            daemon=True,
        )
        t.start()
        threads.append(t)

    compositor_loop(
        df,
        backdrop_bgr,
        slots,
        lock,
        stop_event,
        rtp_proc=rtp_proc,
        hls_proc=hls_proc,
        show_window=not args.no_window,
    )
    stop_event.set()
    for t in threads:
        t.join(timeout=1.0)
    if rtp_proc:
        if rtp_proc.stdin:
            try:
                rtp_proc.stdin.close()
            except Exception:
                pass
        try:
            rtp_proc.terminate()
        except Exception:
            pass
    if hls_proc:
        if hls_proc.stdin:
            try:
                hls_proc.stdin.close()
            except Exception:
                pass
        try:
            hls_proc.terminate()
        except Exception:
            pass


if __name__ == "__main__":
    main()
