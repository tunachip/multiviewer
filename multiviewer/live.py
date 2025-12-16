from __future__ import annotations

import argparse
import signal
import threading
from pathlib import Path
from typing import Dict

import av
import cv2
import numpy as np

from .layout import assign_grid
from .registry import load_registry
from .render import create_placeholder_grid_image


def _rtp_url(ip_address: str) -> str:
    if "://" in ip_address:
        return ip_address
    # If no explicit port is provided, default to 6000.
    host, sep, maybe_port = ip_address.rpartition(":")
    has_port = sep == ":" and maybe_port.isdigit()
    if has_port:
        return f"rtp://{ip_address}"
    return f"rtp://{ip_address}:6000"


def _fit_frame(frame: np.ndarray, target_w: int, target_h: int) -> np.ndarray:
    """Resize frame to fit inside target while preserving aspect via letterboxing."""
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
    """Create a red failure overlay frame."""
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


def stream_worker(
    name: str,
    url: str,
    target_w: int,
    target_h: int,
    slots: Dict[str, np.ndarray],
    lock: threading.Lock,
    stop_event: threading.Event,
    max_failures: int = 3,
) -> None:
    attempts = 0
    while not stop_event.is_set() and attempts < max_failures:
        try:
            with av.open(url) as container:
                for frame in container.decode(video=0):
                    if stop_event.is_set():
                        break
                    img = frame.to_ndarray(format="bgr24")
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


def compositor_loop(
    df,
    backdrop_bgr: np.ndarray,
    slots: Dict[str, np.ndarray],
    lock: threading.Lock,
    stop_event: threading.Event,
    window_name: str = "Multiviewer",
) -> None:
    cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)
    height, width = backdrop_bgr.shape[:2]
    cv2.resizeWindow(window_name, width, height)

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

        cv2.imshow(window_name, frame)
        key = cv2.waitKey(1) & 0xFF
        if key in (ord("q"), 27):
            stop_event.set()
            break

    cv2.destroyAllWindows()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a live RTP multiviewer.")
    parser.add_argument("--registry", required=True, help="Path to the channel registry CSV.")
    parser.add_argument("--width", type=int, default=1920, help="Screen width in pixels.")
    parser.add_argument("--height", type=int, default=1080, help="Screen height in pixels.")
    parser.add_argument("--padding", type=int, default=8, help="Padding between cells in pixels.")
    parser.add_argument("--font-size", type=int, default=28, help="Font size for labels.")
    parser.add_argument("--font", type=str, default=None, help="Optional TTF font path.")
    parser.add_argument("--max-failures", type=int, default=3, help="Retries per stream before marking failed.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = assign_grid(load_registry(args.registry), args.width, args.height, padding=args.padding)

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

    # Handle Ctrl+C cleanly.
    def _handle_sigint(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sigint)

    # Launch stream workers.
    threads = []
    for row in df.iter_rows(named=True):
        url = _rtp_url(str(row["ipAddress"]))
        t = threading.Thread(
            target=stream_worker,
            args=(
                str(row["channelName"]),
                url,
                int(row["w"]),
                int(row["h"]),
                slots,
                lock,
                stop_event,
            ),
            kwargs={"max_failures": args.max_failures},
            daemon=True,
        )
        t.start()
        threads.append(t)

    compositor_loop(df, backdrop_bgr, slots, lock, stop_event)
    stop_event.set()
    for t in threads:
        t.join(timeout=1.0)


if __name__ == "__main__":
    main()
