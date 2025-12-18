from __future__ import annotations

import argparse
import subprocess
import sys
import tkinter as tk
#from pathlib import Path
from tkinter import messagebox
from typing import List

from .registry import load_registry


def fuzzy_subsequence(pattern: str, text: str) -> bool:
    """
    Simple fuzzy match: all pattern chars must appear in order inside text.
    """
    if not pattern:
        return True
    pattern = pattern.lower()
    text = text.lower()
    pos = 0
    for ch in pattern:
        idx = text.find(ch, pos)
        if idx == -1:
            return False
        pos = idx + 1
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Select channels and launch live multiviewer.")
    parser.add_argument("--registry", required=True, help="Path to the channel registry CSV.")
    parser.add_argument("--width",        type=int, default=1920, help="Screen width in pixels.")
    parser.add_argument("--height",       type=int, default=1080, help="Screen height in pixels.")
    parser.add_argument("--padding",      type=int, default=8,    help="Padding between cells in pixels.")
    parser.add_argument("--font-size",    type=int, default=28,   help="Font size for labels.")
    parser.add_argument("--font",         type=str, default=None, help="Optional TTF font path.")
    parser.add_argument("--max-failures", type=int, default=3,    help="Retries per stream before marking failed.")
    return parser.parse_args()


def build_command(args: argparse.Namespace, channels: List[str]) -> list[str]:
    cmd = [
        sys.executable,
        "-m", "multiviewer.live",
        "--registry",     args.registry,
        "--width",        str(args.width),
        "--height",       str(args.height),
        "--padding",      str(args.padding),
        "--font-size",    str(args.font_size),
        "--max-failures", str(args.max_failures),
    ]
    if args.font:
        cmd.extend(["--font", args.font])
    for ch in channels:
        cmd.extend(["--channel", ch])
    return cmd


def main() -> None:
    args = parse_args()
    df = load_registry(args.registry)
    channels = df.get_column("channelName").to_list()

    root = tk.Tk()
    root.title("Select Channels")

    tk.Label(root, text="Search:").pack(anchor="w", padx=8, pady=(8, 0))
    search_var = tk.StringVar()
    entry = tk.Entry(root, textvariable=search_var, width=40)
    entry.pack(fill="x", padx=8, pady=4)

    listbox = tk.Listbox(root, selectmode=tk.EXTENDED, width=50, height=15)
    scrollbar = tk.Scrollbar(root, orient="vertical", command=listbox.yview)
    listbox.config(yscrollcommand=scrollbar.set)
    listbox.pack(side="left", fill="both", expand=True, padx=(8, 0), pady=8)
    scrollbar.pack(side="right", fill="y", padx=(0, 8), pady=8)

    def refresh_list(*_):
        pattern = search_var.get().strip()
        listbox.delete(0, tk.END)
        for ch in channels:
            if fuzzy_subsequence(pattern, ch):
                listbox.insert(tk.END, ch)

    def launch_selected():
        selected = [listbox.get(i) for i in listbox.curselection()]
        if not selected:
            messagebox.showerror("No selection", "Select at least one channel to launch.")
            return
        cmd = build_command(args, selected)
        try:
            subprocess.Popen(cmd)
        except Exception as exc:
            messagebox.showerror("Launch failed", f"Could not start multiviewer:\n{exc}")
            return
        root.destroy()

    refresh_list()

    button_frame = tk.Frame(root)
    button_frame.pack(fill="x", padx=8, pady=(0, 8))
    tk.Button(button_frame, text="Launch Selected", command=launch_selected).pack(side="left")
    tk.Button(button_frame, text="Close", command=root.destroy).pack(side="right")

    entry.bind("<KeyRelease>", refresh_list)
    listbox.bind("<Double-Button-1>", lambda e: launch_selected())

    entry.focus_set()
    root.mainloop()


if __name__ == "__main__":
    main()

