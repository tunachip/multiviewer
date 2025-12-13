from __future__ import annotations

import argparse
from pathlib import Path

from .layout import assign_grid
from .registry import load_registry
from .render import render_placeholder_grid


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a placeholder multiviewer mosaic.")
    parser.add_argument("--registry", required=True,
                        help="Path to the channel registry CSV.")
    parser.add_argument("--width", type=int, default=1920,
                        help="Screen width in pixels.")
    parser.add_argument("--height", type=int, default=1080,
                        help="Screen height in pixels.")
    parser.add_argument("--padding", type=int, default=8,
                        help="Padding between cells in pixels.")
    parser.add_argument("--font", type=str, default=None,
                        help="Optional path to a TTF font for labels.")
    parser.add_argument("--font-size", type=int, default=28,
                        help="Font size for channel labels and placeholders.")
    parser.add_argument("--output", type=Path, default=Path("mosaic.png"),
                        help="Where to write the placeholder mosaic PNG.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = load_registry(args.registry)
    laid_out = assign_grid(df, args.width, args.height, padding=args.padding)
    output_path = render_placeholder_grid(
        laid_out,
        args.width,
        args.height,
        args.output,
        font_size=args.font_size,
        font_path=args.font,
    )
    print(f"Placeholder mosaic written to {output_path}")


if __name__ == "__main__":
    main()
