from __future__ import annotations

from pathlib import Path
from typing import Tuple

import polars as pl
from PIL import Image, ImageDraw, ImageFont

Color = Tuple[int, int, int]

LIGHT_GREY:        Color = (68, 68, 68)
DARK_GREY:         Color = (48, 48, 48)
GRID_OUTLINE:      Color = (110, 110, 110)
TEXT_COLOR:        Color = (230, 230, 230)
PLACEHOLDER_COLOR: Color = (247, 227, 101)


def _load_font(
    font_size: int,
    font_path: str | Path | None = None,
) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """
    Try a few common fonts (including the DejaVu font shipped with Pillow) before
    falling back to the default.
    """
    candidates: list[str] = []
    if font_path:
        candidates.append(str(font_path))
    candidates.extend(
        [
            "DejaVuSans.ttf",  # bundled with Pillow
            "Arial.ttf",
            "LiberationSans-Regular.ttf",
        ]
    )
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, font_size)
        except OSError:
            continue
    return ImageFont.load_default()


def create_placeholder_grid_image(
    df: pl.DataFrame,
    screen_width: int,
    screen_height: int,
    font_size: int = 28,
    font_path: str | Path | None = None,
) -> Image.Image:
    """
    Create a checkered grid with channel names and placeholder '...' per cell.
    Returns a PIL Image to be used as a backdrop/canvas.
    """
    if df.height == 0:
        raise ValueError("Cannot render an empty registry.")

    font  = _load_font(font_size, font_path=font_path)
    image = Image.new("RGB", (screen_width, screen_height), DARK_GREY)
    draw  = ImageDraw.Draw(image)

    for row in df.iter_rows(named=True):
        x, y, w, h = int(row["x"]), int(row["y"]), int(row["w"]), int(row["h"])
        row_idx, col_idx = int(row["row"]), int(row["col"])
        channel_name = str(row["channelName"])

        fill = LIGHT_GREY if (row_idx + col_idx) % 2 == 0 else DARK_GREY
        draw.rectangle([x, y, x + w - 1, y + h - 1], fill=fill, outline=GRID_OUTLINE)

        # Channel name near the top.
        name_bbox = draw.textbbox((0, 0), channel_name, font=font)
        name_x = x + (w - (name_bbox[2] - name_bbox[0])) // 2
        name_y = y + 10
        draw.text((name_x, name_y), channel_name, font=font, fill=TEXT_COLOR)

        # Placeholder '...' centered.
        placeholder = "..."
        ph_bbox = draw.textbbox((0, 0), placeholder, font=font)
        ph_x = x + (w - (ph_bbox[2] - ph_bbox[0])) // 2
        ph_y = y + (h - (ph_bbox[3] - ph_bbox[1])) // 2
        draw.text((ph_x, ph_y), placeholder, font=font, fill=PLACEHOLDER_COLOR)

    return image


def render_placeholder_grid(
    df: pl.DataFrame,
    screen_width:  int,
    screen_height: int,
    output_path:   str | Path,
    font_size:     int = 28,
    font_path:     str | Path | None = None,
) -> Path:
    """
    Render and persist a placeholder grid PNG to disk.
    """
    image = create_placeholder_grid_image(
        df,
        screen_width,
        screen_height,
        font_size=font_size,
        font_path=font_path,
    )
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    image.save(output)
    return output

