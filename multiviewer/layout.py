from __future__ import annotations

import math
from typing import Tuple

import polars as pl


def compute_grid_dimensions(count: int) -> Tuple[int, int]:
    """
    Compute a near-square grid that can fit `count` cells.
    Returns (rows, cols).
    """
    if count <= 0:
        return (0, 0)
    cols = math.ceil(math.sqrt(count))
    rows = math.ceil(count / cols)
    return rows, cols


def assign_grid(
    df: pl.DataFrame,
    screen_width:  int,
    screen_height: int,
    padding:       int = 8,
) -> pl.DataFrame:
    """
    Add X, Y, W, H (and row/col indices) to each channel for layout on the canvas.
    Padding inserts space between cells and around the border.
    """
    count = df.height
    if count == 0:
        return df

    rows, cols   = compute_grid_dimensions(count)
    avail_width  = max(1, screen_width  - padding * (cols + 1))
    avail_height = max(1, screen_height - padding * (rows + 1))
    cell_w       = max(1, avail_width  // cols)
    cell_h       = max(1, avail_height // rows)

    return (df.with_columns(pl.arange(0, pl.count()).alias("_idx"),)
            .with_columns((pl.col("_idx") // cols).alias("row"),
                          (pl.col("_idx") % cols).alias("col"),
                          pl.lit(cell_w).alias("w"),
                          pl.lit(cell_h).alias("h"))
            .with_columns((padding + pl.col("col") * (cell_w + padding)).alias("x"),
                          (padding + pl.col("row") * (cell_h + padding)).alias("y"))
            .drop("_idx"))

