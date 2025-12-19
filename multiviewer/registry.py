from __future__ import annotations

from pathlib import Path

import polars as pl

REQUIRED_COLUMNS = {"channelName", "direction", "ipAddress"}


def load_registry(csv_path: str | Path) -> pl.DataFrame:
    """
    Load and validate the channel registry from a CSV file into a Polars DataFrame.
    Expected columns: channelName, direction, ipAddress
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Registry file not found: {path}")

    df = pl.read_csv(path)
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Registry is missing required columns: {sorted(missing)}")

    # Normalize column order and types to keep downstream code predictable.
    # Normalize rotation/trim if present; otherwise add defaults.
    if "rotation" in df.columns:
        df = df.with_columns(
            pl.col("rotation")
            .cast(pl.Float64, strict=False)
            .fill_null(0)
            .fill_nan(0)
            .alias("rotation")
        )
    else:
        df = df.with_columns(pl.lit(0).alias("rotation"))

    if "trim" in df.columns:
        df = df.with_columns(pl.col("trim").fill_null("").alias("trim"))
    else:
        df = df.with_columns(pl.lit("").alias("trim"))

    # Optional programId, width/height/fps metadata for fan-out and encoding hints.
    if "programId" in df.columns:
        df = df.with_columns(pl.col("programId").cast(pl.Int64, strict=False))
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Int64).alias("programId"))
    if "videoWidth" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Int64).alias("videoWidth"))
    if "videoHeight" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Int64).alias("videoHeight"))
    if "fps" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("fps"))

    return df.select(
        [
            pl.col("channelName").cast(pl.Utf8),
            pl.col("direction").cast(pl.Utf8),
            pl.col("ipAddress").cast(pl.Utf8),
            pl.col("rotation").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("trim").cast(pl.Utf8),
            pl.col("programId"),
            pl.col("videoWidth"),
            pl.col("videoHeight"),
            pl.col("fps"),
            pl.all().exclude(list(REQUIRED_COLUMNS | {"rotation", "trim"})),
        ]
    )
