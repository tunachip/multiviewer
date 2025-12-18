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
    df = df.with_columns(
        [
            pl.when(pl.col("rotation").is_null()).then(0).otherwise(pl.col("rotation")).alias("rotation")
            if "rotation" in df.columns
            else pl.lit(0).alias("rotation"),
            pl.when(pl.col("trim").is_null()).then("").otherwise(pl.col("trim")).alias("trim")
            if "trim" in df.columns
            else pl.lit("").alias("trim"),
        ]
    )

    return df.select(
        [
            pl.col("channelName").cast(pl.Utf8),
            pl.col("direction").cast(pl.Utf8),
            pl.col("ipAddress").cast(pl.Utf8),
            pl.col("rotation").cast(pl.Int64),
            pl.col("trim").cast(pl.Utf8),
            pl.all().exclude(list(REQUIRED_COLUMNS | {"rotation", "trim"})),
        ]
    )
