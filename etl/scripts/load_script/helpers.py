import re
import logging
import pandas as pd
from typing import Any, Optional, Tuple
from airflow.providers.mysql.hooks.mysql import MySqlHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# Helpers
# ============================================================


def get_column_max_length(
        hook: MySqlHook,
        table: str,
        column: str) -> Optional[int]:
    try:
        row = hook.get_first(
            """
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            """,
            parameters=(table, column),
        )
        if row and row[0]:
            return int(row[0])
    except Exception:
        pass
    return None


def get_staging_country_limits(hook: MySqlHook) -> Tuple[int, int]:
    default_len = 30
    o_len = get_column_max_length(
        hook,
        "stg_trips_summary",
        "origin_country") or default_len
    d_len = get_column_max_length(
        hook,
        "stg_trips_summary",
        "destination_country") or default_len
    logger.info(
        f"Staging country column widths -> origin_country:{o_len}, destination_country:{d_len}")
    return o_len, d_len


def sanitize_country_for_staging(
        value: Any,
        max_len: int,
        field_name: str) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if s == "":
        return None

    up = s.upper()

    if up in {"UNKNOWN", "UNKN", "UNK", "NA", "N/A", "NONE", "NULL"}:
        return None

    if re.match(
        r"^\d{4}-\d{2}-\d{2}",
        up) or re.match(
        r"^\d{2}/\d{2}/\d{4}",
            up):
        logger.warning(
            f"[country] Date-like detected in {field_name}: '{s}' -> NULL")
        return None

    up = re.sub(r"[^A-Z]", "", up)
    if up == "":
        return None

    if len(up) > max_len:
        logger.warning(
            f"[country] Too long for {field_name} (len={len(up)}, max={max_len}) -> truncated: '{up[:max_len]}'")
        up = up[:max_len]

    return up
