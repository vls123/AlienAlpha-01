"""
Time utility module for enforcing UTC usage across the system.
"""
from datetime import datetime, timezone
import pandas as pd

def now_utc() -> datetime:
    """Returns the current aware datetime in UTC."""
    return datetime.now(timezone.utc)

def to_utc(dt: datetime) -> datetime:
    """Converts a datetime object to an aware UTC datetime."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def ensure_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures the DataFrame index is timezone-aware UTC.
    Assumes the index is a DatetimeIndex.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("DataFrame index must be a DatetimeIndex")
        
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return df
