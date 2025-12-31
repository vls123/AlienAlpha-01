"""
Anomaly Detector to flag Black Swan events.
"""
import logging
import pandas as pd
from src.data.store import StorageEngine

logger = logging.getLogger(__name__)

class AnomalyDetector:
    def __init__(self, storage: StorageEngine):
        self.storage = storage

    def scan_for_spikes(self, df: pd.DataFrame, threshold_pct: float = 0.10) -> pd.DataFrame:
        """
        Scans DataFrame for price changes > threshold_pct in a single bar.
        Returns a DataFrame of anomalous bars with metadata.
        """
        df = df.copy()
        # Calculate percentage change on 'close' typically, or High/Low range
        # Assuming OHLCV data
        if 'close' not in df.columns or 'open' not in df.columns:
             logger.warning("DataFrame missing 'open' or 'close' columns")
             return pd.DataFrame()

        # Check for single-bar massive moves (Open vs Close, or High/Low range if preferred)
        # Using High/Low range vs Open might be safer but here we check Close vs Open or Close vs Prev Close
        # Request said "spikes > 10% in a single bar". Let's assume (High - Low) / Open > 0.10 or abs(Close - Open) / Open
        
        # Method 1: Intra-bar volatility
        df['pct_move'] = (df['high'] - df['low']) / df['open']
        anomalies = df[df['pct_move'] > threshold_pct].copy()
        
        if not anomalies.empty:
            logger.info(f"Detected {len(anomalies)} black swan candidates.")
            # Tag metadata
            anomalies['event_type'] = 'Black Swan'
            anomalies['description'] = f'Spike > {threshold_pct*100}%'
            
        return anomalies

    def flag_anomalies(self, library: str, symbol: str):
        """
        Reads data, detects anomalies, and could write back metadata.
        """
        try:
            lib = self.storage.get_library(library)
            df = lib.read(symbol).data
            anomalies = self.scan_for_spikes(df)
            if not anomalies.empty:
                # In real app, we update metadata store or write to a separate 'events' collection
                pass
        except Exception as e:
            logger.error(f"Error flagging anomalies for {symbol}: {e}")
