
import os
import sys
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.data.store import StorageEngine

def check_status(symbol="GBPJPY"):
    store = StorageEngine()
    store.connect()
    lib = store.get_library('forex_1m')
    
    if not lib.has_symbol(symbol):
        print(f"Symbol {symbol} NOT found in library.")
        return

    # specific read
    try:
        # Read metadata or tail?
        # ArcticDB efficient read
        df = lib.read(symbol).data
        if df.empty:
            print(f"Symbol {symbol} found but EMPTY.")
        else:
            print(f"Symbol {symbol}: {len(df)} rows.")
            print(f"Start: {df.index.min()}")
            print(f"End:   {df.index.max()}")
            
            # Check 2025
            count_2025 = len(df[df.index.year == 2025])
            print(f"Rows in 2025: {count_2025}")
            
    except Exception as e:
        print(f"Error reading {symbol}: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        check_status(sys.argv[1])
    else:
        check_status()
