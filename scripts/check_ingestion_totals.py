
import os
import sys
import pandas as pd
from datetime import datetime
import pytz

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.data.store import StorageEngine

def check_totals():
    store = StorageEngine()
    store.connect()
    lib = store.get_library('forex_1m')
    
    symbols = lib.list_symbols()
    print(f"Total Symbols in Library: {len(symbols)}")
    
    total_rows = 0
    symbols_with_new_data = 0
    new_data_cutoff = datetime(2025, 1, 1, tzinfo=pytz.UTC)
    
    print(f"{'Symbol':<15} | {'Rows':<10} | {'End Date':<30} | {'New Data?'}")
    print("-" * 75)
    
    for sym in symbols:
        try:
            # Efficiently read metadata (or just index info if possible)
            # lib.read works, but reading full DF is slow if large.
            # ArcticDB usually has .head() or metadata?
            # Using .read() for now, optimized later.
            # Note: ArcticDB read returns a VersionedItem, .data is the DF.
            # We can use `date_range` if available in metadata, but reading `data` is definitive.
            
            # To avoid loading all data, we can try to read just the tail?
            # lib.read(symbol, date_range=...)
            
            # Let's read the index info / tail.
            # actually lib.read(sym).metadata might not be enough.
            # reading full dataframe index is fast enough for 50 symbols x 5M rows? 
            # 5M rows index is lightweight.
            
            item = lib.read(sym)
            df = item.data
            
            if df.empty:
                print(f"{sym:<15} | {'0':<10} | {'N/A':<30} | NO")
                continue
                
            count = len(df)
            total_rows += count
            
            end_date = df.index.max()
            # Ensure end_date is tz-aware for comparison (ArcticDB usually UTC)
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=pytz.UTC)
                
            has_new = end_date >= new_data_cutoff
            
            if has_new:
                symbols_with_new_data += 1
                
            print(f"{sym:<15} | {count:<10} | {str(end_date):<30} | {'YES' if has_new else 'NO'}")
            
        except Exception as e:
            print(f"{sym:<15} | ERROR: {e}")

    print("-" * 75)
    print(f"Total Rows Ingested: {total_rows}")
    print(f"Symbols with 2025+ Data: {symbols_with_new_data}/{len(symbols)}")

if __name__ == "__main__":
    check_totals()
