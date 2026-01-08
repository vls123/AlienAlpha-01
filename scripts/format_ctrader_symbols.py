
import json
import os

def format_symbols():
    json_path = 'ctrader_symbols_dump.json'
    output_path = '/home/vls/.gemini/antigravity/brain/70034409-9462-4871-96eb-e46ef3dd0adf/ctrader_available_tickers.md'
    
    if not os.path.exists(json_path):
        print(f"Error: {json_path} not found.")
        return

    with open(json_path, 'r') as f:
        data = json.load(f)

    # Dedup and Sort
    # Data is list of {'id': ..., 'name': ...}
    # Might contain duplicates if I appended?
    # No, it was a single list.
    
    unique_symbols = sorted(data, key=lambda x: x['name'])
    
    with open(output_path, 'w') as f:
        f.write("# Available CTrader Tickers\n\n")
        f.write(f"Total Count: {len(unique_symbols)}\n\n")
        f.write("| Symbol Name | Symbol ID |\n")
        f.write("|-------------|-----------|\n")
        for sym in unique_symbols:
            f.write(f"| {sym['name']} | {sym['id']} |\n")
            
    print(f"Written to {output_path}")

if __name__ == "__main__":
    format_symbols()
