import sys

def verify_imports():
    required_libraries = [
        "vectorbt",
        "nautilus_trader",
        "pandas_ta",
        "arcticdb"
    ]
    
    missing = []
    for lib in required_libraries:
        try:
            __import__(lib)
            print(f"[OK] {lib} imported successfully")
        except ImportError:
            print(f"[FAIL] {lib} not found")
            missing.append(lib)
            
    if missing:
        print(f"\nMissing libraries: {', '.join(missing)}")
        sys.exit(1)
    else:
        print("\nAll core libraries verified.")
        sys.exit(0)

if __name__ == "__main__":
    verify_imports()
