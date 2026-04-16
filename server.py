"""
Entry-point shim — keeps `python server.py` working for local dev.
The actual application lives in src/app.py (packaged with the wheel).
"""
from src.app import main

if __name__ == "__main__":
    main()
