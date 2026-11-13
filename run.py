#!/usr/bin/env python3
"""Entry point: python3 run.py [--host 127.0.0.1] [--port 8000] [--data-dir ./data]."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.server import main

if __name__ == "__main__":
    main()
