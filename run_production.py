#!/usr/bin/env python3
"""
Run PyRQG in production mode for billion-scale query generation.

Example usage:
    # Test run with 10k queries
    python run_production.py --config configs/test.conf
    
    # Production run with 1 billion queries
    python run_production.py --config configs/billion_scale.conf
    
    # Override query count
    python run_production.py --config configs/test.conf --count 100000
    
    # Save queries to file
    python run_production.py --config configs/test.conf --output queries.sql
    
    # Resume from checkpoint
    python run_production.py --config configs/billion_scale.conf --checkpoint output/billion_scale/checkpoint.json
"""

import sys
from pathlib import Path

# Add to Python path
sys.path.insert(0, str(Path(__file__).parent))

from pyrqg.production.production_rqg import main

if __name__ == "__main__":
    main()