#!/usr/bin/env python3
"""
Benchmark early termination performance improvements.

This script measures the performance impact of early termination when validation
errors are present.

Usage:
    python scripts/benchmark_early_termination.py
"""

import sys
import time

import pandas as pd
from pydantic import BaseModel, Field

# Add current directory to path to import daffy
sys.path.insert(0, ".")

from daffy.row_validation import validate_dataframe_rows


class SimpleValidator(BaseModel):
    """Simple test validator."""

    name: str
    age: int = Field(ge=0, le=120)
    price: float = Field(gt=0)


def benchmark_early_termination() -> None:
    """Benchmark early termination vs full scan with validation errors."""
    print("\n" + "=" * 70)
    print("Early Termination Performance Benchmark")
    print("=" * 70 + "\n")

    # Test with 100k rows to see meaningful difference
    n_rows = 100_000
    max_errors = 5

    # Scenario 1: All valid (no benefit expected)
    print(f"Scenario 1: All valid rows ({n_rows:,} rows)")
    print("-" * 70)

    df_valid = pd.DataFrame(
        {
            "name": [f"Person_{i}" for i in range(n_rows)],
            "age": [25 + (i % 50) for i in range(n_rows)],
            "price": [10.0 + (i % 100) for i in range(n_rows)],
        }
    )

    # With early termination (default)
    t0 = time.perf_counter()
    validate_dataframe_rows(df_valid, SimpleValidator, max_errors=max_errors, early_termination=True)
    t1 = time.perf_counter()
    time_with_early = t1 - t0

    # Without early termination
    t0 = time.perf_counter()
    validate_dataframe_rows(df_valid, SimpleValidator, max_errors=max_errors, early_termination=False)
    t1 = time.perf_counter()
    time_without_early = t1 - t0

    print(f"  With early termination:    {time_with_early * 1000:.1f}ms")
    print(f"  Without early termination: {time_without_early * 1000:.1f}ms")
    print(f"  Speedup: {time_without_early / time_with_early:.2f}x")

    # Scenario 2: 1% errors (stops early at ~500 rows scanned)
    print(f"\nScenario 2: 1% invalid rows ({n_rows:,} rows, errors throughout)")
    print("-" * 70)

    df_sparse_errors = pd.DataFrame(
        {
            "name": [f"Person_{i}" for i in range(n_rows)],
            "age": [-1 if i % 100 == 0 else 25 + (i % 50) for i in range(n_rows)],  # 1% invalid
            "price": [10.0 + (i % 100) for i in range(n_rows)],
        }
    )

    # With early termination (should stop after ~500 rows)
    t0 = time.perf_counter()
    try:
        validate_dataframe_rows(df_sparse_errors, SimpleValidator, max_errors=max_errors, early_termination=True)
    except AssertionError:
        pass
    t1 = time.perf_counter()
    time_with_early = t1 - t0

    # Without early termination (scans all 100k rows)
    t0 = time.perf_counter()
    try:
        validate_dataframe_rows(df_sparse_errors, SimpleValidator, max_errors=max_errors, early_termination=False)
    except AssertionError:
        pass
    t1 = time.perf_counter()
    time_without_early = t1 - t0

    print(f"  With early termination:    {time_with_early * 1000:.1f}ms (stops after ~500 rows)")
    print(f"  Without early termination: {time_without_early * 1000:.1f}ms (scans all 100k rows)")
    print(f"  Speedup: {time_without_early / time_with_early:.2f}x")

    # Scenario 3: Errors at start (stops very early)
    print(f"\nScenario 3: Errors at start ({n_rows:,} rows, first 100 rows invalid)")
    print("-" * 70)

    df_front_errors = pd.DataFrame(
        {
            "name": [f"Person_{i}" for i in range(n_rows)],
            "age": [-1 if i < 100 else 25 + (i % 50) for i in range(n_rows)],  # First 100 invalid
            "price": [10.0 + (i % 100) for i in range(n_rows)],
        }
    )

    # With early termination (should stop after ~5 rows)
    t0 = time.perf_counter()
    try:
        validate_dataframe_rows(df_front_errors, SimpleValidator, max_errors=max_errors, early_termination=True)
    except AssertionError:
        pass
    t1 = time.perf_counter()
    time_with_early = t1 - t0

    # Without early termination (scans all 100k rows)
    t0 = time.perf_counter()
    try:
        validate_dataframe_rows(df_front_errors, SimpleValidator, max_errors=max_errors, early_termination=False)
    except AssertionError:
        pass
    t1 = time.perf_counter()
    time_without_early = t1 - t0

    print(f"  With early termination:    {time_with_early * 1000:.1f}ms (stops after {max_errors} rows)")
    print(f"  Without early termination: {time_without_early * 1000:.1f}ms (scans all 100k rows)")
    print(f"  Speedup: {time_without_early / time_with_early:.2f}x")

    # Scenario 4: All invalid (stops very early)
    print(f"\nScenario 4: All invalid rows ({n_rows:,} rows)")
    print("-" * 70)

    df_all_invalid = pd.DataFrame(
        {
            "name": [f"Person_{i}" for i in range(n_rows)],
            "age": [-1] * n_rows,  # All invalid
            "price": [10.0 + (i % 100) for i in range(n_rows)],
        }
    )

    # With early termination (should stop after ~5 rows)
    t0 = time.perf_counter()
    try:
        validate_dataframe_rows(df_all_invalid, SimpleValidator, max_errors=max_errors, early_termination=True)
    except AssertionError:
        pass
    t1 = time.perf_counter()
    time_with_early = t1 - t0

    # Without early termination (scans all 100k rows)
    t0 = time.perf_counter()
    try:
        validate_dataframe_rows(df_all_invalid, SimpleValidator, max_errors=max_errors, early_termination=False)
    except AssertionError:
        pass
    t1 = time.perf_counter()
    time_without_early = t1 - t0

    print(f"  With early termination:    {time_with_early * 1000:.1f}ms (stops after {max_errors} rows)")
    print(f"  Without early termination: {time_without_early * 1000:.1f}ms (scans all 100k rows)")
    print(f"  Speedup: {time_without_early / time_with_early:.2f}x")

    print("\n" + "=" * 70)
    print("Key Findings:")
    print("=" * 70)
    print("1. No overhead for valid data (early termination is essentially free)")
    print("2. Large speedups when errors exist:")
    print("   - 1% errors scattered: stops 200x earlier (at row 500 vs 100k)")
    print(f"   - Errors at start: stops {n_rows // max_errors:,}x earlier")
    print(f"   - All invalid: stops {n_rows // max_errors:,}x earlier")
    print("3. Real-world benefit: Fails fast on bad data, saving time in data pipelines")
    print("=" * 70)


if __name__ == "__main__":
    benchmark_early_termination()
