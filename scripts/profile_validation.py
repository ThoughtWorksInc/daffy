#!/usr/bin/env python3
"""
Profile row validation to identify performance bottlenecks.

This script measures the time spent in each step of the validation process
to identify where optimizations will have the most impact.

Usage:
    python scripts/profile_validation.py --size 10000
    python scripts/profile_validation.py --size 100000 --detailed
"""

import argparse
import math
import sys
import time
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field, TypeAdapter

# Add current directory to path to import daffy
sys.path.insert(0, ".")

from daffy.row_validation import validate_dataframe_rows


class SimpleValidator(BaseModel):
    """Simple test validator."""

    name: str
    age: int = Field(ge=0, le=120)
    price: float = Field(gt=0)


def _convert_nan_to_none(row_dict: dict[str, Any]) -> dict[str, Any]:
    """Convert NaN values to None for Pydantic compatibility."""
    return {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row_dict.items()}


def profile_validation_steps(n_rows: int, detailed: bool = False) -> dict[str, float]:
    """Profile each step of the validation process."""
    print(f"\n{'=' * 70}")
    print(f"Profiling validation with {n_rows:,} rows")
    print(f"{'=' * 70}\n")

    # Generate test data
    df = pd.DataFrame(
        {
            "name": [f"Person_{i}" for i in range(n_rows)],
            "age": [25 + (i % 50) for i in range(n_rows)],
            "price": [10.0 + (i % 100) for i in range(n_rows)],
        }
    )

    results = {}

    # Step 1: DataFrame to dict conversion
    print("Step 1: DataFrame → dict conversion")
    t0 = time.perf_counter()
    records = df.to_dict("records")
    t1 = time.perf_counter()
    step1_time = t1 - t0
    results["to_dict_records"] = step1_time
    print(f"  to_dict('records'): {step1_time * 1000:.1f}ms")

    if detailed:
        # Compare with other conversion methods
        t0 = time.perf_counter()
        _ = df.to_dict("index")
        t1 = time.perf_counter()
        results["to_dict_index"] = t1 - t0
        print(f"  to_dict('index'):   {(t1 - t0) * 1000:.1f}ms")

        # itertuples approach
        t0 = time.perf_counter()
        columns = df.columns.tolist()
        _ = [dict(zip(columns, row)) for row in df.itertuples(index=False, name=None)]
        t1 = time.perf_counter()
        results["itertuples"] = t1 - t0
        print(f"  itertuples + zip:   {(t1 - t0) * 1000:.1f}ms")

        # NumPy approach
        t0 = time.perf_counter()
        columns = df.columns.tolist()
        values = df.to_numpy()
        _ = [dict(zip(columns, row)) for row in values]
        t1 = time.perf_counter()
        results["numpy"] = t1 - t0
        print(f"  NumPy + zip:        {(t1 - t0) * 1000:.1f}ms")

    # Step 2: NaN conversion
    print("\nStep 2: NaN conversion")
    t0 = time.perf_counter()
    records_clean = [_convert_nan_to_none(r) for r in records]
    t1 = time.perf_counter()
    step2_time = t1 - t0
    results["nan_conversion"] = step2_time
    print(f"  List comprehension: {step2_time * 1000:.1f}ms")

    if detailed:
        # Test fillna approach (only if there are NaN values)
        import numpy as np

        df_with_nan = df.copy()
        df_with_nan.iloc[::100, 2] = np.nan  # Add some NaN values
        t0 = time.perf_counter()
        _ = df_with_nan.where(pd.notna(df_with_nan), None)
        t1 = time.perf_counter()
        results["fillna"] = t1 - t0
        print(f"  DataFrame.where():  {(t1 - t0) * 1000:.1f}ms")

    # Step 3: TypeAdapter creation
    print("\nStep 3: TypeAdapter creation")
    t0 = time.perf_counter()
    adapter = TypeAdapter(list[SimpleValidator])
    t1 = time.perf_counter()
    step3_time = t1 - t0
    results["adapter_creation"] = step3_time
    print(f"  TypeAdapter init:   {step3_time * 1000:.1f}ms")

    # Step 4: Batch validation with TypeAdapter
    print("\nStep 4: Validation methods")
    t0 = time.perf_counter()
    try:
        adapter.validate_python(records_clean)
    except Exception:
        pass
    t1 = time.perf_counter()
    step4_time = t1 - t0
    results["batch_validation"] = step4_time
    print(f"  Batch (TypeAdapter): {step4_time * 1000:.1f}ms")

    if detailed:
        # Compare with row-by-row validation
        t0 = time.perf_counter()
        for record in records_clean:
            try:
                SimpleValidator.model_validate(record)
            except Exception:
                pass
        t1 = time.perf_counter()
        results["row_by_row"] = t1 - t0
        print(f"  Row-by-row:          {(t1 - t0) * 1000:.1f}ms")

        # Test with cached adapter (simulating reuse)
        t0 = time.perf_counter()
        try:
            adapter.validate_python(records_clean)
        except Exception:
            pass
        t1 = time.perf_counter()
        results["batch_validation_cached"] = t1 - t0
        print(f"  Batch (reused):      {(t1 - t0) * 1000:.1f}ms")

    # Total time
    total_time = step1_time + step2_time + step3_time + step4_time
    results["total"] = total_time

    print(f"\n{'=' * 70}")
    print(f"Total time: {total_time * 1000:.1f}ms")
    print(f"{'=' * 70}")

    # Breakdown percentages
    print("\nTime breakdown:")
    print(f"  DataFrame → dict:  {step1_time / total_time * 100:.1f}%")
    print(f"  NaN conversion:    {step2_time / total_time * 100:.1f}%")
    print(f"  TypeAdapter init:  {step3_time / total_time * 100:.1f}%")
    print(f"  Validation:        {step4_time / total_time * 100:.1f}%")

    return results


def profile_daffy_vs_components(n_rows: int) -> None:
    """Compare full daffy validation vs individual components."""
    print(f"\n{'=' * 70}")
    print(f"Daffy end-to-end vs component times ({n_rows:,} rows)")
    print(f"{'=' * 70}\n")

    df = pd.DataFrame(
        {
            "name": [f"Person_{i}" for i in range(n_rows)],
            "age": [25 + (i % 50) for i in range(n_rows)],
            "price": [10.0 + (i % 100) for i in range(n_rows)],
        }
    )

    # Time full daffy validation
    t0 = time.perf_counter()
    try:
        validate_dataframe_rows(df, SimpleValidator)
    except Exception:
        pass
    t1 = time.perf_counter()
    daffy_time = t1 - t0

    print(f"Daffy (full):        {daffy_time * 1000:.1f}ms")

    # Compare with sum of components
    component_results = profile_validation_steps(n_rows, detailed=False)
    component_total = component_results["total"]

    print(f"\nComponent total:     {component_total * 1000:.1f}ms")
    print(f"Daffy overhead:      {(daffy_time - component_total) * 1000:.1f}ms")
    print(f"Overhead %:          {(daffy_time - component_total) / daffy_time * 100:.1f}%")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Profile row validation performance")
    parser.add_argument("--size", type=int, default=10000, help="Number of rows to test")
    parser.add_argument("--detailed", action="store_true", help="Run detailed profiling with alternative approaches")
    parser.add_argument("--compare-daffy", action="store_true", help="Compare full daffy validation vs components")
    args = parser.parse_args()

    if args.compare_daffy:
        profile_daffy_vs_components(args.size)
    else:
        profile_validation_steps(args.size, args.detailed)

    print("\n" + "=" * 70)
    print("Profiling complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
