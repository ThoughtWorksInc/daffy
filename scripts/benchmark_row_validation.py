#!/usr/bin/env python3
"""
Performance benchmarks for row validation.

Compares daffy's row validation performance against Pandantic, a competing library
for validating pandas DataFrames with Pydantic models.

Usage:
    # Install benchmark dependencies first
    pip install pandantic

    # Run all benchmarks
    python scripts/benchmark_row_validation.py

    # Run specific scenario
    python scripts/benchmark_row_validation.py --scenario simple

    # Run specific size
    python scripts/benchmark_row_validation.py --size 10000

Requirements:
    - pandas
    - polars
    - pydantic >= 2.4.0
    - pandantic
    - daffy (from current directory)
"""

import argparse
import sys
import time
from typing import Any

import pandas as pd
import polars as pl
from pydantic import BaseModel, ConfigDict, Field

# Add current directory to path to import daffy
sys.path.insert(0, ".")

from daffy.row_validation import validate_dataframe_rows

try:
    from pandantic import Pandantic  # type: ignore[import-not-found]
except ImportError:
    print("ERROR: pandantic not installed. Install with: pip install pandantic")
    sys.exit(1)


# ============================================================================
# Validation Models
# ============================================================================


class SimpleValidator(BaseModel):
    """Simple validation: 3 fields with basic constraints."""

    model_config = ConfigDict(str_strip_whitespace=True)

    name: str
    age: int = Field(ge=0, le=120)
    price: float = Field(gt=0)


class MediumValidator(BaseModel):
    """Medium complexity: 10 fields with various validators."""

    model_config = ConfigDict(str_strip_whitespace=True)

    id: int = Field(ge=1)
    name: str = Field(min_length=1, max_length=100)
    email: str
    age: int = Field(ge=0, le=120)
    salary: float = Field(ge=0)
    is_active: bool
    department: str
    score: float = Field(ge=0, le=100)
    years_experience: int = Field(ge=0, le=50)
    rating: float = Field(ge=1.0, le=5.0)


class AddressValidator(BaseModel):
    """Nested model for complex validation."""

    street: str
    city: str
    zipcode: str = Field(pattern=r"^\d{5}$")


class ComplexValidator(BaseModel):
    """Complex validation with nested models."""

    model_config = ConfigDict(str_strip_whitespace=True)

    id: int = Field(ge=1)
    name: str = Field(min_length=1)
    email: str = Field(pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    age: int = Field(ge=18, le=120)
    salary: float = Field(ge=0, le=1000000)
    address: AddressValidator


# ============================================================================
# Data Generation
# ============================================================================


def generate_simple_data(n_rows: int) -> dict[str, list[Any]]:
    """Generate simple test data."""
    return {
        "name": [f"Person_{i}" for i in range(n_rows)],
        "age": [25 + (i % 50) for i in range(n_rows)],
        "price": [10.0 + (i % 100) for i in range(n_rows)],
    }


def generate_medium_data(n_rows: int) -> dict[str, list[Any]]:
    """Generate medium complexity test data."""
    return {
        "id": list(range(1, n_rows + 1)),
        "name": [f"Person_{i}" for i in range(n_rows)],
        "email": [f"person{i}@example.com" for i in range(n_rows)],
        "age": [25 + (i % 50) for i in range(n_rows)],
        "salary": [50000.0 + (i % 100000) for i in range(n_rows)],
        "is_active": [i % 2 == 0 for i in range(n_rows)],
        "department": [f"Dept_{i % 10}" for i in range(n_rows)],
        "score": [50.0 + (i % 50) for i in range(n_rows)],
        "years_experience": [i % 30 for i in range(n_rows)],
        "rating": [1.0 + (i % 4) for i in range(n_rows)],
    }


def generate_complex_data(n_rows: int) -> dict[str, list[Any]]:
    """Generate complex test data with nested structures."""
    # For complex validation, we need to handle nested models differently
    # This is a simplified version - full nested validation would need more setup
    return {
        "id": list(range(1, n_rows + 1)),
        "name": [f"Person_{i}" for i in range(n_rows)],
        "email": [f"person{i}@example.com" for i in range(n_rows)],
        "age": [25 + (i % 50) for i in range(n_rows)],
        "salary": [50000.0 + (i % 100000) for i in range(n_rows)],
        "address": [
            {"street": f"{i} Main St", "city": "City", "zipcode": f"{10000 + (i % 90000):05d}"} for i in range(n_rows)
        ],
    }


# ============================================================================
# Benchmarking Functions
# ============================================================================


def benchmark_daffy_pandas(df: pd.DataFrame, validator: type[BaseModel], runs: int = 5) -> float:
    """Benchmark daffy with pandas DataFrame."""
    times: list[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        validate_dataframe_rows(df, validator)
        end = time.perf_counter()
        times.append(end - start)
    return sum(times) / len(times)


def benchmark_daffy_polars(df: pl.DataFrame, validator: type[BaseModel], runs: int = 5) -> float:
    """Benchmark daffy with polars DataFrame."""
    times: list[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        validate_dataframe_rows(df, validator)
        end = time.perf_counter()
        times.append(end - start)
    return sum(times) / len(times)


def benchmark_pandantic(df: pd.DataFrame, validator: type[BaseModel], runs: int = 5) -> float:
    """Benchmark pandantic validation."""
    pandantic_validator = Pandantic(schema=validator)  # type: ignore[misc]
    times: list[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        try:
            pandantic_validator.validate(dataframe=df, errors="raise")
        except (ValueError, Exception):
            # Pandantic raises ValueError on validation error, which is expected
            pass
        end = time.perf_counter()
        times.append(end - start)
    return sum(times) / len(times)


def benchmark_pydantic_baseline(df: pd.DataFrame, validator: type[BaseModel], runs: int = 5) -> float:
    """Benchmark raw Pydantic row-by-row validation (baseline)."""
    times: list[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        for _, row in df.iterrows():
            try:
                validator.model_validate(row.to_dict())
            except Exception:
                pass
        end = time.perf_counter()
        times.append(end - start)
    return sum(times) / len(times)


# ============================================================================
# Results Formatting
# ============================================================================


def format_time(seconds: float) -> str:
    """Format time in appropriate unit."""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.1f} μs"
    elif seconds < 1:
        return f"{seconds * 1000:.1f} ms"
    else:
        return f"{seconds:.2f} s"


def format_throughput(n_rows: int, seconds: float) -> str:
    """Format throughput as rows/second."""
    throughput = n_rows / seconds
    if throughput >= 1_000_000:
        return f"{throughput / 1_000_000:.2f}M rows/s"
    elif throughput >= 1_000:
        return f"{throughput / 1_000:.1f}K rows/s"
    else:
        return f"{throughput:.0f} rows/s"


def print_results(scenario: str, n_rows: int, results: dict[str, float | None]) -> None:
    """Print benchmark results in a nice table."""
    print(f"\n{'=' * 70}")
    print(f"Scenario: {scenario}")
    print(f"Rows: {n_rows:,}")
    print(f"{'=' * 70}")

    print(f"{'Method':<30} {'Time':<12} {'Throughput':<15} {'vs Daffy':<10}")
    print("-" * 70)

    # Sort by time (fastest first)
    sorted_results = sorted(results.items(), key=lambda x: x[1] if x[1] is not None else float("inf"))
    daffy_time = results.get("Daffy (pandas)")

    for method, time_taken in sorted_results:
        if time_taken is None:
            print(f"{method:<30} {'N/A':<12} {'N/A':<15} {'N/A':<10}")
            continue

        throughput = format_throughput(n_rows, time_taken)
        time_str = format_time(time_taken)

        if daffy_time and time_taken > 0:
            speedup = time_taken / daffy_time
            speedup_str = f"{speedup:.2f}x" if speedup > 1 else f"{1 / speedup:.2f}x faster"
        else:
            speedup_str = "baseline"

        print(f"{method:<30} {time_str:<12} {throughput:<15} {speedup_str:<10}")


# ============================================================================
# Main Benchmark Runner
# ============================================================================


def run_benchmark(scenario: str, n_rows: int) -> None:
    """Run benchmark for a specific scenario and size."""

    if scenario == "simple":
        data = generate_simple_data(n_rows)
        validator = SimpleValidator
    elif scenario == "medium":
        data = generate_medium_data(n_rows)
        validator = MediumValidator
    else:
        print(f"Unknown scenario: {scenario}")
        return

    # Create DataFrames
    df_pandas = pd.DataFrame(data)
    df_polars = pl.DataFrame(data)

    # Run benchmarks
    results: dict[str, float | None] = {}

    print(f"\nBenchmarking {scenario} validation with {n_rows:,} rows...")

    # Daffy pandas
    print("  - Daffy (pandas)...")
    results["Daffy (pandas)"] = benchmark_daffy_pandas(df_pandas, validator)

    # Daffy polars
    print("  - Daffy (polars)...")
    results["Daffy (polars)"] = benchmark_daffy_polars(df_polars, validator)

    # Pandantic
    print("  - Pandantic...")
    results["Pandantic"] = benchmark_pandantic(df_pandas, validator)

    # Raw Pydantic baseline (only for smaller datasets)
    if n_rows <= 10000:
        print("  - Pydantic (row-by-row baseline)...")
        results["Pydantic (baseline)"] = benchmark_pydantic_baseline(df_pandas, validator)

    # Print results
    print_results(scenario, n_rows, results)


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Benchmark row validation performance")
    parser.add_argument(
        "--scenario", choices=["simple", "medium", "all"], default="all", help="Validation scenario to benchmark"
    )
    parser.add_argument("--size", type=int, choices=[1000, 10000, 100000], help="DataFrame size (number of rows)")
    args = parser.parse_args()

    scenarios = ["simple", "medium"] if args.scenario == "all" else [args.scenario]
    sizes = [args.size] if args.size else [1000, 10000, 100000]

    print("=" * 70)
    print("Row Validation Performance Benchmarks")
    print("=" * 70)

    for scenario in scenarios:
        for size in sizes:
            run_benchmark(scenario, size)

    print("\n" + "=" * 70)
    print("Benchmarks complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
