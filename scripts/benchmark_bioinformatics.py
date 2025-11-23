#!/usr/bin/env python3
"""
Benchmark row validation with realistic bioinformatics feature engineering scenario.

This simulates validating a cancer research feature store with:
- Gene expression levels
- Clinical measurements
- Patient demographics
- Mutation markers
- Treatment outcomes
- Missing data patterns (common in real datasets)
"""

import sys
import time
from typing import Any, Literal, Optional

import numpy as np
import pandas as pd
import polars as pl
from pydantic import BaseModel, Field, field_validator

sys.path.insert(0, ".")

from daffy.row_validation import validate_dataframe_rows

# ============================================================================
# Bioinformatics Feature Model
# ============================================================================


class BioinformaticsFeatures(BaseModel):
    """Realistic bioinformatics feature store schema for cancer research."""

    # Patient identifiers
    patient_id: str = Field(min_length=5, max_length=20)
    sample_id: str = Field(min_length=5, max_length=20)
    cohort: Literal["training", "validation", "test"]

    # Clinical measurements (often missing)
    age: Optional[int] = Field(None, ge=0, le=120)
    bmi: Optional[float] = Field(None, ge=10.0, le=60.0)
    tumor_size_mm: Optional[float] = Field(None, ge=0.0, le=300.0)

    # Gene expression levels (log2 normalized, common range)
    gene_tp53_expr: float = Field(ge=-10.0, le=15.0)
    gene_brca1_expr: float = Field(ge=-10.0, le=15.0)
    gene_brca2_expr: float = Field(ge=-10.0, le=15.0)
    gene_myc_expr: float = Field(ge=-10.0, le=15.0)
    gene_erbb2_expr: float = Field(ge=-10.0, le=15.0)

    # Protein markers (often missing in real data)
    protein_ki67_percent: Optional[float] = Field(None, ge=0.0, le=100.0)
    protein_er_status: Optional[Literal["positive", "negative", "unknown"]] = None
    protein_pr_status: Optional[Literal["positive", "negative", "unknown"]] = None
    protein_her2_status: Optional[Literal["positive", "negative", "equivocal"]] = None

    # Mutation markers (boolean flags)
    mutation_tp53: bool
    mutation_brca1: bool
    mutation_brca2: bool
    mutation_pten: bool

    # Tumor characteristics
    tumor_grade: Literal["G1", "G2", "G3", "GX"]
    tumor_stage: Literal["I", "II", "III", "IV", "unknown"]
    histology_type: str = Field(min_length=3, max_length=50)

    # Treatment and outcomes
    received_chemotherapy: bool
    received_radiotherapy: bool
    months_followup: Optional[float] = Field(None, ge=0.0, le=240.0)
    disease_free_survival_months: Optional[float] = Field(None, ge=0.0, le=240.0)
    overall_survival_months: Optional[float] = Field(None, ge=0.0, le=240.0)

    # Derived features
    risk_score: float = Field(ge=0.0, le=1.0)
    treatment_response: Optional[Literal["complete", "partial", "stable", "progressive"]] = None

    # Quality metrics
    sample_quality_score: float = Field(ge=0.0, le=100.0)
    sequencing_depth: int = Field(ge=1000000, le=500000000)  # 1M to 500M reads
    contamination_rate: float = Field(ge=0.0, le=1.0)

    @field_validator("disease_free_survival_months")
    @classmethod
    def dfs_must_be_lte_followup(cls, v: Optional[float], info: Any) -> Optional[float]:
        """Disease-free survival cannot exceed follow-up time."""
        if v is not None and info.data.get("months_followup") is not None:
            if v > info.data["months_followup"]:
                raise ValueError("DFS cannot exceed follow-up time")
        return v


# ============================================================================
# Data Generation
# ============================================================================


def generate_bioinformatics_data(n_rows: int, missing_rate: float = 0.15) -> pd.DataFrame:
    """
    Generate realistic bioinformatics feature data.

    Args:
        n_rows: Number of samples to generate
        missing_rate: Proportion of optional fields that are missing (typical in real data)
    """
    np.random.seed(42)

    data = {
        # Identifiers
        "patient_id": [f"PAT{i:08d}" for i in range(n_rows)],
        "sample_id": [f"SAMPLE{i:08d}" for i in range(n_rows)],
        "cohort": np.random.choice(["training", "validation", "test"], n_rows, p=[0.7, 0.15, 0.15]),
        # Clinical measurements (with missing values)
        "age": np.where(np.random.random(n_rows) < missing_rate, np.nan, np.random.randint(25, 90, n_rows)),
        "bmi": np.where(
            np.random.random(n_rows) < missing_rate, np.nan, np.random.normal(27.5, 5.0, n_rows).clip(15.0, 50.0)
        ),
        "tumor_size_mm": np.where(
            np.random.random(n_rows) < missing_rate, np.nan, np.random.lognormal(2.5, 0.8, n_rows).clip(5.0, 200.0)
        ),
        # Gene expression (log2 normalized, rarely missing)
        "gene_tp53_expr": np.random.normal(3.5, 2.0, n_rows),
        "gene_brca1_expr": np.random.normal(2.8, 1.8, n_rows),
        "gene_brca2_expr": np.random.normal(3.2, 1.9, n_rows),
        "gene_myc_expr": np.random.normal(4.1, 2.2, n_rows),
        "gene_erbb2_expr": np.random.normal(3.0, 2.5, n_rows),
        # Protein markers (often missing)
        "protein_ki67_percent": np.where(
            np.random.random(n_rows) < missing_rate * 1.5, np.nan, (np.random.beta(2, 5, n_rows) * 100).clip(0, 100)
        ),
        "protein_er_status": np.where(  # type: ignore[call-overload]
            np.random.random(n_rows) < missing_rate,
            None,
            np.random.choice(["positive", "negative", "unknown"], n_rows, p=[0.7, 0.25, 0.05]),
        ),
        "protein_pr_status": np.where(  # type: ignore[call-overload]
            np.random.random(n_rows) < missing_rate,
            None,
            np.random.choice(["positive", "negative", "unknown"], n_rows, p=[0.65, 0.30, 0.05]),
        ),
        "protein_her2_status": np.where(  # type: ignore[call-overload]
            np.random.random(n_rows) < missing_rate,
            None,
            np.random.choice(["positive", "negative", "equivocal"], n_rows, p=[0.20, 0.75, 0.05]),
        ),
        # Mutation markers (boolean)
        "mutation_tp53": np.random.random(n_rows) < 0.35,
        "mutation_brca1": np.random.random(n_rows) < 0.10,
        "mutation_brca2": np.random.random(n_rows) < 0.12,
        "mutation_pten": np.random.random(n_rows) < 0.18,
        # Tumor characteristics
        "tumor_grade": np.random.choice(["G1", "G2", "G3", "GX"], n_rows, p=[0.15, 0.40, 0.35, 0.10]),
        "tumor_stage": np.random.choice(["I", "II", "III", "IV", "unknown"], n_rows, p=[0.20, 0.35, 0.25, 0.15, 0.05]),
        "histology_type": np.random.choice(
            [
                "invasive ductal carcinoma",
                "invasive lobular carcinoma",
                "ductal carcinoma in situ",
                "mucinous carcinoma",
                "tubular carcinoma",
            ],
            n_rows,
            p=[0.60, 0.15, 0.10, 0.08, 0.07],
        ),
        # Treatment
        "received_chemotherapy": np.random.random(n_rows) < 0.65,
        "received_radiotherapy": np.random.random(n_rows) < 0.55,
        # Outcomes (with missing values)
        "months_followup": np.where(
            np.random.random(n_rows) < missing_rate * 0.5,  # Less missing for key outcomes
            np.nan,
            np.random.exponential(30, n_rows).clip(1.0, 180.0),
        ),
        # Derived features
        "risk_score": np.random.beta(2, 5, n_rows),
        "treatment_response": np.where(  # type: ignore[call-overload]
            np.random.random(n_rows) < missing_rate * 2,  # More missing (not all treated)
            None,
            np.random.choice(["complete", "partial", "stable", "progressive"], n_rows, p=[0.25, 0.35, 0.25, 0.15]),
        ),
        # Quality metrics
        "sample_quality_score": np.random.beta(8, 2, n_rows) * 100,
        "sequencing_depth": np.random.lognormal(18, 0.5, n_rows).astype(int).clip(5_000_000, 300_000_000),
        "contamination_rate": np.random.beta(1, 20, n_rows),
    }

    df = pd.DataFrame(data)

    # Add DFS and OS based on follow-up (with constraints)
    followup = df["months_followup"].fillna(0).clip(0, 240)  # Clip to valid range
    df["disease_free_survival_months"] = np.where(
        np.random.random(n_rows) < missing_rate * 0.5, np.nan, (np.random.uniform(0, 1, n_rows) * followup).clip(0, 240)
    )
    df["overall_survival_months"] = np.where(
        np.random.random(n_rows) < missing_rate * 0.5, np.nan, (np.random.uniform(0, 1, n_rows) * followup).clip(0, 240)
    )

    return df


# ============================================================================
# Benchmarking
# ============================================================================


def benchmark_bioinformatics_validation(n_rows: int, runs: int = 3) -> dict[str, float]:
    """Benchmark validation on bioinformatics data."""
    print(f"\n{'=' * 70}")
    print("Bioinformatics Feature Validation Benchmark")
    print(f"Dataset: {n_rows:,} samples with {len(BioinformaticsFeatures.model_fields)} features")
    print(f"{'=' * 70}\n")

    # Generate data
    print("Generating realistic bioinformatics data...")
    df_pandas = generate_bioinformatics_data(n_rows)

    # Convert to polars (handle nullable types)
    try:
        df_polars = pl.from_pandas(df_pandas)
    except ImportError:
        print("Warning: pyarrow not installed, skipping polars benchmark")
        df_polars = None

    # Calculate missing data stats
    missing_pct = (df_pandas.isnull().sum().sum() / (len(df_pandas) * len(df_pandas.columns))) * 100
    print("Dataset characteristics:")
    print(f"  - Rows: {len(df_pandas):,}")
    print(f"  - Columns: {len(df_pandas.columns)}")
    print(f"  - Missing data: {missing_pct:.1f}%")
    print(f"  - Memory: {df_pandas.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    print()

    results = {}

    # Benchmark pandas
    print(f"Benchmarking Daffy (pandas) - {runs} runs...")
    times: list[float] = []
    for i in range(runs):
        start = time.perf_counter()
        validate_dataframe_rows(df_pandas, BioinformaticsFeatures)
        end = time.perf_counter()
        times.append(end - start)
        print(f"  Run {i + 1}: {(end - start) * 1000:.1f}ms")

    avg_time = sum(times) / len(times)
    results["pandas"] = avg_time
    throughput = n_rows / avg_time
    print(f"  Average: {avg_time * 1000:.1f}ms ({throughput:,.0f} rows/sec)\n")

    # Benchmark polars
    if df_polars is not None:
        print(f"Benchmarking Daffy (polars) - {runs} runs...")
        times: list[float] = []
        for i in range(runs):
            start = time.perf_counter()
            validate_dataframe_rows(df_polars, BioinformaticsFeatures)
            end = time.perf_counter()
            times.append(end - start)
            print(f"  Run {i + 1}: {(end - start) * 1000:.1f}ms")

        avg_time = sum(times) / len(times)
        results["polars"] = avg_time
        throughput = n_rows / avg_time
        print(f"  Average: {avg_time * 1000:.1f}ms ({throughput:,.0f} rows/sec)\n")

    return results


def main() -> None:
    """Run bioinformatics benchmarks."""
    import argparse

    parser = argparse.ArgumentParser(description="Benchmark bioinformatics feature validation")
    parser.add_argument("--size", type=int, default=10000, help="Number of samples to validate (default: 10000)")
    parser.add_argument("--runs", type=int, default=3, help="Number of benchmark runs (default: 3)")
    args = parser.parse_args()

    results = benchmark_bioinformatics_validation(args.size, args.runs)

    print(f"{'=' * 70}")
    print("Summary")
    print(f"{'=' * 70}")
    print(f"Pandas: {results['pandas'] * 1000:.1f}ms ({args.size / results['pandas']:,.0f} rows/sec)")
    if "polars" in results:
        print(f"Polars: {results['polars'] * 1000:.1f}ms ({args.size / results['polars']:,.0f} rows/sec)")
    print()


if __name__ == "__main__":
    main()
