# Row Validation Performance Benchmarks

This directory contains performance benchmarking scripts for daffy's row validation feature.

## Quick Start

```bash
# Install benchmark dependencies
pip install pandantic

# Run all benchmarks (simple + medium scenarios, 1k/10k/100k rows)
python scripts/benchmark_row_validation.py

# Run specific scenario
python scripts/benchmark_row_validation.py --scenario simple

# Run specific size
python scripts/benchmark_row_validation.py --size 10000
```

## What Gets Benchmarked

### Libraries Compared
- **Daffy (pandas)**: Our batch validation with pandas DataFrames
- **Daffy (polars)**: Our batch validation with polars DataFrames
- **Pandantic**: Competing library for DataFrame validation with Pydantic
- **Pydantic (baseline)**: Raw row-by-row validation (only for ≤10k rows)

### Scenarios
- **Simple**: 3 fields (name, age, price) with basic constraints
- **Medium**: 10 fields with various validators and types

### DataFrame Sizes
- 1,000 rows
- 10,000 rows
- 100,000 rows

## Sample Results

### 100,000 rows - Simple Validation

| Method | Time | Throughput | vs Daffy |
|--------|------|------------|----------|
| Pandantic | 145ms | 688K rows/s | 1.7x faster |
| Daffy (polars) | 214ms | 467K rows/s | 1.2x faster |
| Daffy (pandas) | 246ms | 407K rows/s | baseline |

### Key Findings

1. **Pandantic is faster** - Pandantic outperforms daffy by ~40-70% across scenarios
2. **Polars helps** - Using polars improves performance by ~15-20% vs pandas
3. **All are fast** - All libraries process 200K-700K rows/second
4. **Much better than naive** - All batch approaches are 5-10x faster than row-by-row

## Why Benchmark?

These benchmarks help us:
- Understand competitive positioning vs Pandantic
- Identify optimization opportunities
- Validate that batch processing is worthwhile
- Make informed trade-offs between features and performance

## Notes

- Benchmarks are **not included in CI** (too slow)
- Run manually when investigating performance
- Results vary by hardware, Python version, and dataset characteristics
- Pandantic only supports pandas (not polars)
