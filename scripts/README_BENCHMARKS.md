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

## Benchmark Environment

**Hardware**: MacBook Pro M1 Pro
**Python**: 3.10

*Note: Performance numbers are hardware-dependent. Your results may vary based on CPU, memory, and Python version.*

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
| Daffy (pandas) | 133ms | 751K rows/s | baseline |
| Daffy (polars) | 145ms | 688K rows/s | 1.09x slower |
| Pandantic | 149ms | 669K rows/s | 1.12x slower |

### 100,000 rows - Medium Validation

| Method | Time | Throughput | vs Daffy |
|--------|------|------------|----------|
| Daffy (pandas) | 220ms | 454K rows/s | baseline |
| Daffy (polars) | 273ms | 367K rows/s | 1.24x slower |
| Pandantic | 296ms | 338K rows/s | 1.34x slower |

### Key Findings

1. **Daffy is now fastest** - Optimizations make daffy 12-34% faster than competitors
2. **Major speedup** - 1.8-2.2x faster than previous daffy implementation
3. **Polars is competitive** - Polars performance is close to pandas for validation
4. **All are fast** - All libraries process 300K-750K rows/second
5. **Much better than naive** - Optimized approaches are 5-10x faster than row-by-row baseline

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
