#!/usr/bin/env python3
"""
Manual script to test daffy with different dependency combinations.

This script helps verify that the optional dependencies work correctly
by testing with different combinations of pandas/polars.

Usage:
    # First build a wheel to avoid dev dependencies
    uv build --wheel

    # Test with pandas only
    WHEEL=$(ls dist/daffy-*.whl | head -n1)
    uv run --no-project --with "pandas>=1.5.1" --with "$WHEEL" python scripts/test_isolated_deps.py pandas

    # Test with polars only
    WHEEL=$(ls dist/daffy-*.whl | head -n1)
    uv run --no-project --with "polars>=1.7.0" --with "$WHEEL" python scripts/test_isolated_deps.py polars

    # Test with both
    WHEEL=$(ls dist/daffy-*.whl | head -n1)
    uv run --no-project --with "pandas>=1.5.1" --with "polars>=1.7.0" --with "$WHEEL" \\
        python scripts/test_isolated_deps.py both

    # Test with neither (should fail gracefully)
    WHEEL=$(ls dist/daffy-*.whl | head -n1)
    uv run --no-project --with "$WHEEL" python scripts/test_isolated_deps.py none
"""

import importlib.util
import sys
from typing import Any


def test_pandas_only() -> bool:
    """Test daffy with only pandas installed."""
    print("Testing pandas-only configuration...")

    if importlib.util.find_spec("pandas") is None:
        print("❌ Pandas not available")
        return False
    else:
        print("✅ Pandas import successful")

    if importlib.util.find_spec("polars") is not None:
        print("❌ Polars should not be available")
        print("   Note: This test requires polars not to be installed")
        print("   This is expected to work only in CI with truly isolated environments")
        return False
    else:
        print("✅ Polars correctly not available")

    try:
        from daffy import df_in, df_out
        from daffy.utils import HAS_PANDAS, HAS_POLARS

        assert HAS_PANDAS, f"HAS_PANDAS should be True, got {HAS_PANDAS}"
        assert not HAS_POLARS, f"HAS_POLARS should be False, got {HAS_POLARS}"
        print("✅ Library detection correct")

        @df_in(columns=["A", "B"])
        @df_out(columns=["A", "B", "C"])
        def test_func(df: Any) -> Any:
            df = df.copy()
            df["C"] = df["A"] + df["B"]
            return df

        import pandas as pd  # Re-import for local scope

        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        result = test_func(df)

        assert list(result.columns) == ["A", "B", "C"], f"Wrong columns: {list(result.columns)}"
        print("✅ Decorators work correctly")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_polars_only() -> bool:
    """Test daffy with only polars installed."""
    print("Testing polars-only configuration...")

    if importlib.util.find_spec("polars") is None:
        print("❌ Polars not available")
        return False
    else:
        print("✅ Polars import successful")

    if importlib.util.find_spec("pandas") is not None:
        print("❌ Pandas should not be available")
        print("   Note: This test requires pandas not to be installed")
        print("   This is expected to work only in CI with truly isolated environments")
        return False
    else:
        print("✅ Pandas correctly not available")

    try:
        from daffy import df_in, df_out
        from daffy.utils import HAS_PANDAS, HAS_POLARS

        assert not HAS_PANDAS, f"HAS_PANDAS should be False, got {HAS_PANDAS}"
        assert HAS_POLARS, f"HAS_POLARS should be True, got {HAS_POLARS}"
        print("✅ Library detection correct")

        @df_in(columns=["A", "B"])
        @df_out(columns=["A", "B", "C"])
        def test_func(df: Any) -> Any:
            import polars as pl  # Import here for local scope

            return df.with_columns((pl.col("A") + pl.col("B")).alias("C"))

        import polars as pl  # Re-import for local scope

        df = pl.DataFrame({"A": [1, 2], "B": [3, 4]})
        result = test_func(df)

        assert result.columns == ["A", "B", "C"], f"Wrong columns: {result.columns}"
        print("✅ Decorators work correctly")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_both() -> bool:
    """Test daffy with both libraries installed."""
    print("Testing both pandas and polars configuration...")

    if importlib.util.find_spec("pandas") is None or importlib.util.find_spec("polars") is None:
        print("❌ Both libraries should be available")
        return False
    else:
        print("✅ Both libraries import successful")

    try:
        from daffy import df_in
        from daffy.utils import HAS_PANDAS, HAS_POLARS

        assert HAS_PANDAS, f"HAS_PANDAS should be True, got {HAS_PANDAS}"
        assert HAS_POLARS, f"HAS_POLARS should be True, got {HAS_POLARS}"
        print("✅ Library detection correct")

        @df_in(columns=["A", "B"])
        def test_func(df: Any) -> Any:
            return df

        # Test both types work
        import pandas as pd  # Re-import for local scope
        import polars as pl  # Re-import for local scope

        pandas_df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        polars_df = pl.DataFrame({"A": [1, 2], "B": [3, 4]})

        test_func(pandas_df)
        test_func(polars_df)

        print("✅ Both DataFrame types work")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_none() -> bool:
    """Test daffy with no DataFrame libraries installed."""
    print("Testing no DataFrame libraries configuration...")

    if importlib.util.find_spec("pandas") is not None:
        print("❌ Pandas should not be available")
        print("   Note: This test requires a clean environment without pandas/polars")
        print("   This is expected to work only in CI with isolated environments")
        return False
    else:
        print("✅ Pandas correctly not available")

    if importlib.util.find_spec("polars") is not None:
        print("❌ Polars should not be available")
        print("   Note: This test requires a clean environment without pandas/polars")
        print("   This is expected to work only in CI with isolated environments")
        return False
    else:
        print("✅ Polars correctly not available")

    try:
        from daffy.utils import HAS_PANDAS, HAS_POLARS  # noqa: F401

        print("❌ Should have failed during import")
        return False
    except ImportError as e:
        expected_msg = "No DataFrame library found"
        if expected_msg in str(e):
            print("✅ Correctly failed with expected error message")
            return True
        else:
            print(f"❌ Wrong error message: {e}")
            return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)

    test_type = sys.argv[1].lower()

    if test_type == "pandas":
        success = test_pandas_only()
    elif test_type == "polars":
        success = test_polars_only()
    elif test_type == "both":
        success = test_both()
    elif test_type == "none":
        success = test_none()
    else:
        print(f"Unknown test type: {test_type}")
        print(__doc__)
        sys.exit(1)

    if success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1)
