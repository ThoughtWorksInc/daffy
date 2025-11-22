# Row Validation Implementation Plan V2

## Overview

Add Pydantic-based row validation to Daffy using modern best practices and consistent API design.

### Core Design Principles

1. **Consistent naming**: `row_validator` parameter, consistent error types
2. **Performance first**: Use Pydantic's batch validation and efficient DataFrame iteration
3. **Modern Pydantic**: Leverage Pydantic 2.4+ features (TypeAdapter, ConfigDict, field validators)
4. **Basics first**: Full support for pandas/polars before adding advanced features
5. **Test everything**: Comprehensive tests for each phase

---

## Implementation Phases (Reordered)

**Status:**
- ✅ Phase 1: Foundation - Optional Pydantic Support
- ✅ Phase 2: Row Validation Core Logic
- ⏳ Phase 3: Configuration Support (Current)
- Phase 4: Decorator Integration - df_in
- Phase 5: Decorator Integration - df_out
- Phase 6: Polars-specific Optimizations
- Phase 7: Documentation

---

### Phase 1: Foundation - Optional Pydantic Support ✅
**Goal**: Set up infrastructure for optional Pydantic dependency

**Files to create**:
- `daffy/pydantic_types.py` - Similar to dataframe_types.py pattern

**Implementation**:
```python
# daffy/pydantic_types.py
from __future__ import annotations

from typing import TYPE_CHECKING, Any

# Runtime import with availability flag
try:
    from pydantic import BaseModel, ValidationError, ConfigDict, TypeAdapter
    from pydantic import __version__ as PYDANTIC_VERSION

    # Check for minimum version (2.4.0 for TypeAdapter)
    major, minor = map(int, PYDANTIC_VERSION.split('.')[:2])
    if major < 2 or (major == 2 and minor < 4):
        raise ImportError(f"Pydantic {PYDANTIC_VERSION} is too old. Daffy requires Pydantic >= 2.4.0")

    HAS_PYDANTIC = True
except ImportError as e:
    BaseModel = None  # type: ignore
    ValidationError = None  # type: ignore
    ConfigDict = None  # type: ignore
    TypeAdapter = None  # type: ignore
    HAS_PYDANTIC = False
    PYDANTIC_VERSION = None

# Compile-time types for type checkers
if TYPE_CHECKING:
    from pydantic import BaseModel, ValidationError, ConfigDict, TypeAdapter

def get_pydantic_available() -> bool:
    """Check if Pydantic is available."""
    return HAS_PYDANTIC

def require_pydantic() -> None:
    """Raise ImportError if Pydantic is not available or too old."""
    if not HAS_PYDANTIC:
        raise ImportError(
            "Pydantic >= 2.4.0 is required for row validation. "
            "Install it with: pip install 'pydantic>=2.4.0'"
        )
```

**Tests**:
```python
# tests/test_pydantic_types.py
import pytest

def test_pydantic_availability():
    """Test that HAS_PYDANTIC reflects actual availability."""
    from daffy.pydantic_types import HAS_PYDANTIC, get_pydantic_available

    try:
        import pydantic
        # Check version
        major, minor = map(int, pydantic.__version__.split('.')[:2])
        if major >= 2 and minor >= 4:
            assert HAS_PYDANTIC is True
            assert get_pydantic_available() is True
        else:
            assert HAS_PYDANTIC is False
            assert get_pydantic_available() is False
    except ImportError:
        assert HAS_PYDANTIC is False
        assert get_pydantic_available() is False

def test_require_pydantic():
    """Test require_pydantic behavior."""
    from daffy.pydantic_types import require_pydantic, HAS_PYDANTIC

    if HAS_PYDANTIC:
        require_pydantic()  # Should not raise
    else:
        with pytest.raises(ImportError, match="Pydantic >= 2.4.0"):
            require_pydantic()
```

**Commit**: `feat: add optional Pydantic 2.4+ dependency support`

---

### Phase 2: Row Validation Core Logic with Modern Pydantic ✅
**Goal**: Implement efficient row validation using TypeAdapter for batch processing

**Files to create**:
- `daffy/row_validation.py` - Core validation logic with modern patterns

**Implementation**:
```python
# daffy/row_validation.py
from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, Iterator

from daffy.dataframe_types import get_dataframe_types
from daffy.pydantic_types import HAS_PYDANTIC, require_pydantic

if TYPE_CHECKING:
    from pydantic import BaseModel, ValidationError, TypeAdapter
    from daffy.dataframe_types import DataFrameType

if HAS_PYDANTIC:
    from pydantic import ValidationError as PydanticValidationError
    from pydantic import TypeAdapter
else:
    PydanticValidationError = None  # type: ignore
    TypeAdapter = None  # type: ignore


def _iterate_dataframe_with_index(df: DataFrameType) -> Iterator[tuple[Any, dict]]:
    """
    Efficiently iterate over DataFrame rows with proper index handling.

    Returns iterator of (index_label, row_dict) tuples.
    Works for both pandas and polars, handles all index types.
    """
    # Polars DataFrames
    if hasattr(df, 'iter_rows'):
        # Polars doesn't have index, use position
        for idx, row in enumerate(df.iter_rows(named=True)):
            yield idx, row  # row is already a dict

    # Pandas DataFrames
    elif hasattr(df, 'iterrows'):
        for idx_label, row in df.iterrows():
            # idx_label could be int, string, datetime, etc.
            yield idx_label, row.to_dict()

    else:
        raise TypeError(f"Unknown DataFrame type: {type(df)}")


def _convert_nan_to_none(row_dict: dict) -> dict:
    """Convert NaN values to None for Pydantic compatibility."""
    import math

    cleaned = {}
    for key, value in row_dict.items():
        if isinstance(value, float) and math.isnan(value):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned


def validate_dataframe_rows(
    df: DataFrameType,
    row_validator: type[BaseModel],
    max_errors: int = 5,
    convert_nans: bool = True,
) -> None:
    """
    Validate DataFrame rows against a Pydantic model using batch validation.

    Args:
        df: DataFrame to validate (pandas or polars)
        row_validator: Pydantic BaseModel class for validation
        max_errors: Maximum number of errors to collect before stopping
        convert_nans: Whether to convert NaN to None for Pydantic

    Raises:
        AssertionError: If any rows fail validation (consistent with Daffy)
        ImportError: If Pydantic is not installed
    """
    require_pydantic()

    if not isinstance(df, get_dataframe_types()):
        raise TypeError(f"Expected DataFrame, got {type(df)}")

    # Try batch validation first (fastest)
    try:
        _validate_batch(df, row_validator, max_errors, convert_nans)
    except Exception as e:
        # Fallback to iterative validation if batch fails
        # This can happen with complex models or memory constraints
        _validate_iterative(df, row_validator, max_errors, convert_nans)


def _validate_batch(
    df: DataFrameType,
    row_validator: type[BaseModel],
    max_errors: int,
    convert_nans: bool,
) -> None:
    """
    Batch validation using TypeAdapter - much faster for large DataFrames.
    """
    # Convert entire DataFrame to list of dicts
    if hasattr(df, 'to_dict'):
        records = df.to_dict('records')
    elif hasattr(df, 'iter_rows'):
        records = list(df.iter_rows(named=True))
    else:
        raise TypeError(f"Cannot convert {type(df)} to records")

    # Convert NaNs if needed
    if convert_nans:
        records = [_convert_nan_to_none(r) for r in records]

    # Create TypeAdapter for batch validation
    adapter = TypeAdapter(list[row_validator])

    try:
        # Validate all at once - very fast!
        adapter.validate_python(records)
    except PydanticValidationError as e:
        # Extract row-level errors from batch validation
        errors_by_row = []

        for error in e.errors()[:max_errors]:
            # Error location is like [row_index, field_name, ...]
            if error['loc'] and isinstance(error['loc'][0], int):
                row_idx = error['loc'][0]

                # Get index label for better error message
                if hasattr(df, 'index'):
                    idx_label = df.index[row_idx]
                else:
                    idx_label = row_idx

                errors_by_row.append((idx_label, error))

        if errors_by_row:
            _raise_validation_error(df, errors_by_row, len(e.errors()))


def _validate_iterative(
    df: DataFrameType,
    row_validator: type[BaseModel],
    max_errors: int,
    convert_nans: bool,
) -> None:
    """
    Fallback iterative validation - slower but works for all cases.
    """
    failed_rows = []

    for idx_label, row_dict in _iterate_dataframe_with_index(df):
        if convert_nans:
            row_dict = _convert_nan_to_none(row_dict)

        try:
            row_validator.model_validate(row_dict)
        except PydanticValidationError as e:
            failed_rows.append((idx_label, e))
            if len(failed_rows) >= max_errors:
                break

    if failed_rows:
        _raise_validation_error(df, failed_rows, len(failed_rows))


def _raise_validation_error(
    df: DataFrameType,
    errors: list[tuple[Any, Any]],
    total_errors: int,
) -> None:
    """
    Format and raise AssertionError with detailed row validation information.

    Uses AssertionError for consistency with existing Daffy validation.
    """
    total_rows = len(df)
    shown_errors = len(errors)

    # Build detailed error message
    error_lines = [
        f"Row validation failed for {total_errors} out of {total_rows} rows:",
        "",
    ]

    # Show details for each failed row
    for idx_label, error in errors:
        error_lines.append(f"  Row {idx_label}:")

        # Handle Pydantic ValidationError structure
        if isinstance(error, dict):
            # From batch validation
            field_path = ".".join(str(x) for x in error['loc'][1:] if x != '__root__')
            if field_path:
                error_lines.append(f"    - {field_path}: {error['msg']}")
            else:
                error_lines.append(f"    - {error['msg']}")
        elif hasattr(error, 'errors'):
            # From iterative validation
            for err_dict in error.errors():
                field = ".".join(str(loc) for loc in err_dict['loc'] if loc != '__root__')
                if field:
                    error_lines.append(f"    - {field}: {err_dict['msg']}")
                else:
                    error_lines.append(f"    - {err_dict['msg']}")
        else:
            error_lines.append(f"    - {str(error)}")

        error_lines.append("")

    # Add truncation notice if needed
    if total_errors > shown_errors:
        remaining = total_errors - shown_errors
        error_lines.append(f"  ... and {remaining} more row(s) with errors")

    message = "\n".join(error_lines)
    raise AssertionError(message)
```

**Tests**:
```python
# tests/test_row_validation.py
import pytest
import math

# Skip module if Pydantic not available
pydantic = pytest.importorskip("pydantic", minversion="2.4.0")
from pydantic import BaseModel, Field, ConfigDict

from daffy.row_validation import validate_dataframe_rows


class SimpleValidator(BaseModel):
    """Simple test validator."""
    model_config = ConfigDict(str_strip_whitespace=True)

    name: str
    age: int = Field(ge=0, le=120)
    price: float = Field(gt=0)


class TestPandasValidation:
    """Test row validation with pandas DataFrames."""

    pandas = pytest.importorskip("pandas")

    def test_all_valid_rows(self):
        """Test validation passes when all rows are valid."""
        import pandas as pd

        df = pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "price": [10.5, 20.0, 15.75],
        })

        # Should not raise
        validate_dataframe_rows(df, SimpleValidator)

    def test_batch_validation_with_errors(self):
        """Test batch validation catches multiple errors efficiently."""
        import pandas as pd

        df = pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie", "David"],
            "age": [25, -5, 35, 150],  # Bob and David invalid
            "price": [10.5, 20.0, -15.0, 30.0],  # Charlie invalid
        })

        with pytest.raises(AssertionError) as exc_info:
            validate_dataframe_rows(df, SimpleValidator)

        message = str(exc_info.value)
        assert "Row validation failed for" in message
        assert "Row 1:" in message  # Bob (index 1)
        assert "age" in message

    def test_nan_handling(self):
        """Test NaN values are converted to None."""
        import pandas as pd
        import numpy as np

        df = pd.DataFrame({
            "name": ["Alice", "Bob"],
            "age": [25, 30],
            "price": [10.5, np.nan],  # NaN should fail validation
        })

        with pytest.raises(AssertionError) as exc_info:
            validate_dataframe_rows(df, SimpleValidator)

        message = str(exc_info.value)
        assert "Row 1:" in message
        assert "price" in message

    def test_non_integer_index(self):
        """Test validation works with non-integer DataFrame index."""
        import pandas as pd

        df = pd.DataFrame({
            "name": ["Alice", "Bob"],
            "age": [25, -5],  # Bob invalid
            "price": [10.5, 20.0],
        }, index=["person_a", "person_b"])  # String index

        with pytest.raises(AssertionError) as exc_info:
            validate_dataframe_rows(df, SimpleValidator)

        message = str(exc_info.value)
        assert "Row person_b:" in message  # Uses actual index label


class TestPolarsValidation:
    """Test row validation with polars DataFrames."""

    polars = pytest.importorskip("polars")

    def test_polars_valid_rows(self):
        """Test validation passes with valid polars DataFrame."""
        import polars as pl

        df = pl.DataFrame({
            "name": ["Alice", "Bob"],
            "age": [25, 30],
            "price": [10.5, 20.0],
        })

        validate_dataframe_rows(df, SimpleValidator)

    def test_polars_invalid_rows(self):
        """Test validation fails with invalid polars DataFrame."""
        import polars as pl

        df = pl.DataFrame({
            "name": ["Alice", "Bob"],
            "age": [25, -5],  # Bob invalid
            "price": [10.5, 20.0],
        })

        with pytest.raises(AssertionError) as exc_info:
            validate_dataframe_rows(df, SimpleValidator)

        message = str(exc_info.value)
        assert "Row 1:" in message  # Polars uses position
        assert "age" in message


def test_max_errors_limit():
    """Test that max_errors limits the number of errors shown."""
    pandas = pytest.importorskip("pandas")
    import pandas as pd

    df = pd.DataFrame({
        "name": [str(i) for i in range(10)],
        "age": [-1] * 10,  # All invalid
        "price": [10.0] * 10,
    })

    with pytest.raises(AssertionError) as exc_info:
        validate_dataframe_rows(df, SimpleValidator, max_errors=3)

    message = str(exc_info.value)

    # Should show first 3 errors
    assert "Row 0:" in message
    assert "Row 1:" in message
    assert "Row 2:" in message

    # Should indicate more errors exist
    assert "and 7 more row(s) with errors" in message
```

**Commit**: `feat: add efficient row validation with modern Pydantic`

---

### Phase 3: Configuration Support (Moved Earlier)
**Goal**: Set up configuration before decorator integration

**Files to modify**:
- `daffy/config.py` - Add row validation configuration options

**Implementation**:
```python
# In daffy/config.py

def get_row_validation_config() -> dict:
    """
    Get all row validation configuration options.

    Returns:
        Dictionary with row validation settings
    """
    config = get_config()

    return {
        "max_errors": config.get("row_validation_max_errors", 5),
        "convert_nans": config.get("row_validation_convert_nans", True),
    }


def get_row_validation_max_errors(max_errors: int | None = None) -> int:
    """Get max_errors setting for row validation."""
    if max_errors is not None:
        return max_errors

    config = get_row_validation_config()
    return config["max_errors"]


def get_row_validation_convert_nans(convert_nans: bool | None = None) -> bool:
    """Get convert_nans setting for row validation."""
    if convert_nans is not None:
        return convert_nans

    config = get_row_validation_config()
    return config["convert_nans"]
```

**pyproject.toml format**:
```toml
[tool.daffy]
strict = false
row_validation_max_errors = 5  # Show up to 5 failed rows
row_validation_convert_nans = true  # Auto-convert NaN to None
```

**Tests**:
```python
# tests/test_row_validation_config.py
import pytest
from pathlib import Path

def test_row_validation_config_defaults():
    """Test default row validation configuration."""
    from daffy.config import get_row_validation_config

    config = get_row_validation_config()
    assert config["max_errors"] == 5
    assert config["convert_nans"] is True


def test_row_validation_config_from_file(tmp_path, monkeypatch):
    """Test loading row validation config from pyproject.toml."""
    config_file = tmp_path / "pyproject.toml"
    config_file.write_text("""
[tool.daffy]
row_validation_max_errors = 10
row_validation_convert_nans = false
""")

    monkeypatch.chdir(tmp_path)

    # Clear cache
    from daffy import config
    config._config_cache = None

    from daffy.config import get_row_validation_config

    cfg = get_row_validation_config()
    assert cfg["max_errors"] == 10
    assert cfg["convert_nans"] is False

    # Clean up
    config._config_cache = None


def test_explicit_overrides_config():
    """Test that explicit parameters override configuration."""
    from daffy.config import (
        get_row_validation_max_errors,
        get_row_validation_convert_nans
    )

    assert get_row_validation_max_errors(20) == 20
    assert get_row_validation_convert_nans(False) is False
```

**Commit**: `feat: add configuration support for row validation`

---

### Phase 4: Decorator Integration - df_in
**Goal**: Add `row_validator` parameter to `@df_in` decorator

**Files to modify**:
- `daffy/decorators.py` - Add row_validator parameter to df_in

**Implementation**:
```python
# In daffy/decorators.py

# Add imports at top
from daffy.pydantic_types import HAS_PYDANTIC

if TYPE_CHECKING:
    from pydantic import BaseModel

# Modify df_in signature
def df_in(
    columns: ColumnsDef = None,
    *,
    name: str | None = None,
    strict: bool | None = None,
    row_validator: type[BaseModel] | None = None,  # NEW - better name
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to validate DataFrame input.

    Args:
        columns: Expected column names or column-to-dtype mapping
        name: Parameter name containing the DataFrame
        strict: Whether to disallow extra columns
        row_validator: Pydantic model for validating row data (requires pydantic >= 2.4.0)
    """
    def wrapper_df_in(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def inner(*args: Any, **kwargs: Any) -> T:
            # Extract DataFrame parameter
            df = get_parameter(func, name, *args, **kwargs)
            param_name = get_parameter_name(func, name, *args, **kwargs)
            context = format_param_context(param_name, func.__name__, False)

            # Assert it's a DataFrame
            assert_is_dataframe(df, context)

            # Validate columns if specified (existing)
            if columns is not None:
                validate_dataframe(
                    df,
                    columns,
                    get_strict(strict),
                    param_name,
                    func.__name__,
                    False,
                )

            # Validate rows if validator specified (NEW)
            if row_validator is not None:
                from daffy.row_validation import validate_dataframe_rows
                from daffy.config import get_row_validation_config

                config = get_row_validation_config()

                try:
                    validate_dataframe_rows(
                        df,
                        row_validator,
                        max_errors=config["max_errors"],
                        convert_nans=config["convert_nans"],
                    )
                except AssertionError as e:
                    # Add function context to error message
                    raise AssertionError(f"{str(e)}{context}") from e

            # Call original function
            return func(*args, **kwargs)

        return inner
    return wrapper_df_in
```

**Tests**:
```python
# tests/test_df_in_row_validation.py
import pytest

pydantic = pytest.importorskip("pydantic", minversion="2.4.0")
pandas = pytest.importorskip("pandas")

from pydantic import BaseModel, Field, ConfigDict
import pandas as pd

from daffy import df_in


class PersonValidator(BaseModel):
    """Validator for person data."""
    model_config = ConfigDict(str_strip_whitespace=True)

    name: str
    age: int = Field(ge=0, le=120)


def test_df_in_with_valid_rows():
    """Test df_in passes validation with valid rows."""
    @df_in(row_validator=PersonValidator)
    def process_people(df):
        return df

    df = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "age": [25, 30],
    })

    result = process_people(df)
    assert result.equals(df)


def test_df_in_with_invalid_rows():
    """Test df_in raises AssertionError for invalid rows."""
    @df_in(row_validator=PersonValidator)
    def process_people(df):
        return df

    df = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "age": [25, -5],  # Invalid age
    })

    with pytest.raises(AssertionError) as exc_info:
        process_people(df)

    message = str(exc_info.value)
    assert "Row validation failed" in message
    assert "Row 1:" in message  # Bob's row
    assert "age" in message
    assert "function 'process_people' parameter 'df'" in message


def test_df_in_with_columns_and_row_validator():
    """Test df_in validates both columns and rows."""
    @df_in(
        columns=["name", "age"],
        row_validator=PersonValidator
    )
    def process_people(df):
        return df

    # Test missing column (should fail column validation first)
    df = pd.DataFrame({
        "name": ["Alice"],
        # Missing 'age' column
    })

    with pytest.raises(AssertionError, match="Missing columns"):
        process_people(df)

    # Test invalid data (should fail row validation)
    df = pd.DataFrame({
        "name": ["Alice"],
        "age": [150],  # Age too high
    })

    with pytest.raises(AssertionError) as exc_info:
        process_people(df)

    message = str(exc_info.value)
    assert "Row validation failed" in message


def test_df_in_with_named_parameter():
    """Test df_in with specific parameter name."""
    @df_in(name="people_df", row_validator=PersonValidator)
    def process(data, people_df, config=None):
        return people_df

    df = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "age": [25, -5],  # Invalid
    })

    with pytest.raises(AssertionError) as exc_info:
        process("some_data", df)

    message = str(exc_info.value)
    assert "parameter 'people_df'" in message
```

**Commit**: `feat: add row_validator parameter to df_in decorator`

---

### Phase 5: Decorator Integration - df_out
**Goal**: Add `row_validator` parameter to `@df_out` decorator

**Files to modify**:
- `daffy/decorators.py` - Add row_validator parameter to df_out

**Implementation**:
```python
# In daffy/decorators.py

# Modify df_out signature
def df_out(
    columns: ColumnsDef = None,
    *,
    strict: bool | None = None,
    row_validator: type[BaseModel] | None = None,  # NEW
) -> Callable[[Callable[..., DF]], Callable[..., DF]]:
    """
    Decorator to validate DataFrame output.

    Args:
        columns: Expected column names or column-to-dtype mapping
        strict: Whether to disallow extra columns
        row_validator: Pydantic model for validating row data (requires pydantic >= 2.4.0)
    """
    def wrapper_df_out(func: Callable[..., DF]) -> Callable[..., DF]:
        @functools.wraps(func)
        def inner(*args: Any, **kwargs: Any) -> DF:
            # Call original function
            result = func(*args, **kwargs)

            # Format context for errors
            context = format_param_context(None, func.__name__, True)

            # Assert it's a DataFrame
            assert_is_dataframe(result, context)

            # Validate columns if specified
            if columns is not None:
                validate_dataframe(
                    result,
                    columns,
                    get_strict(strict),
                    None,
                    func.__name__,
                    True,
                )

            # Validate rows if validator specified (NEW)
            if row_validator is not None:
                from daffy.row_validation import validate_dataframe_rows
                from daffy.config import get_row_validation_config

                config = get_row_validation_config()

                try:
                    validate_dataframe_rows(
                        result,
                        row_validator,
                        max_errors=config["max_errors"],
                        convert_nans=config["convert_nans"],
                    )
                except AssertionError as e:
                    # Add function context to error message
                    raise AssertionError(f"{str(e)}{context}") from e

            return result

        return inner
    return wrapper_df_out
```

**Tests**: Similar pattern to df_in tests

**Commit**: `feat: add row_validator parameter to df_out decorator`

---

### Phase 6: Polars-specific Optimizations
**Goal**: Ensure excellent Polars support with proper testing

**Tests to add**:
```python
# tests/test_row_validation_polars.py
import pytest

polars = pytest.importorskip("polars")
pydantic = pytest.importorskip("pydantic", minversion="2.4.0")

import polars as pl
from pydantic import BaseModel, Field

from daffy import df_in, df_out
from daffy.row_validation import validate_dataframe_rows


class DataValidator(BaseModel):
    """Test validator."""
    id: int
    value: float = Field(gt=0)
    category: str


def test_polars_batch_validation():
    """Test batch validation works efficiently with Polars."""
    df = pl.DataFrame({
        "id": list(range(1000)),
        "value": [1.0] * 999 + [-1.0],  # Last row invalid
        "category": ["A"] * 1000,
    })

    with pytest.raises(AssertionError) as exc_info:
        validate_dataframe_rows(df, DataValidator)

    message = str(exc_info.value)
    assert "Row 999:" in message
    assert "value" in message


def test_polars_lazy_frame_error():
    """Test that LazyFrames are not supported (need collection first)."""
    df = pl.DataFrame({
        "id": [1, 2],
        "value": [1.0, 2.0],
        "category": ["A", "B"],
    }).lazy()  # LazyFrame

    # Should get clear error about LazyFrame
    with pytest.raises(TypeError, match="DataFrame"):
        validate_dataframe_rows(df.collect(), DataValidator)  # Need to collect first


def test_polars_null_handling():
    """Test Polars null values are handled properly."""
    df = pl.DataFrame({
        "id": [1, 2],
        "value": [1.0, None],  # Polars uses None not NaN
        "category": ["A", "B"],
    })

    with pytest.raises(AssertionError) as exc_info:
        validate_dataframe_rows(df, DataValidator)

    message = str(exc_info.value)
    assert "Row 1:" in message
```

**Commit**: `test: add comprehensive Polars row validation tests`

---

### Phase 7: Documentation
**Goal**: Document the new feature comprehensively

**Files to modify**:
- `README.md` - Add row validation example
- `docs/usage.md` - Detailed usage guide
- `CHANGELOG.md` - Document new feature

**README.md addition**:
```markdown
### Row Data Validation

Validate DataFrame row data using Pydantic models:

```python
from daffy import df_in, df_out
from pydantic import BaseModel, Field

class ProductValidator(BaseModel):
    name: str
    price: float = Field(gt=0)  # Must be positive
    stock: int = Field(ge=0)    # Must be non-negative

@df_in(
    columns=["name", "price", "stock"],  # Column validation
    row_validator=ProductValidator       # Row data validation
)
@df_out(row_validator=ProductValidator)
def process_products(df):
    df["price"] = df["price"] * 1.1  # 10% markup
    return df
```

Row validation uses modern Pydantic features for performance:
- Batch validation with TypeAdapter (Pydantic 2.4+)
- Automatic NaN to None conversion
- Efficient iteration for both pandas and polars

Install with Pydantic support:
```bash
pip install daffy "pydantic>=2.4.0"
```

Configure validation behavior in pyproject.toml:
```toml
[tool.daffy]
row_validation_max_errors = 10  # Show up to 10 failed rows
row_validation_convert_nans = true  # Auto-convert NaN to None
```
```

**Commit**: `docs: add row validation documentation`

---

## Testing Strategy

### Comprehensive Test Coverage
1. **Both DataFrame libraries**: Test with pandas and polars separately
2. **Index types**: Integer, string, datetime indexes for pandas
3. **Performance**: Benchmark batch vs iterative validation
4. **Edge cases**: Empty DataFrames, all-null columns, very large DataFrames
5. **Error messages**: Verify context is included properly
6. **Configuration**: Test all config options and precedence

### CI Integration
Add to existing CI matrix:
```yaml
- name: Test with Pydantic
  run: |
    uv sync --group test
    uv pip install "pydantic>=2.4.0"
    uv run pytest tests/test_row_validation*.py

- name: Test without Pydantic
  run: |
    uv sync --group test
    uv run pytest tests/ -k "not row_validation"
```

---

## Key Improvements in V2

1. ✅ **Better naming**: `row_validator` instead of `row_model`
2. ✅ **Consistent errors**: Uses `AssertionError` like existing validators
3. ✅ **Modern Pydantic**: TypeAdapter for batch validation, ConfigDict
4. ✅ **Performance first**: Batch validation by default, efficient iteration
5. ✅ **Config earlier**: Phase 3 instead of Phase 5
6. ✅ **Index handling**: Supports all pandas index types
7. ✅ **NaN handling**: Automatic conversion to None
8. ✅ **Both libraries**: Excellent support for pandas and polars
9. ✅ **Clear phases**: Focus on basics before advanced features
10. ✅ **Comprehensive tests**: Each phase thoroughly tested

## Success Metrics

- All tests pass with pandas + pydantic
- All tests pass with polars + pydantic
- Graceful failure without pydantic
- Performance: <100ms overhead for 10k row DataFrame
- Coverage remains above 95%
- Error messages are clear and actionable