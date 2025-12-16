# plan.md (v2) — Combined roadmap (Daffy + Pandera-inspired)

This plan merges the earlier roadmap with the new research. It keeps Daffy’s core principles:
- **Low barrier to integrate/remove** (decorators on normal functions)
- **No custom DataFrame types** (plain pandas DataFrames in user code)
- Defaults remain unchanged unless users opt in via decorator flags or `pyproject.toml`.

---

## 1) What I think about the new research (quick take)

I agree strongly with:
- **Nullable validation**: high-value, easy, very mainstream.
- **Uniqueness validation**: high-value, easy, very mainstream.
- **Basic value checks** (small subset): high-value, moderate effort, worth doing.

I’d treat these as “validation” (aligned with Daffy).

I’m more cautious about:
- **strict='filter'** and **drop_invalid=True**: both *transform* user data. They can still fit Daffy if they are **explicitly opt-in** and **default to non-invasive behavior** (copy or “validate only”).
- **Ordered columns**: useful in exports/CSV APIs, but less common than nullable/unique. Still easy enough to include.

I’d keep **schema inference** as a later “nice-to-have”.

---

## 2) Recommended “next 5” to implement (value vs effort)

### 1. Nullable column validation (NEW)
- Opt-in per-column: `nullable: bool` (default True).
- Project default optional (`[tool.daffy] nullable_default = true`).

### 2. Uniqueness validation (NEW)
- `unique=["col"]` and composite `unique=[["a","b"]]`.

### 3. Optional columns (NEW)
- Per-column `required: bool` or per-decorator `optional=[...]` (choose one unified approach; see below).

### 4. Basic vectorized value checks (NEW)
- Minimal ops: `gt/ge/lt/le/isin/notnull/str_regex/between`.

### 5. Validation modes + lazy aggregation (NEW)
- `validation_mode = "error" | "warn" | "off"` + `lazy=true|false`.

**Near-next (phase 2 / optional, more “transformative”):**
- `strict="filter"` (drop extra cols) — opt-in, and ideally on a copy.
- `coerce=True` (type coercion) — opt-in, on a copy by default.
- `drop_invalid=True` (row filtering) — opt-in, and safest if it returns a filtered df (or filters on a copy).

---

## 3) Public API proposal (backwards-compatible)

### 3.1 Keep current `columns=` formats working
Daffy currently supports:
- `columns=["Brand", "Price"]`
- `columns={"Brand": "object", "Price": "int64"}`

This must continue to work unchanged.

### 3.2 Extend dict values to allow rich column specs
Allow:
```python
columns={
  "price": {"dtype": "float64", "nullable": False, "checks": {"gt": 0}},
  "status": {"dtype": "object", "checks": {"isin": ["active","pending","closed"]}},
  "optional_col": {"dtype": "int64", "required": False},
}
```

Rules:
- If value is a **string** → dtype-only (existing behavior).
- If value is a **dict** → supports:
  - `dtype: str | None`
  - `nullable: bool` (default: True)
  - `required: bool` (default: True)
  - `checks: dict[str, object]` (optional)

This keeps all the “extra flags” in one place (matches the new research’s suggested style).

### 3.3 Decorator-level args (for cross-column features)
Add to `df_in/df_out` (all `None` = “use config default”):
- `strict: bool | Literal["filter"] | None`
- `ordered: bool | None` *(only meaningful when columns are list or dict order is intended)*
- `unique: list[str] | list[list[str]] | None`
- `lazy: bool | None`
- `on_error: Literal["error","warn","off"] | None`
- (optional later) `coerce: bool | None`, `coerce_copy: bool | None`
- (optional later) `drop_invalid: bool | None`, `drop_invalid_copy: bool | None`

---

## 4) `pyproject.toml` config additions

Extend `[tool.daffy]`:

```toml
[tool.daffy]
# existing
strict = false

# new (phase 1)
validation_mode = "error"   # "error" | "warn" | "off"
lazy = false
nullable_default = true
checks_max_errors = 5
unique_max_errors = 5
ordered = false

# optional later (phase 2)
coerce = false
coerce_copy = true
strict_filter_copy = true
drop_invalid = false
drop_invalid_copy = true

# existing
row_validation_max_errors = 5
row_validation_convert_nans = true
```

Decorator args always override config if provided.

---

## 5) Internal refactor (do first)

### 5.1 Central validation pipeline
Create one internal function used by both `df_in` and `df_out`:

```python
def validate_df(
    df,
    *,
    columns_spec,
    strict,
    ordered,
    unique,
    lazy,
    context,
    cfg,
):
    # returns (maybe_transformed_df, issues)
```

### 5.2 Issue and error types
Introduce:
- `Issue(code, message, details={...})`
- `DaffyValidationError(issues, context)` (subclass `AssertionError`)

### 5.3 Handling modes: error/warn/off
Decorator wrapper should:
- If mode == off: skip validation, call function.
- Else run validation and:
  - warn: `warnings.warn(msg, stacklevel=3)`
  - error: raise

---

## 6) Feature specs + implementation notes

## 6.1 Nullable validation (phase 1)

### Usage
```python
@df_in(columns={"price": {"dtype": "float64", "nullable": False}})
def f(df): ...
```

### Behavior
- If nullable is False → fail if any `isna()` in that column.
- For regex keys, apply to all matched columns.
- Respect `required=False`: if column missing, skip nullable check.

### Implementation
- After existence check (and optional coercion if ever enabled), compute:
  - `na_count = df[col].isna().sum()`
  - if `na_count > 0` → issue.

### Error message suggestion
- `Column 'price' in function 'f' parameter 'df' contains 3 null values but nullable=False`

Tests:
- nullable false with NaN → error
- nullable true with NaN → ok
- missing optional column with nullable false → ok

---

## 6.2 Uniqueness validation (phase 1)

### Usage
```python
@df_in(columns=["user_id"], unique=["user_id"])
def f(df): ...

@df_in(unique=[["order_id","line_item"]])
def g(df): ...
```

### Behavior
- For each uniqueness spec:
  - if any duplicates → fail
- Report up to `unique_max_errors` duplicate keys or row indices.

### Implementation
- `dupes = df.duplicated(subset=subset_cols, keep=False)`
- if any: capture indices or example duplicate values.

Tests:
- simple unique column
- composite unique
- works with strict and ordered flags

---

## 6.3 Optional columns (phase 1)

Prefer **one** approach:
- **Option A (recommended):** per-column `required=False` (inside rich column spec)
- Option B: decorator `optional=[...]`

**Recommendation:** Option A, because it composes naturally with `nullable` and `checks`.

### Usage
```python
@df_in(columns={
  "discount": {"dtype": "float64", "required": False},
})
def f(df): ...
```

Tests:
- required=False and missing → ok
- required=True and missing → error
- present but dtype wrong → error

---

## 6.4 Basic vectorized checks (phase 1)

### Usage
```python
@df_in(columns={
  "price": {"dtype": "float64", "checks": {"gt": 0}},
  "status": {"dtype": "object", "checks": {"isin": ["active","pending","closed"]}},
  "name": {"dtype": "object", "checks": {"str_regex": r"^[A-Za-z].+"}},
})
def f(df): ...
```

### Supported ops (v1)
- `gt`, `ge`, `lt`, `le`, `eq`, `ne`
- `between`: (lo, hi) inclusive
- `isin`: list/set
- `notnull`: true
- `str_regex`: pattern

### Implementation
- For each column + checks:
  - compute boolean pass mask vectorized
  - if any failures, record up to `checks_max_errors` failing indices
- Regex column keys expand to multiple columns.

Tests:
- each op + max errors
- checks skipped if column is missing and `required=False`
- checks after dtype validation (or before, but dtype mismatch should be reported clearly)

---

## 6.5 Lazy aggregation + validation modes (phase 1)

### Usage
```python
@df_in(columns=[...], lazy=True, on_error="warn")
def f(df): ...
```

### Behavior
- `lazy=False` default: fail-fast (but keep current “missing columns: [...]” aggregation).
- `lazy=True`: accumulate issues from:
  - missing columns
  - dtype mismatches
  - nullable violations
  - uniqueness violations
  - vectorized checks
  - row validation summary (single issue)

### Implementation
- All validators return list[Issue].
- If lazy: return all issues; else raise on first (or return first).

---

## 7) Transformative features (phase 2) — only if you want them

These are useful but more “Pandera-like” in that they can modify data.

### 7.1 strict="filter"
- If strict is `"filter"`, drop extra columns.
- Default should remain strict=True/False behavior.
- Best practice: apply on a copy (config `strict_filter_copy=true`).

### 7.2 coerce=True
- Attempt `astype` conversion for columns with dtype.
- Do on copy by default.

### 7.3 drop_invalid=True (row_validator)
- If enabled, filter out rows that fail Pydantic validation.
- Warning: changes semantics; document clearly.
- Safest implementation: filter on copy and log/warn about drop count.

---

## 8) Milestones (implementation order)

### Milestone 1 — Refactor (must)
- [ ] `Issue`, `ValidationContext`, `DaffyValidationError`
- [ ] central `validate_df` pipeline
- [ ] mode handling (error/warn/off) + lazy

### Milestone 2 — Nullable + Optional + Checks (high value)
- [ ] rich column spec parsing
- [ ] required/nullable logic
- [ ] minimal vectorized checks

### Milestone 3 — Uniqueness + Ordered
- [ ] `unique` validation
- [ ] `ordered=True` validation for list / ordered dict keys

### Milestone 4 — (optional) strict="filter", coerce
- [ ] implement and document as transformative opt-ins

### Milestone 5 — (optional) drop_invalid + schema inference
- [ ] only if demand appears

---

## 9) Testing checklist (pytest)

- Backwards compatibility:
  - old `columns=[...]` and `columns={col: dtype}` behave identically
- New features:
  - nullable false
  - required false
  - checks ops
  - unique and composite unique
  - ordered
  - lazy aggregation
  - warn/off modes

---

## 10) Acceptance criteria

- No breaking changes to existing Daffy usage patterns.
- All new behaviors are opt-in or default-configurable.
- Error messages still include:
  - function name
  - input vs output (parameter vs return)
  - parameter name when applicable
- Transformative behaviors (filter/coerce/drop) are clearly labeled and off by default.

