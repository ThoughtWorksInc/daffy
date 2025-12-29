"""DAFFY DataFrame Column Validator.

In projects using Pandas, it's very common to have functions that take Pandas DataFrames as input or produce them as
output. It's hard to figure out quickly what these DataFrames contain. This library offers simple decorators to
annotate your functions so that they document themselves and that documentation is kept up-to-date by validating
the input and output on runtime.
"""

from .checks import CheckName
from .decorators import df_in, df_log, df_out
from .validation import ColumnConstraints

__all__ = ["df_in", "df_out", "df_log", "ColumnConstraints", "CheckName"]
