"""DAFFY DataFrame Column Validator.

In projects using Pandas, it's very common to have functions that take Pandas DataFrames as input or produce them as
output. It's hard to figure out quickly what these DataFrames contain. This library offers simple decorators to
annotate your functions so that they document themselves and that documentation is kept up-to-date by validating
the input and output on runtime.
"""

__version__ = "0.5.0"

from .decorators import df_in  # noqa
from .decorators import df_log  # noqa
from .decorators import df_out  # noqa
