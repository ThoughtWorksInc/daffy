# Development Guide

## Setup

To set up the development environment, clone the repository, install dependencies with Poetry and run tests with PyTest:

```sh
poetry install
poetry shell
pytest
```

## Code Quality

To enable linting on each commit, run `pre-commit install`. After that, your every commit will be checked with `ruff` and `mypy`.

## Testing

The project uses pytest for testing. The tests are organized into separate files for each decorator:

- `test_df_in.py`: Tests for the df_in decorator
- `test_df_out.py`: Tests for the df_out decorator
- `test_df_log.py`: Tests for the df_log decorator
- `test_decorators.py`: Tests for decorator combinations
- `test_config.py`: Tests for configuration functionality

Run the tests with:

```sh
poetry run pytest
```

To get a coverage report:

```sh
poetry run pytest --cov=daffy
```

## Contributing

Contributions are welcome! Here are some guidelines:

1. Create a feature branch from the main branch
2. Add tests for new functionality
3. Make sure all tests pass before submitting a PR
4. Update documentation as necessary

Please include tests with pull requests to ensure that your changes work as expected.