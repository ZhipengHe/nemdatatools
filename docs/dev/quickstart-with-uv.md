# Quick Start Guide: Setting Up NEMDataTools with UV

This guide will help you quickly set up a NEMDataTools development environment
using [UV](https://docs.astral.sh/uv/) for dependency management.

## Prerequisites

- Python 3.11 or higher (UV can install one for you)
- Git (for version control)

## Step 1: Install UV

If you don't have UV installed:

```bash
# Standalone installer (recommended)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or using pip
pip install uv
```

## Step 2: Clone the Project

```bash
git clone https://github.com/ZhipengHe/nemdatatools.git
cd nemdatatools
```

## Step 3: Install Dependencies from the Lockfile

```bash
# Creates .venv, installs the locked dependency tree, and installs
# nemdatatools in editable mode with the dev and docs extras
uv sync --locked --all-extras
```

There is no need to activate the virtual environment: prefix commands with
`uv run` and they execute inside `.venv`. If you prefer an activated shell,
`source .venv/bin/activate` still works.

## Step 4 (Optional): Pin the Interpreter

To pin the Python version used for the environment, write a
`.python-version` file:

```bash
uv python pin 3.11
```

## Step 5: Run Tests

```bash
# Run tests with pytest
uv run pytest

# Run with coverage
uv run pytest --cov=nemdatatools
```

## Step 6: Formatting, Linting, and Commit Checks

The project runs all format/lint/type checks through pre-commit:

```bash
# Install the git hook once
uv run pre-commit install

# Run all checks against the whole tree
uv run pre-commit run --all-files
```

## Step 7: Build Documentation

```bash
uv run sphinx-build -b html docs docs/_build/html
```

## Next Steps

- Refer to the [UV Integration Guide](uv-integration.md) for the day-to-day
  dependency workflow (adding, updating, and locking dependencies)
- CI installs from `uv.lock` with `uv sync --locked`, so commit the updated
  lockfile whenever you change dependencies
