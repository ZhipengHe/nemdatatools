# Quick Start Guide: Setting Up NEMDataTools with UV

This guide will help you quickly set up the NEMDataTools project using UV for dependency management.

## Prerequisites

- Python 3.7 or higher
- Git (for version control)

## Step 1: Install UV

If you don't have UV installed:

```bash
# Install UV using pip
pip install uv

# Or using curl (alternative method)
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Step 2: Clone and Setup the Project

```bash
# Clone the repository (or create it if it doesn't exist)
git clone https://github.com/yourusername/nemdatatools.git
# Or: mkdir nemdatatools

cd nemdatatools

# Create project structure if starting from scratch
mkdir -p src/nemdatatools tests docs .github/workflows
```

## Step 3: Create and Activate Virtual Environment

```bash
# Create a virtual environment
uv venv

# Activate the virtual environment
# On Linux/macOS:
source .venv/bin/activate
# On Windows:
# .venv\Scripts\activate
```

## Step 4: Setup Configuration Files

Create the following configuration files:

### 1. `.uv.toml`

```toml
[pip]
resolution = "highest"
upgrade-strategy = "eager"

[venv]
python = "3.10"  # Specify your preferred Python version
```

### 2. `pyproject.toml`

See the full `pyproject.toml` file in the project artifacts.

## Step 5: Install Dependencies with UV

```bash
# Install the project in development mode with all dependencies
uv pip install -e ".[dev,docs]"
```

## Step 6: Implement Core Modules

Start implementing the core modules:

1. Create/update `src/nemdatatools/__init__.py` with version info
2. Add `downloader.py`, `cache.py`, `timeutils.py`, and `processor.py` modules
3. Write tests in the `tests/` directory

## Step 7: Run Tests

```bash
# Run tests with pytest
pytest

# Run with coverage
pytest --cov=nemdatatools
```

## Step 8: Code Formatting and Linting

```bash
# Format code
black src tests

# Sort imports
isort src tests

# Type checking
mypy src
```

## Step 9: Build Documentation

```bash
# Install documentation dependencies
uv pip install -e ".[docs]"

# Build documentation
cd docs
sphinx-build -b html . _build/html
```

## Automation Script

For your convenience, a setup automation script is included with the project artifacts to quickly bootstrap the project structure with proper UV configuration.

```bash
# Make the script executable
chmod +x setup_nemdatatools_with_uv.sh

# Run the script
./setup_nemdatatools_with_uv.sh
```

## Next Steps

- Implement the components according to the implementation plan
- Follow the development workflow using UV for dependency management
- Refer to the UV Integration Guide for more details on using UV effectively

This quick start guide helps you rapidly set up the NEMDataTools project with UV for efficient dependency management.
