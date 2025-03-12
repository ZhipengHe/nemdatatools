#!/bin/bash
# This script sets up the NEMDataTools project with UV

# Install UV if not already installed
if ! command -v uv &> /dev/null
then
    echo "Installing UV..."
    pip install uv
fi

# Create project directory structure
mkdir -p nemdatatools
cd nemdatatools

# Create src layout
mkdir -p src/nemdatatools
mkdir -p tests
mkdir -p docs
mkdir -p .github/workflows

# Create basic files
touch src/nemdatatools/__init__.py
touch README.md
touch LICENSE
touch .gitignore

# Create UV config file
cat > .uv.toml << 'EOF'
[pip]
resolution = "highest"
upgrade-strategy = "eager"

[venv]
python = "3.10"
EOF

# Create pyproject.toml
cat > pyproject.toml << 'EOF'
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "nemdatatools"
version = "0.1.0"
description = "Tools for accessing and preprocessing AEMO data for the National Electricity Market"
readme = "README.md"
requires-python = ">=3.10"
authors = [
    {name = "Your Name", email = "your.email@example.com"},
]
license = {text = "MIT"}
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
]
dependencies = [
    "requests>=2.25.0",
    "pandas>=1.3.0",
    "numpy>=1.20.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=6.0",
    "pytest-cov>=2.12",
    "black>=25.0.0",
    "mypy>=1.14.0",
    "isort>=6.0.0",
    "pre-commit>=3.3.2",
    "ruff>=0.5.0",
    "commitizen>=4.4.0",
]
docs = [
    "sphinx>=8.0.0",
    "sphinx-rtd-theme>=3.0.0",
    "myst-parser>=4.0.0",
]

[project.urls]
Homepage = "https://github.com/yourusername/nemdatatools"
Documentation = "https://nemdatatools.readthedocs.io/"
"Bug Tracker" = "https://github.com/yourusername/nemdatatools/issues"

[tool.setuptools]
package-dir = {"" = "src"}

[tool.black]
line-length = 88
target-version = ["py310"]

[tool.isort]
profile = "black"
line_length = 88

[tool.mypy]
python_version = "3.10"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true
disallow_incomplete_defs = true

[tool.ruff.lint.extend-per-file-ignores]
"tests/*.py" = [
    "S101", # asserts allowed in tests...
]
"docs/conf.py" = [
    "A001", # allow use `copyright` in config file for sphinx
]
EOF

# Create .pre-commit-config.yaml
cat > .pre-commit-config.yaml << 'EOF'
repos:
-   repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v5.0.0
    hooks:
    -   id: check-yaml
    -   id: end-of-file-fixer
    -   id: trailing-whitespace
    -   id: check-added-large-files
    -   id: check-toml
    -   id: check-merge-conflict
    -   id: debug-statements
    -   id: detect-private-key
    -   id: check-ast
    -   id: check-case-conflict

-   repo: https://github.com/psf/black
    rev: 25.1.0
    hooks:
    -   id: black
        language_version: python3

-   repo: https://github.com/pycqa/isort
    rev: 6.0.1
    hooks:
    -   id: isort
        args: ["--profile", "black"]

-   repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.9.10
    hooks:
    -   id: ruff
        args: [
            "--fix",
            "--line-length=88",
            "--select=E,F,D,I,N,C4,B,A,W,S,COM,RUF",
            "--ignore=E203,D213,D203"
        ]
    -   id: ruff-format
        args: ["--check"]

-   repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.15.0
    hooks:
    -   id: mypy
        additional_dependencies: [types-requests]
        exclude: "tests/"
        args: ["--ignore-missing-imports"]

-   repo: https://github.com/asottile/pyupgrade
    rev: v3.19.1
    hooks:
    -   id: pyupgrade
        args: [--py310-plus]

-   repo: https://github.com/commitizen-tools/commitizen
    rev: v4.4.1
    hooks:
    -   id: commitizen
        stages: [commit-msg]
        additional_dependencies: ['commitizen']

-   repo: https://github.com/econchick/interrogate
    rev: 1.7.0
    hooks:
    -   id: interrogate
        args: [-vv, -i, --fail-under=80]
EOF
cat > .gitignore << 'EOF'
# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# Distribution / packaging
dist/
build/
*.egg-info/

# Unit test / coverage reports
htmlcov/
.tox/
.coverage
.coverage.*
.cache
coverage.xml
*.cover

# Environments
.env
.venv
env/
venv/
ENV/

# Documentation
docs/_build/

# IDE specific files
.idea/
.vscode/
*.swp
*.swo

# UV specific
.uv/

# MacOS specific
.DS_Store
EOF

# Create a basic init file
cat > src/nemdatatools/__init__.py << 'EOF'
"""NEMDataTools: Tools for accessing and preprocessing AEMO data."""

__version__ = "0.1.0"
EOF

# Create virtual environment with UV
echo "Creating virtual environment with UV..."
uv venv

# Activate virtual environment
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    echo "On Windows, activate with: .venv\\Scripts\\activate"
else
    echo "Activating virtual environment..."
    source .venv/bin/activate

    # Install in development mode
    echo "Installing in development mode..."
    uv pip install -e ".[dev,docs]"

    # Setup pre-commit
    echo "Setting up pre-commit..."
    pre-commit install
    pre-commit install --hook-type commit-msg
fi

echo "NEMDataTools project setup with UV is complete!"
echo "Next steps:"
echo "1. If on Windows, activate the environment: .venv\\Scripts\\activate"
echo "2. Begin implementing the core modules in src/nemdatatools/"
echo "3. Write tests in the tests/ directory"
echo "4. Build documentation in the docs/ directory"
