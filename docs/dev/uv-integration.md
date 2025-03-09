# Using UV for Dependency Management in NEMDataTools

[UV](https://github.com/astral-sh/uv) is a fast, reliable Python package installer and resolver written in Rust. This guide explains how to leverage UV to manage dependencies in the NEMDataTools project.

## Installing UV

Before setting up the project, you need to install UV:

```bash
# Install UV using pip
pip install uv

# Or using curl (alternative method)
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Project Setup with UV

Follow these steps to set up NEMDataTools using UV:

### 1. Create Project Structure

```bash
# Create project directory
mkdir -p nemdatatools
cd nemdatatools

# Create initial directory structure
mkdir -p src/nemdatatools tests docs .github/workflows
```

### 2. Create Virtual Environment with UV

```bash
# Create a virtual environment in .venv
uv venv

# Activate the virtual environment
# On Linux/macOS:
source .venv/bin/activate
# On Windows:
# .venv\Scripts\activate
```

### 3. Initialize Dependencies with UV

After setting up your `pyproject.toml` file:

```bash
# Install the project in development mode with all dependencies
uv pip install -e ".[dev,docs]"
```

### 4. UV Configuration

Create a `.uv.toml` file in your project root for UV-specific configuration:

```toml
[pip]
resolution = "highest"
upgrade-strategy = "eager"

[venv]
python = "3.10"  # Specify your preferred Python version
```

## Development Workflow with UV

### Installing New Dependencies

When you need to add new dependencies:

1. Add them to `pyproject.toml`
2. Run:
   ```bash
   uv pip install -e ".[dev,docs]"
   ```

### Updating Dependencies

To update all dependencies to their latest compatible versions:

```bash
uv pip install --upgrade -e ".[dev,docs]"
```

### Managing Specific Versions

UV handles specific versions well. When you need a specific version in your pyproject.toml:

```toml
dependencies = [
    "requests>=2.25.0,<3.0.0",
    "pandas==1.3.0",
]
```

### Locking Dependencies

UV can generate a lock file to ensure reproducible installations:

```bash
uv pip freeze > requirements.lock
```

To install from the lock file:

```bash
uv pip install -r requirements.lock
```

## Integration with CI/CD

For GitHub Actions, include these steps in your workflow file:

```yaml
- name: Set up Python
  uses: actions/setup-python@v4
  with:
    python-version: '3.10'

- name: Install UV
  run: pip install uv

- name: Install dependencies
  run: uv pip install -e ".[dev]"

- name: Run tests
  run: pytest
```

## Performance Considerations

UV offers several performance benefits for NEMDataTools:

1. **Faster installation:** Dependencies install much faster, especially on CI/CD
2. **More reliable resolution:** Fewer dependency conflicts
3. **Reduced environment sizes:** More efficient management of dependencies

## Troubleshooting

If you encounter issues with UV:

1. Check that your `.uv.toml` configuration is correct
2. Try running with the verbose flag: `uv pip install -v -e ".[dev,docs]"`
3. Clear the UV cache if needed: `uv cache clear`
