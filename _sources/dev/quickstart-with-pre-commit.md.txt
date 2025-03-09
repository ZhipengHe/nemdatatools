# Adding pre-commit to NEMDataTools

Pre-commit is a fantastic tool that runs checks before each commit, ensuring code quality and consistency. This guide will show you how to integrate pre-commit with your NEMDataTools project, specifically working with UV for dependency management.

## Step 1: Update `pyproject.toml` to include pre-commit

First, add pre-commit to your development dependencies in `pyproject.toml`:

```toml
[project.optional-dependencies]
dev = [
    "pytest>=6.0",
    "pytest-cov>=2.12",
    "black>=21.5b2",
    "mypy>=0.812",
    "isort>=5.9.1",
    "pre-commit>=3.3.2",  # Add this line
    "flake8>=6.0.0",      # Add this line
    "commitizen>=3.2.2",  # Add this line
]
```

## Step 2: Install pre-commit using UV

Install the updated development dependencies:

```bash
# Make sure your virtual environment is activated
uv pip install -e ".[dev]"
```

## Step 3: Create a pre-commit configuration

Create a `.pre-commit-config.yaml` file in the root of your project with the configuration provided in the artifacts.

This configuration includes:
- Basic file checks (YAML, TOML, trailing whitespace, etc.)
- Code formatting with Black
- Import sorting with isort
- Style checking with flake8
- Type checking with mypy
- Python upgrades with pyupgrade
- Commit message formatting with commitizen

## Step 4: Install the pre-commit hooks

```bash
# Install the pre-commit hooks
pre-commit install

# Also install the commit message hook
pre-commit install --hook-type commit-msg
```

## Step 5: Run pre-commit manually (optional)

You can run pre-commit manually on all files:

```bash
pre-commit run --all-files
```

## Using pre-commit with UV

When you need to update pre-commit or its dependencies:

```bash
# Update dependencies
uv pip install --upgrade -e ".[dev]"

# Update pre-commit hooks
pre-commit autoupdate
```

## Integration with your development workflow

1. **Local Development**: Pre-commit will automatically run before each commit
2. **CI/CD Pipeline**: Add pre-commit to your GitHub Actions workflow

Example GitHub Actions step:

```yaml
- name: Run pre-commit
  run: |
    uv pip install pre-commit
    pre-commit run --all-files
```

## Customizing pre-commit configuration

You can customize the `.pre-commit-config.yaml` file to suit your needs:

1. **Add hooks**: There are [many pre-commit hooks](https://pre-commit.com/hooks.html) available
2. **Adjust parameters**: Modify hook arguments to suit your project
3. **Skip hooks**: Use `SKIP=hook_id git commit` to skip specific hooks

## Commit message convention with commitizen

The provided configuration includes commitizen, which enforces a standard commit message format. Commits will follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
type(scope): description

[optional body]

[optional footer(s)]
```

Examples:
- `feat: add new downloader function`
- `fix(cache): resolve issue with file locking`
- `docs: update README with UV instructions`

## Troubleshooting pre-commit

If you encounter issues:

1. **Hooks failing**: Read the error message and fix the issue
2. **Skip hooks temporarily**: `SKIP=flake8 git commit -m "message"`
3. **Update hooks**: `pre-commit autoupdate`
4. **Reinstall hooks**: `pre-commit uninstall && pre-commit install`

## Additional Tips

1. **Editor Integration**: Many editors have pre-commit plugins
2. **Automated fixes**: Most hooks will automatically fix issues when possible
3. **Performance**: Some hooks can be slow (like mypy) - you can exclude them for rapid development
