# Using UV for Dependency Management in NEMDataTools

[UV](https://docs.astral.sh/uv/) is a fast, reliable Python package installer
and resolver written in Rust. This guide explains how NEMDataTools uses UV and
its lockfile (`uv.lock`) to manage dependencies.

## Installing UV

```bash
# Standalone installer (recommended)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or using pip
pip install uv
```

## Environment Setup

The repository commits a `uv.lock` file describing the exact, fully resolved
dependency tree. One command builds the environment from it:

```bash
uv sync --locked --all-extras
```

This creates `.venv`, installs the locked versions of all runtime, `dev`, and
`docs` dependencies, and installs `nemdatatools` in editable mode. `--locked`
fails if `uv.lock` is out of date with `pyproject.toml` instead of silently
re-resolving — the same guarantee CI relies on.

Run tools through the environment with `uv run` (no activation needed):

```bash
uv run pytest
uv run pre-commit run --all-files
```

To pin the interpreter version for the environment, use
`uv python pin 3.11`, which writes a `.python-version` file. UV behavior
settings, if ever needed, belong in a `[tool.uv]` section of
`pyproject.toml` or a `uv.toml` file.

## Development Workflow

### Adding a Dependency

1. Add it to `pyproject.toml` (under `dependencies` or the appropriate
   optional extra), or run `uv add <package>` to do both steps at once
2. Re-lock and sync:

   ```bash
   uv lock
   uv sync --locked --all-extras
   ```

3. Commit the `pyproject.toml` and `uv.lock` changes together

### Updating Dependencies

```bash
# Update everything to the latest versions allowed by pyproject.toml
uv lock --upgrade

# Or update a single package
uv lock --upgrade-package pandas

# Then apply the new lock to your environment
uv sync --locked --all-extras
```

Routine updates arrive automatically: Dependabot opens a monthly PR that
bumps `uv.lock` (see `.github/dependabot.yml`), and CI tests the bumped tree
because it installs with `--locked`.

### Managing Version Constraints

Constraints live in `pyproject.toml`; the lockfile records the exact versions
chosen within them:

```toml
dependencies = [
    "requests>=2.25.0,<3.0.0",
    "pandas==1.3.0",
]
```

## Integration with CI/CD

The GitHub Actions workflows install from the lockfile the same way. The
essential steps (see `.github/workflows/tests.yml` for the real thing — the
repository SHA-pins its actions):

```yaml
- name: Install uv and Python
  uses: astral-sh/setup-uv@v6
  with:
    python-version: '3.11'
    enable-cache: true
    cache-dependency-glob: "uv.lock"

- name: Install locked dependencies
  run: uv sync --locked --extra dev

- name: Run tests
  run: uv run --no-sync pytest
```

## Performance Considerations

UV offers several performance benefits for NEMDataTools:

1. **Faster installation:** Dependencies install much faster, especially on
   CI/CD, where wheels are served from UV's cache
2. **More reliable resolution:** One resolver, one lockfile, no drift between
   local and CI environments
3. **Reproducibility:** `uv sync --locked` produces the same environment on
   every machine

## Troubleshooting

If you encounter issues with UV:

1. `uv sync --locked` failing with a lock mismatch means `pyproject.toml`
   changed without re-locking — run `uv lock` and commit the result
2. Try running with the verbose flag: `uv sync --locked --all-extras -v`
3. Clear the UV cache if needed: `uv cache clean`
