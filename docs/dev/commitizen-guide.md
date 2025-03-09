# Commitizen Conventional Commit Guide

This guide explains how to use commitizen with NEMDataTools to follow the [Conventional Commits](https://www.conventionalcommits.org/) specification.

## What is Commitizen?

Commitizen is a tool that helps you write standardized commit messages according to the Conventional Commits format. When used with pre-commit, it ensures all commit messages follow the same format.

## Commit Message Format

```
type(optional scope): description

[optional body]

[optional footer(s)]
```

### Types

The type must be one of the following:

- **feat**: A new feature
- **fix**: A bug fix
- **docs**: Documentation changes
- **style**: Code style changes (whitespace, formatting, etc.)
- **refactor**: Code changes that neither fix bugs nor add features
- **perf**: Performance improvements
- **test**: Adding or fixing tests
- **build**: Changes to the build system or dependencies
- **ci**: Changes to CI configuration files and scripts
- **chore**: Other changes that don't modify src or test files

### Scope

The scope is optional and should be a noun describing the section of the codebase affected:

- `downloader`
- `cache`
- `processor`
- `timeutils`
- `tests`
- `docs`

### Description

The description is a short summary of the code changes. It should:
- Use imperative, present tense (e.g., "change" not "changed" or "changes")
- Not capitalize the first letter
- Not end with a period

### Examples

```
feat(downloader): add retry mechanism for HTTP requests
```

```
fix(cache): resolve file permission issue when creating cache directory
```

```
docs: update installation instructions with UV commands
```

```
refactor(processor): simplify data standardization function
```

## Creating Commits with Commitizen

### Interactive Mode

Use commitizen in interactive mode:

```bash
cz commit
```

This will guide you through creating a valid commit message by asking for:
1. The type of change
2. The scope (optional)
3. A short description
4. A longer description (optional)
5. Breaking changes (optional)
6. Issues closed (optional)

### Conventional git commit

You can also write commits directly using the conventional format:

```bash
git commit -m "feat(downloader): add support for P5MIN data type"
```

Pre-commit will validate if the message conforms to the conventional format.

## Breaking Changes

For breaking changes, add a `!` after the type/scope and include a `BREAKING CHANGE:` footer:

```
feat(api)!: remove deprecated endpoints

BREAKING CHANGE: The following endpoints have been removed as they were deprecated in v0.5.0
```

## Referencing Issues

Reference issues at the end of the commit message:

```
fix(processor): handle missing values in CSV data

Closes #123
```

## Benefits of Using Commitizen

1. **Standardized history**: Makes the project history readable and structured
2. **Automated versioning**: Can be used to automatically determine version numbers
3. **Automated changelog**: Simplifies generating changelogs from commit messages
4. **Clarity**: Clearly communicates the intent of changes

## Commitizen CLI Commands

```bash
# Create a commit interactively
cz commit

# Check if the last commit message follows the convention
cz check --rev-range HEAD

# Bump version based on commits (when ready for release)
cz bump
```

## Integration with NEMDataTools

Commitizen is integrated with NEMDataTools through:

1. **pre-commit hook**: Validates commit messages automatically
2. **pyproject.toml**: Configuration for commitizen
3. **Development workflow**: Used throughout the project development

## Customizing Commitizen

You can customize commitizen by adding a configuration to your `pyproject.toml`:

```toml
[tool.commitizen]
name = "cz_conventional_commits"
version = "0.1.0"
tag_format = "v$version"
```

This ensures everyone on the project follows the same commit conventions.
