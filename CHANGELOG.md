# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-07-12

### Changed (v0.2 rewrite — breaking)
- Rebuilt the package around AEMO's three-tier publication model
  (Reports CURRENT / Reports ARCHIVE / MMSDM monthly archive). One
  `fetch(table, start, end)` call stitches whichever tiers a date range
  needs; the v0.1 `fetch_data(data_type, ...)` API and its hand-registered
  data types are gone.
- Remote files are discovered by reading directory listings and
  pattern-matching instead of constructing filenames. This absorbs the
  August 2024 `PUBLIC_DVD_*` → `PUBLIC_ARCHIVE#*` filename-era change,
  the DVD-era `_ALL`/numbered split names, and fetches every `FILEnn`
  part of multi-part tables (v0.1 silently fetched only `FILE01`).
- `resample()` replaces `resample_data()` and fixes two correctness bugs:
  aggregation buckets now close/label on the right to match AEMO's
  interval-ending timestamps (v0.1 misaligned every aggregate by one
  interval), and frames holding several regions or units must be grouped
  explicitly instead of being averaged together.

### Added
- Curated catalog across four data families (prices/demand, generation,
  forecasts, bids) with per-era names and known availability gaps;
  ranges crossing a gap raise `AvailabilityGapError` naming the
  substitute table (e.g. the bid tables removed at the 2021
  five-minute-settlement transition).
- `fetch_mmsdm_table()` escape hatch for any of the ~236 MMSDM tables.
- `fetch_price_and_demand()` for the aggregated visualisation-service CSVs.
- Two-layer cache: raw payloads (provenance) plus per-table Parquet
  (fast re-reads).
- `tables()` and `availability()` discovery helpers.

### Fixed (relative to v0.1)
- Plain-HTTP nemweb URLs replaced with HTTPS (v0.1 depended on a 307
  redirect).
- P5MIN tables were unfetchable in both filename eras (missing `_ALL`
  suffix / `#ALL#` segment).
- `ROOFTOPPV_ACTUAL` never matched the real table name
  `ROOFTOP_PV_ACTUAL`.
- REPORTS_CURRENT date filtering contradicting its documentation could
  return an empty DataFrame for same-day ranges.
- The request-range-keyed cache could clip boundary days between two
  adjacent fetches; the artifact-keyed cache cannot (#3).

### Removed
- **Python 3.10 support** — the package now requires Python 3.11+.
- The v0.1 public API: `fetch_data`, `download_multiple_tables`,
  `download_yearly_data`, `calculate_price_statistics`,
  `calculate_demand_statistics`, `create_time_windows`, `resample_data`,
  `check_connection`, `get_available_data_types`.

### Infrastructure
- CI installs from the committed `uv.lock`, workflow actions are pinned
  to commit SHAs, and Dependabot manages dependency updates on a
  monthly cadence.
- CodeRabbit review configuration (advisory, non-blocking).

## [0.1.1rc1] and earlier

### Added (pre-rewrite)
- Project structure setup:
    - Create uv environment (`.uv.toml`, `uv.lock`) and `ptproject.toml`
    - Add pre-commit hooks (`.pre-commit-config.yaml`)
    - Add `CHANGELOG.md` and `README.md` files
    - Development guide in `./docs/dev` directory
    - Create `src`, `docs`, `tests` and `.github` directories
    - Add `.gitignore` file
    - Add MIT License to the project
    - Add `CONTRIBUTING.md` and `CODE_OF_CONDUCT.md` files
- Create GitHub Actions workflow for testing (`.github/workflows/test.yml`)

[Unreleased]: https://github.com/ZhipengHe/nemdatatools/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/ZhipengHe/nemdatatools/compare/v0.1.1rc1...v0.2.0
