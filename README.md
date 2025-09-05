# NEMDataTools

An MIT-licensed Python package for accessing and preprocessing data from the Australian Energy Market Operator (AEMO) for the National Electricity Market (NEM).

## Overview

NEMDataTools provides a clean, efficient interface for:
- Downloading raw data from AEMO's public data sources
- Processing various AEMO data formats
- Managing time series data with appropriate timestamps
- Supporting multiple data tables and report types
- Delivering preprocessed data ready for analysis

This package is designed for researchers, analysts, and developers who need to work with AEMO data under a permissive MIT license.

## Installation

### From PyPI (Recommended)

```bash
pip install nemdatatools
```

### From TestPyPI (Pre-releases)

```bash
pip install --index-url https://test.pypi.org/simple/ nemdatatools
```

### From Source (Development)

```bash
# Clone the repository
git clone https://github.com/ZhipengHe/nemdatatools.git
cd nemdatatools

# Install in development mode with all dependencies
pip install -e ".[dev,docs]"

# Or install just the core package
pip install -e .
```

### Requirements

- Python 3.10 or higher
- pandas, numpy, requests, pyarrow, tqdm

## Quick Start

```python
import nemdatatools as ndt

# Download and process dispatch price data with automatic caching
data = ndt.fetch_data(
    data_type="DISPATCHPRICE",
    start_date="2023/01/01",
    end_date="2023/01/02",
    regions=["NSW1", "VIC1"],
    cache_path="./cache"  # Enable local caching
)

# Data is already processed and standardized
print(f"Downloaded {len(data)} records")
print(data.head())

# Advanced analysis with built-in functions
stats = ndt.calculate_price_statistics(data)
resampled = ndt.resample_data(data, '1H')  # Resample to hourly
windows = ndt.create_time_windows(data, window_size='4H')  # 4-hour windows
```

## Core Features

- **🚀 Complete Data Pipeline**: Download → Extract → Process → Cache → Analyze in one API call
- **📊 Core Data Types**: MMSDM dispatch data, pre-dispatch forecasts, with framework for expansion
- **⚡ Intelligent Caching**: Metadata-based local caching with configurable TTL
- **🔄 Advanced Processing**: Data standardization, time series resampling, statistical analysis
- **⏰ Time-Aware**: Proper AEST timezone handling and dispatch interval management
- **🌏 Region Support**: All NEM regions (NSW1, VIC1, QLD1, SA1, TAS1) with filtering
- **🛡️ Production Ready**: Robust error handling, retry logic, comprehensive testing

## Development Status

NEMDataTools has reached **production readiness** with core functionality complete and thoroughly tested.

### ✅ **Completed Features**

- [x] **Complete Data Pipeline**
    - [x] Multi-source data downloading (MMSDM, pre-dispatch, static)
    - [x] ZIP file extraction and CSV processing
    - [x] Intelligent caching with metadata management
    - [x] End-to-end data standardization and validation

- [x] **Advanced Processing Capabilities**
    - [x] Time series resampling and statistical analysis
    - [x] Price and demand calculation functions
    - [x] Time window creation for analysis
    - [x] AEST timezone and dispatch interval handling

- [x] **Production Infrastructure**
    - [x] Comprehensive error handling and retry logic
    - [x] 79 test functions with 58% coverage
    - [x] Pre-commit hooks with Black, Ruff, MyPy
    - [x] GitHub Actions CI/CD pipeline
    - [x] Type annotations throughout codebase

### 🚧 **In Progress**

- [ ] **Data Type Expansion**: Adding support for remaining MMSDM tables
- [ ] **Documentation**: API reference and advanced usage guides

### 📋 **Tested Data Types**

| Data Type | Status | Description |
|-----------|--------|-------------|
| `DISPATCHPRICE` | ✅ Fully Tested | 5-minute dispatch prices by region |
| `DISPATCHREGIONSUM` | ✅ Fully Tested | 5-minute regional dispatch summary |
| `DISPATCH_UNIT_SCADA` | ✅ Fully Tested | Generator SCADA readings |
| `PREDISPATCHPRICE` | ✅ Fully Tested | Pre-dispatch price forecasts |
| `PRICE_AND_DEMAND` | ✅ Tested | Direct CSV price and demand data |
| `P5MIN_REGIONSOLUTION` | ⚠️ Framework Ready | 5-minute pre-dispatch (implementation complete, testing pending) |
| Static Data Types | ✅ Framework Ready | Registration lists and boundaries |

## Documentation

### Development Guide

Here are some documents to help you get started with developing NEMDataTools:

- **Project Planning**:
    - [Implementation Plan](./docs/dev/implementation-plan.md): Detailed plan for implementing core modules
    - [Project Board](./docs/dev/project-structure.md): Overview of the project structure and milestones
- **Development Workflow**:
    - [Quickstart with UV](./docs/dev/quickstart-with-uv.md): Setting up the development environment with Universal Viewer
    - [UV Integration Guide](./docs/dev/uv-integration.md): Using UV for dependency management
    - [Quickstart with Pre-Commit](./docs/dev/quickstart-with-pre-commit.md): Setting up pre-commit hooks for code quality
    - [Commitizen Guide](./docs/dev/commitizen-guide.md): Using Commitizen for standardized commit messages

### API Reference

Detailed documentation is available at [Documentation (WIP)](https://zhipenghe.me/nemdatatools/).


## API Reference

### Core Functions

```python
# Main data fetching function
data = ndt.fetch_data(
    data_type="DISPATCHPRICE",
    start_date="2023/01/01",
    end_date="2023/01/02",
    regions=["NSW1", "VIC1"],
    cache_path="./cache"
)

# Check available data types
available_types = ndt.get_available_data_types()

# Batch operations
ndt.download_multiple_tables(
    tables=["DISPATCHPRICE", "DISPATCHREGIONSUM"],
    start_date="2023/01/01",
    end_date="2023/01/02"
)

# Advanced analysis
stats = ndt.calculate_price_statistics(data)
resampled = ndt.resample_data(data, '1H')
windows = ndt.create_time_windows(data, window_size='4H')
```

## Contributing

Contributions are welcome! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

NEMDataTools is released under the MIT License. See the [LICENSE](LICENSE) file for details.
