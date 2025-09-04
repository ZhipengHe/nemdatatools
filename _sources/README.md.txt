# NEMDataTools Documentation

An MIT-licensed Python package for accessing and preprocessing data from the Australian Energy Market Operator (AEMO) for the National Electricity Market (NEM).

## Overview

NEMDataTools provides a production-ready interface for:
- **Complete data pipeline**: Download → Extract → Process → Cache → Analyze
- **Multi-source support**: MMSDM, pre-dispatch, and static data
- **Advanced processing**: Time series resampling, statistical analysis
- **Intelligent caching**: Metadata-based local caching with configurable TTL
- **Production features**: Error handling, retry logic, comprehensive testing

This package is designed for researchers, analysts, and developers who need reliable access to AEMO data.

## Installation

### From Source (Recommended)

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
- [ ] **Package Distribution**: PyPI release preparation

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

## Documentation Structure

- **Development Guides**: Setup instructions and development workflow
- **API Reference**: Coming soon - detailed function documentation
- **Examples**: Coming soon - working code examples and tutorials

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

## License

NEMDataTools is released under the MIT License.
