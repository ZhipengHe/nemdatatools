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

[TODO: Add installation instructions]

```bash
pip install nemdatatools
```

## Quick Start

[TODO: Add a simple example here]

```python
from nemdatatools import downloader, processor

# Download dispatch price data
data = downloader.fetch_data(
    data_type="DISPATCHPRICE",
    start_date="2023/01/01",
    end_date="2023/01/02",
    regions=["NSW1", "VIC1"]
)

# Process the data
processed_data = processor.standardize(data)

# Analyze
print(processed_data.head())
```

## Core Features

- **Flexible Data Access**: Download historical or recent AEMO data
- **Efficient Caching**: Local caching to minimize redundant downloads
- **Data Preprocessing**: Clean and standardize raw AEMO data formats
- **Time Utilities**: Handle dispatch intervals, trading intervals, and forecast horizons
- **Region Filtering**: Focus on specific NEM regions
- **Data Type Support**: Access different data types (prices, demand, forecasts, etc.)

## Development Roadmap

The development of NEMDataTools is divided into several phases and milestones. The roadmap is subject to change based on community feedback and project requirements. See the [Project Board](./docs/dev/project-structure.md) for more details.

- [ ] **Phase 1: Project Setup**
    - [x] **Milestone 1:** Development environment setup
        - [x] Project structure setup
        - [x] Base configuration handling
    - [ ] **Milestone 2:** Core Module Skeletons
        - [x] Basic module structure
        - [x] Testing framework setup
        - [x] Setup Documentation with Sphinx
        - [x] GitHub Actions workflows

- [ ] **Phase 2: Core Functionality Implementation**
    - [ ] **Milestone 3:** Time Utilities and Cache Management
        - [ ] Time utilities implementation
        - [ ] Local cache management
    - [ ] **Milestone 4:** Data Downloading
        - [ ] AEMO URL and endpoint mapping
        - [ ] Core data fetching module
        - [ ] Test downloading

- [ ] **Phase 3: Data Processing**
    - [ ] **Milestone 5:** Basic Data Processing
        - [ ] CSV/ZIP/XML parsers for AEMO formats
        - [ ] Data standardization utilities
    - [ ] **Milestone 6:** Advanced Data Processing
        - [ ] Time series processing functions
        - [ ] Implement Predispatch Handlers
        - [ ] Statistical analysis functions

- [ ] **Phase 4: Documentation and Examples**
    - [ ] **Milestone 7**
        - [ ] API documentation
        - [ ] Usage examples
        - [ ] Installation and setup guide

- [ ] **Phase 5: Quality Assurance and Release (Milestone 8)**
    - [ ] **Milestone 8**
        - [ ] Code quality checks
        - [ ] Performance optimization
        - [ ] Comprehensive test suite
        - [ ] Prepare for initial release
        - [ ] CI/CD pipeline setup
        - [ ] Release

- [ ] **Phase 6: Continuous Development**
    - [ ] **Keep ongoing development**
        - [ ] Monitor issues and feature requests
        - [ ] Expand Supported Data Types
        - [ ] Advanced Features (Visualizations, ML models)
        - [ ] Community Building
        - [ ] Improve Documentation

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
    - [Commitzen Guide](./docs/dev/commitzen-guide.md): Using Commitizen for standardized commit messages

### API Reference

Detailed documentation is available at [TODO: Add documentation link]().


## Supported Data Types

NEMDataTools currently supports the following AEMO data types:

| Data Type | Description | Status |
|-----------|-------------|--------|
| DISPATCHPRICE | Dispatch price data | Planned |
| DISPATCHREGIONSUM | Regional dispatch summary | Planned |
| PREDISPATCH | Pre-dispatch forecasts | Planned |
| P5MIN | 5-minute pre-dispatch | Planned |

## Contributing

Contributions are welcome! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

NEMDataTools is released under the MIT License. See the [LICENSE](LICENSE) file for details.
