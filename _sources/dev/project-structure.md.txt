# NEMDataTools Project Board

This guide outlines the project structure and implementation plan for the NEMDataTools Python package. The project is designed to provide a comprehensive toolkit for accessing, processing, and analyzing Australian Energy Market Operator (AEMO) data.

> **Note**: This guide just provides a basic structure and setup for a Python package project. You may need to adapt it based on your specific requirements.


## Phase 1: Project Setup (Milestones 1-2)

### Milestone 1: Development Environment Setup

1. **Install UV**
   - `pip install uv` or use the recommended curl installation method
   - Configure UV with a `.uv.toml` file

2. **Create Project Structure**
   - Initialize git repository
   - Create directory structure following the src layout:
     ```
     nemdatatools/
     ├── .github/workflows/
     ├── docs/
     ├── src/nemdatatools/
     ├── tests/
     ├── .gitignore
     ├── LICENSE
     ├── pyproject.toml
     ├── README.md
     ├── .uv.toml
     ```

3. **Configure Packaging**
   - Create `pyproject.toml` with minimal dependencies
   - Create virtual environment: `uv venv`
   - Activate the environment: `source .venv/bin/activate`
   - Install in development mode: `uv pip install -e ".[dev]"`

### Milestone 2: Core Module Skeletons

1. **Implement Basic Module Structure**
   - Create `__init__.py` with version info
   - Add skeleton implementations for:
     - `downloader.py`
     - `cache.py`
     - `timeutils.py`
     - `processor.py`

2. **Set Up Testing Framework**
   - Configure pytest with coverage
   - Create basic test files
   - Set up GitHub Actions workflows

## Phase 2: Core Functionality Implementation (Milestones 3-4)

### Milestone 3: Time Utilities and Cache Management

1. **Implement Time Utilities**
   - Date parsing and formatting
   - Interval generation
   - Time period handling for AEMO data types
   - Forecast horizon calculations
   - Write comprehensive tests

2. **Implement Cache Management**
   - Cache directory structure
   - Metadata tracking
   - Cache lookup and retrieval
   - Cache invalidation
   - Write tests for caching logic

### Milestone 4: Data Downloading

1. **Map AEMO Data Sources**
   - Identify public endpoints for each data type
   - Create URL templates
   - Implement data source mapping

2. **Implement Downloader**
   - HTTP request handling with retries
   - Error handling
   - Authentication (if needed)
   - Input validation
   - Integration with cache
   - Batch downloading for multiple regions

3. **Test Downloading**
   - Create mock responses for testing
   - Implement end-to-end tests
   - Verify cache integration


## Phase 3: Data Processing (Milestones 5-6)

### Milestone 5: Basic Data Processing

1. **Implement Data Standardization**
   - Column normalization
   - Data type conversion
   - Date parsing
   - Missing value handling

2. **Data Type Handlers**
   - DISPATCHPRICE handler
   - DISPATCHREGIONSUM handler
   - Write tests for each handler

### Milestone 6: Advanced Processing

1. **Implement Time Series Processing**
   - Resampling and aggregation
   - Rolling statistics
   - Time-based filtering
   - Region-based processing
   - Tests for time series functions

2. **Implement Predispatch Handlers**
   - PREDISPATCH processing
   - P5MIN processing
   - Tests for forecast data handling

23. **Statistical Functions**
   - Price statistics
   - Demand statistics
   - Data aggregation utilities
   - Visualization helpers

## Phase 4: Documentation and Examples (Milestone 7)

1. **API Documentation**
   - Set up Sphinx (or other documentation tool)
   - Document all public functions
   - Create API reference

2. **Usage Examples**
   - Basic usage examples
   - Advanced usage examples
   - Jupyter notebooks with real-world data

3. **Installation and Setup Guide**
   - UV-specific installation instructions
   - Development setup instructions
   - Dependency management guide

## Phase 5: Quality Assurance and Release (Milestone 8)

1. **Code Quality**
   - Run and fix linting issues (black, isort)
   - Run and fix type checking issues (mypy)
   - Ensure 90%+ test coverage

2. **Comprehensive Test Suite**
   - Add edge cases to tests
   - Implement integration tests
   - Test on multiple Python versions

3. **Performance Optimization**
   - Profile code for bottlenecks
   - Optimize data loading and processing
   - Implement caching improvements

4. **Prepare for Release**
   - Update version number
   - Finalize README
   - Create CHANGELOG
   - Update package metadata

5. **CI/CD Pipeline Setup**
   - Set up GitHub Actions workflows
   - Include linting, testing, and deployment steps
   - Automate versioning and release tagging

6. **Release**
   - Build package with UV
   - Test installation in clean environment
   - Publish to PyPI

## Phase 6: Continuous Improvement (Ongoing)

1. **Monitor and Fix Issues**
   - Address bug reports
   - Implement feature requests

2. **Expand Supported Data Types**
   - Add support for additional AEMO data types
   - Enhance processing capabilities

3. **Community Building**
   - Respond to user questions
   - Review and merge contributions
   - Update documentation based on user feedback


## Development Practices

Throughout the implementation, adhere to these practices:

1. **Dependency Management with UV**
   - Add new dependencies to pyproject.toml
   - Install with `uv pip install -e ".[dev,docs]"`
   - Periodically update with `uv pip install --upgrade -e ".[dev,docs]"`
   - Create requirements.lock file: `uv pip freeze > requirements.lock`

2. **Version Control**
   - Commit frequently with descriptive messages
   - Use feature branches for development
   - Create pull requests for code review

3. **Testing**
   - Write tests before implementation (TDD)
   - Maintain high test coverage
   - Include edge cases in tests

4. **Documentation**
   - Document code as you write it
   - Keep README and user guides up-to-date
   - Include examples for new features

5. **Code Quality**
   - Run black and isort before commits
   - Use mypy for type checking
   - Follow PEP 8 guidelines
