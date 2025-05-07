# Bronze Layer Repository

## Overview
Bronze is a data pipeline framework that handles raw data ingestion and staging in PostgreSQL. It provides a structured way to extract data from various sources, transform it into a consistent format, and load it into staging tables.

## Features
- Modular extractor system for different data sources
- Asynchronous data processing support
- Built-in logging and error handling
- PostgreSQL integration
- Discord data extraction support
- Configurable pipeline system

## Project Structure
```
bronze/
├── src/
│   └── bronze/
│       ├── __init__.py
│       ├── ddl/           # SQL table definitions
│       ├── extractors/    # Data source extractors
│       └── utils/         # Shared utilities
├── scripts/
│   └── pipelines/        # Pipeline entry points
├── pyproject.toml        # Project configuration
└── README.md
```

## Components

### Extractors (`src/bronze/extractors/`)
Data source-specific modules that handle data extraction and initial transformation.

#### Available Extractors:
- **SampleExtractor**: Template extractor demonstrating the interface
- **DiscordExtractor**: Extracts messages and threads from Discord channels

Each extractor implements:
- `parse()`: Main method that returns a pandas DataFrame
- Source-specific data fetching methods
- Data transformation logic

### DDL (`src/bronze/ddl/`)
SQL scripts that define the structure of staging tables.

### Utilities (`src/bronze/utils/`)
Shared functionality used across the project:
- Pipeline orchestration
- Database operations
- Logging and error handling

### Pipelines (`scripts/pipelines/`)
Entry points for running data pipelines:
- `sample_pipeline.py`: Example pipeline
- `discord_pipeline.py`: Discord data extraction pipeline

## Getting Started

### Prerequisites
- Python 3.13 or higher
- PostgreSQL database
- Required environment variables (see Configuration)

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd bronze
   ```

2. Install uv (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. Create and activate a virtual environment:
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

4. Install dependencies:
   ```bash
   uv sync
   ```

5. Install the package in development mode:
   ```bash
   uv pip install -e .
   ```

### Configuration

Set up the following environment variables:
- `DATABASE_URL`: PostgreSQL connection string
- `DARCY_KEY`: Discord bot token (for Discord extractor)
- `TEST_SERVER_ID`: Discord server ID (for Discord extractor)

### Running Pipelines

1. Sample Pipeline:
   ```bash
   uv run python scripts/pipelines/sample_pipeline.py
   ```

2. Discord Pipeline:
   ```bash
   uv run python scripts/pipelines/discord_pipeline.py
   ```

## Development

### Adding New Components

#### New Extractor
1. Create a new file in `src/bronze/extractors/`
2. Implement the extractor class with:
   - `parse()` method returning a DataFrame
   - Data fetching and transformation methods
   - Error handling

Example:
```python
from typing import Optional
import pandas as pd

class NewExtractor:
    def parse(self, input_path: Optional[str] = None) -> pd.DataFrame:
        # Your implementation here
        pass
```

#### New Pipeline
1. Create a new file in `scripts/pipelines/`
2. Import required components
3. Implement the pipeline logic

Example:
```python
from bronze.extractors.new_extractor import NewExtractor
from bronze.utils.pipeline import run_pipeline

def main():
    run_pipeline(
        extractor_class=NewExtractor,
        ddl_filename='create_new_table.sql',
        table_name='new_data'
    )

if __name__ == "__main__":
    main()
```

### Testing
Run tests using pytest:
```bash
pytest
```

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License
[Add your license information here]

## Support
[Add support information here]
