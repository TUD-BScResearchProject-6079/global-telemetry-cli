# Global Telemetry Data Processing System

This project implements a comprehensive data processing system that merges NDT7 and Cloudflare AIM datasets for global telemetry analysis. It processes network measurement data from BigQuery, standardizes it, and stores it in a PostgreSQL database for analysis.

## Features

- **Data Loading**: Downloads and processes telemetry data from BigQuery (NDT7 and Cloudflare datasets)
- **Data Processing**: Standardizes city names, validates servers, and merges datasets
- **Best Server Analysis**: Identifies optimal servers for each client location based on latency
- **Starlink Analysis**: Tracks countries with Starlink measurements
- **Automated Updates**: Updates ASN data, airport codes, and city information
- **Comprehensive Logging**: Detailed logging with UTC timestamps

## Requirements

- **Python version:** 3.13
- **Database:** PostgreSQL
- **Google Cloud:** BigQuery access for M-Lab datasets
- Recommended: Use a virtual environment (e.g., `venv` or `virtualenv`)

## Setup

1. **Clone the repository:**
   ```sh
   git clone <repo-url>
   cd global-telemetry-data-processing
   ```

2. **Create and activate a virtual environment:**
   ```sh
   python3.13 -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On Unix/macOS:
   source venv/bin/activate
   ```

3. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```

4. **Configure environment variables:**

   Create a `.env` file in the root directory with the following content:
   ```env
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=<your_user>
   DB_PASSWORD=<your_password>
   DB_NAME=<your_database_name>
   ```

5. **Set up Google Cloud credentials** for BigQuery access (follow Google Cloud documentation)

## Usage

The main entry point is `src/main.py` with various command-line options:

### Initialize Database
```sh
python src/main.py --init
```
Creates all required tables and populates them with initial data.

### Process Daily Data
```sh
python src/main.py --date 2024-01-15
```
Downloads and processes telemetry data for a specific date (YYYY-MM-DD format).

### Update Best Servers
```sh
python src/main.py --update-best-servers 2024-01-01:2024-01-31
```
Updates best server mappings for a date range.

### Update Countries with Starlink
```sh
python src/main.py --update-countries-with-starlink 2024-01-01:2024-01-31
```
Updates the list of countries with Starlink measurements.

### Update Reference Data
```sh
python src/main.py --update asn,airport,cities
```
Updates ASN data, airport codes, and city information.

### Drop All Tables (Use with caution!)
```sh
python src/main.py --drop
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `--init` | Initialize database tables and populate with reference data |
| `--date YYYY-MM-DD` | Process telemetry data for specific date |
| `--update-best-servers DATE_RANGE` | Update best server mappings |
| `--update-countries-with-starlink DATE_RANGE` | Update Starlink country data |
| `--update CHOICES` | Update reference data (asn, airport, cities) |
| `--drop` | Drop all database tables |

## Data Sources

- **NDT7**: Network Diagnostic Tool measurements from M-Lab
- **Cloudflare AIM**: Cloudflare's speed test measurements
- **CAIDA ASRank**: ASN and organization data
- **GeoNames**: City and region information
- **Airport Codes**: IATA airport code mappings

## Project Structure

```
global-telemetry-data-processing/
│
├── src/
│   ├── main.py                    # Main entry point
│   ├── handler.py                 # Command handlers
│   ├── factory.py                 # Factory pattern implementation
│   ├── data_loader.py             # BigQuery data loading
│   ├── data_processer.py          # Data processing and standardization
│   ├── table_init.py              # Database table initialization
│   ├── logger.py                  # Logging utilities
│   ├── utils.py                   # Helper utilities
│   ├── enums.py                   # Enumerations
│   ├── custom_exceptions.py       # Custom exception classes
│   ├── caida_api_queries.py       # CAIDA API integration
│   └── sql/                       # SQL queries
│       ├── bigquery_queries.py
│       ├── create_queries.py
│       ├── insert_queries.py
│       ├── delete_queries.py
│       └── ...
├── data/
│   └── cities.csv                 # City reference data
├── logs/                          # Log files (auto-generated)
├── .env                           # Environment configuration
├── requirements.txt               # Python dependencies
├── setup.cfg                      # Tool configurations
├── pyproject.toml                 # Project configuration
├── build.sh                       # Build/lint script
├── format.sh                      # Code formatting script
└── README.md
```

## Logging

- Logging is handled by the `LogUtils` class in `src/logger.py`
- Logs are written to the `logs/` directory with UTC timestamps
- Console output is also provided for real-time monitoring
- Use `@LogUtils.log_function` decorator to automatically log function execution

## Development

### Code Quality
The project includes automated code quality checks:

```sh
# Format code
./format.sh

# Run linting and type checking
./build.sh
```

### Database Schema
The system creates and manages the following main tables:
- `unified_telemetry`: Merged NDT7 and Cloudflare data
- `ndt_server_for_city`: Best NDT7 servers for each city
- `cf_server_for_city`: Best Cloudflare servers for each city
- `countries_with_starlink_measurements`: Countries with Starlink data
- `as_statistics`: ASN information
- `cities`: City name standardization data
- `airport_country`: Airport code mappings

## Notes

- Ensure Python 3.13 is installed and available in your PATH
- The script processes past UTC dates only (cannot process current or future dates)
- BigQuery access requires proper Google Cloud authentication
- Large datasets may require significant processing time and storage space
- Monitor logs for detailed execution information and error handling

## Contributing

1. Follow the existing code style (Black, isort, flake8)
2. Add type hints for all functions
3. Use the `@LogUtils.log_function` decorator for important functions
4. Update tests when adding new functionality
