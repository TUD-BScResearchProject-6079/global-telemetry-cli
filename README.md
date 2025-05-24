# Global Telemetry System

This project implements a job that merges the NDT7 and Cloudflare AIM datasets for telemetry data analysis.

## Requirements

- **Python version:** 3.13
- Recommended: Use a virtual environment (e.g., `venv` or `virtualenv`).

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
   DB_HOST=<your_host>
   DB_PORT=<your_port>
   DB_USER=<your_user>
   DB_PASSWORD=<your_password_here>
   DB_NAME=<your_DB_here>
   ```

## Logging

- Logging is handled by the `LogUtils` class in `source/logger.py`.
- Logs are written to the `logs/` directory with UTC timestamps and also output to the console.
- Decorate any function with `@LogUtils.log_function` to automatically log its execution and exceptions.

## Usage

- Place your data processing scripts in the `source/` directory.
- Import and use the utilities as needed.
- Run your main script as usual:
  ```sh
  python source/your_script.py
  ```

## Project Structure

```
global-telemetry-data-processing/
│
├── source/
│   ├── logger.py
│   └── ... (other source files)
├── logs/
│   └── ... (log files generated at runtime)
├── .env
├── requirements.txt
└── README.md
```

## Notes

- Ensure Python 3.13 is installed and available in your PATH.
- For any issues, please open an issue or contact the maintainer.
