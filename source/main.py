import argparse
from datetime import datetime
import os

from __init__ import logger
from data_loader import DataLoader
from data_processer import DataProcesser
from dotenv import load_dotenv
import psycopg2
from table_init import TableInitializer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Global Telemetry System")

    parser.add_argument(
        "--init",
        action="store_true",
        help="Initialize the database by creating and populating the required tables.",
    )

    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop all tables in the database. Use with caution!",
    )

    parser.add_argument(
        "--date",
        type=str,
        help="Collect network measurements for a specific UTC date (format: yyyy-mm-dd).",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv()
    logger.info("Starting the application...")

    try:
        with psycopg2.connect(
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT"),
        ) as conn:
            logger.info("Connected to the database successfully.")
            if args.drop:
                confirm = input("Are you sure you want to drop all tables? (y/n): ").strip().lower()
                if confirm == 'y':
                    logger.info("Drop flag confirmed. Dropping all tables...")
                    table_initializer = TableInitializer(conn)
                    table_initializer.drop_tables()
                else:
                    logger.info("Drop flag detected, but operation cancelled by user.")
            if args.init:
                logger.info("Initialization flag detected. Performing setup...")
                table_initializer = TableInitializer(conn)
                table_initializer.initialize_tables()
            if args.date:
                try:
                    date = datetime.strptime(args.date, "%Y-%m-%d").date()
                except ValueError:
                    logger.error("Invalid date format. Use YYYY-MM-DD.")
                    return
                logger.info(f"Running with specified date: {date}")
                data_loader = DataLoader(conn)
                data_loader.load_data(date)
                data_processer = DataProcesser(conn)
                data_processer.process_data()

    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        return

    logger.info("Application exited successfully.")


if __name__ == "__main__":
    main()
