import argparse
from datetime import datetime
import os

from __init__ import logger
from dotenv import load_dotenv
from enums import UpdateChoices
from factory import Factory
import psycopg2
from utils import parse_date_range


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Global Telemetry System")

    parser.add_argument(
        "--init",
        "-i",
        action="store_true",
        help="Initialize the database by creating and populating the required tables.",
    )

    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop all tables in the database. Use with caution!",
    )

    parser.add_argument(
        "--update-best-servers",
        "-ubs",
        type=str,
        help="Update best server for a specific date range. Use format yyyy-mm-dd or yyyy-mm-dd:yyyy-mm-dd, where the first date is the start (left of :) and the second date is the end (right of :). The end date is optional.",
    )

    parser.add_argument(
        "--update-countries-with-starlink",
        "-ucws",
        type=str,
        help="Update countries with Starlink for a specific date range. Use format yyyy-mm-dd or yyyy-mm-dd:yyyy-mm-dd, where the first date is the start (left of :) and the second date is the end (right of :). The end date is optional.",
    )

    parser.add_argument(
        "--update",
        "-u",
        type=str,
        choices=[update_choice.value for update_choice in UpdateChoices],
        help="Choose the operation mode: fast, slow, or auto.",
    )

    parser.add_argument(
        "--date",
        "-d",
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
            factory = Factory(conn)
            if args.drop:
                confirm = input("Are you sure you want to drop all tables? (y/n): ").strip().lower()
                if confirm == 'y':
                    logger.info("Drop flag confirmed. Dropping all tables...")
                    table_initializer = factory.get_table_initializer()
                    table_initializer.drop_tables()
                else:
                    logger.info("Drop flag detected, but operation cancelled by user.")
            if args.init:
                logger.info("Initialization flag detected. Performing setup...")
                table_initializer = factory.get_table_initializer()
                table_initializer.initialize_tables()
            if args.update_best_servers:
                start_date, end_date = parse_date_range(args.update_best_servers)
                data_loader = factory.get_data_loader()
                data_loader.update_best_servers(start_date, end_date)
            if args.update_countries_with_starlink:
                start_date, end_date = parse_date_range(args.update_best_servers)
                data_loader = factory.get_data_loader()
                data_loader.update_countries_with_starlink(start_date, end_date)
            if args.update:
                choice = UpdateChoices(args.update)
                logger.info(f"Update choice selected: {choice}")
                table_initializer = factory.get_table_initializer()
                if choice == UpdateChoices.ASN_DATE:
                    table_initializer.update_isns()
                elif choice == UpdateChoices.AIRPORT_CODES:
                    table_initializer.update_airport_codes()
                elif choice == UpdateChoices.CITIES:
                    table_initializer.update_cities()
            if args.date:
                date = datetime.strptime(args.date, "%Y-%m-%d").date()
                logger.info(f"Running with specified date: {date}")
                data_loader = factory.get_data_loader()
                data_loader.load_data(date)
                data_processer = factory.get_data_processer()
                data_processer.process_data()

    except Exception as e:
        logger.error(f"Application encountered an exception: {e}")
        return

    logger.info("Application exited successfully.")


if __name__ == "__main__":
    main()
