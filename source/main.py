import argparse
import os

from __init__ import logger
from dotenv import load_dotenv
from enums import UpdateChoices
from factory import Factory
from handler import Handler
import psycopg2


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
        help="Choose the update(s) to perform. Use 'asn' for updating ISNs, 'airport' for updating airport codes, and 'cities' for updating city names. You can specify multiple updates by separating them with commas (e.g., 'asn,airport').",
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
            handler = Handler(Factory(conn))
            if args.drop:
                handler.drop()
            if args.init:
                handler.init()
            if args.update_best_servers:
                handler.update_best_servers(args.update_best_servers)
            if args.update_countries_with_starlink:
                handler.update_countries_with_starlink(args.update_countries_with_starlink)
            if args.update:
                handler.update(args.update)
            if args.date:
                handler.date(args.date)

    except Exception as e:
        logger.error(f"Application encountered an exception: {e}")
        return

    logger.info("Application exited successfully.")


if __name__ == "__main__":
    main()
