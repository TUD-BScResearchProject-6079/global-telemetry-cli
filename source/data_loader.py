from datetime import date, datetime, timezone

from __init__ import logger
from custom_exceptions import InvalidDateError
from google.cloud import bigquery
from logger import LogUtils
from pandas import DataFrame
from psycopg2 import sql
from psycopg2.extensions import connection, cursor
from psycopg2.extras import execute_values
from sql.bigquery_queries import get_cf_formatted_query, get_ndt_formatted_query
from sql.insert_queries import (
    cf_temp_insert_query,
    ndt_temp_insert_query,
    processed_date_insert_query,
)
from sql.select_queries import (
    processed_date_select_query,
    top_five_isps_countries_with_starlink_select_query,
)


class DataLoader:
    def __init__(self, conn: connection) -> None:
        self._conn = conn
        self._client = bigquery.Client(project="measurement-lab")

    @LogUtils.log_function
    def load_data(self, date: date) -> None:
        with self._conn.cursor() as cur:
            self._check_date(cur, date)
            top_isns = self._get_top_isns(cur)
            ndt7_query = get_ndt_formatted_query(date.strftime("%Y-%m-%d"), top_isns)
            cf_query = get_cf_formatted_query(date.strftime("%Y-%m-%d"), top_isns)
            self._download_data(cur, ndt7_query, ndt_temp_insert_query, 'NDT7')
            self._download_data(cur, cf_query, cf_temp_insert_query, 'Cloudflare')
            self._insert_processed_date(cur, date)
            self._conn.commit()

    @LogUtils.log_function
    def update_best_servers(self) -> None:
        pass

    @LogUtils.log_function
    def update_countries_with_starlink(self) -> None:
        pass

    def _check_date(self, cur: cursor, date_to_process: date) -> None:
        if date_to_process >= datetime.now(timezone.utc).date():
            raise InvalidDateError("The script can only run on dates that have already completed (past UTC dates).")
        cur.execute(processed_date_select_query, (date_to_process.strftime("%Y-%m-%d"),))
        if cur.fetchone():
            raise InvalidDateError(
                f"Data for {date_to_process} has already been processed. Please choose a different date."
            )
        logger.info(f"Date {date_to_process.strftime('%Y-%m-%d')} is valid for processing.")

    def _insert_processed_date(self, cur: cursor, date_to_process: date) -> None:
        data_tuples = [(date_to_process.strftime("%Y-%m-%d"),)]
        execute_values(cur, processed_date_insert_query, data_tuples)
        logger.info(f"Inserted processed date: {date_to_process.strftime('%Y-%m-%d')} into the database.")

    def _download_data(
        self,
        cur: cursor,
        download_query: str,
        insert_query: sql.SQL,
        dataset_name: str,
    ) -> None:
        df: DataFrame = self._client.query(download_query).to_dataframe()
        logger.info(f"Downloaded {len(df)} rows from BigQuery from {dataset_name}.")
        df.replace('', None, inplace=True)
        df = df.astype(object).where(df.notnull(), None)
        data_tuples = [tuple(x) for x in df.to_records(index=False)]
        execute_values(cur, insert_query, data_tuples)
        logger.info(f"Inserted {len(data_tuples)} rows into the database from {dataset_name}.")

    def _get_top_isns(self, cur: cursor) -> str:
        cur.execute(top_five_isps_countries_with_starlink_select_query)
        row = cur.fetchone()
        if not row:
            logger.warning("No ISPs found in the database. Using default ISPs.")
            return "14593"
        isps_str: str = row[0]
        return isps_str
