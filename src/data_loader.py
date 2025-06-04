from datetime import date

from google.cloud import bigquery
from pandas import DataFrame
from psycopg2 import sql
from psycopg2.extensions import connection, cursor
from psycopg2.extras import execute_values

from __init__ import logger
from custom_exceptions import InvalidDateError
from enums import ExecutionDecision, CsvFiles, Tables
from logger import LogUtils
from sql.bigquery_queries import (
    get_cf_best_servers_query,
    get_cf_formatted_query,
    get_countries_with_starlink_query,
    get_ndt_best_servers_query,
    get_ndt_formatted_query,
)
from sql.delete_queries import delete_all_from_table_query
from sql.insert_queries import (
    cf_best_server_insert_query,
    cf_temp_insert_query,
    countries_with_starlink_measurements_insert_query,
    ndt_best_server_insert_query,
    ndt_temp_insert_query,
    processed_date_insert_query,
)
from sql.select_queries import (
    processed_date_select_query,
    top_five_isps_countries_with_starlink_select_query,
)
from utils import save_dataframe_to_csv


class DataLoader:
    def __init__(self, conn: connection) -> None:
        self._conn = conn
        self._client = bigquery.Client(project="measurement-lab")

    @LogUtils.log_function
    def load_data(self, date: date, skip_inserted_dates: bool = False) -> ExecutionDecision:
        with self._conn.cursor() as cur:
            if (result := self._check_date(cur, date, skip_inserted_dates = skip_inserted_dates)) == ExecutionDecision.SKIP:
                logger.info(f"Skipping data loading for {date.strftime('%Y-%m-%d')} as it has already been processed.")
                return result
            top_asns = self._get_top_asns(cur)
            ndt7_query = get_ndt_formatted_query(date.strftime("%Y-%m-%d"), top_asns)
            cf_query = get_cf_formatted_query(date.strftime("%Y-%m-%d"), top_asns)
            self._download_data(cur, ndt7_query, ndt_temp_insert_query, 'NDT7')
            self._download_data(cur, cf_query, cf_temp_insert_query, 'Cloudflare')
            self._insert_processed_date(cur, date)
            self._conn.commit()
        return ExecutionDecision.OK

    @LogUtils.log_function
    def update_best_servers(self, date_from: date, date_to: date) -> None:
        with self._conn.cursor() as cur:
            top_asns = self._get_top_asns(cur)
            ndt_query = get_ndt_best_servers_query(date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d"), top_asns)
            cf_query = get_cf_best_servers_query(date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d"), top_asns)
            delete_ndt_query = delete_all_from_table_query(Tables.NDT_BEST_SERVERS.value)
            cur.execute(delete_ndt_query)
            logger.info("Deleted all rows from NDT7 Best Servers table.")
            ndt_df = self._download_data(cur, ndt_query, ndt_best_server_insert_query, 'NDT7 Best Servers')
            save_dataframe_to_csv(ndt_df, CsvFiles.NDT_BEST_SERVERS.value)

            delete_cf_query = delete_all_from_table_query(Tables.CF_BEST_SERVERS.value)
            cur.execute(delete_cf_query)
            logger.info("Deleted all rows from Cloudflare Best Servers table.")
            cf_df = self._download_data(cur, cf_query, cf_best_server_insert_query, 'Cloudflare Best Servers')
            save_dataframe_to_csv(cf_df, CsvFiles.CF_BEST_SERVERS.value)
            self._conn.commit()

    @LogUtils.log_function
    def update_countries_with_starlink(self, date_from: date, date_to: date) -> None:
        with self._conn.cursor() as cur:
            delete_query = delete_all_from_table_query(Tables.COUNTRIES_WITH_STARLINK_MEASUREMENTS.value)
            cur.execute(delete_query)
            logger.info("Deleted all rows from Countries with Starlink Measurements table.")
            download_query = get_countries_with_starlink_query(
                date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d")
            )
            df = self._download_data(
                cur,
                download_query,
                countries_with_starlink_measurements_insert_query,
                'Countries with Starlink Measurements',
            )
            save_dataframe_to_csv(df, CsvFiles.COUNTRIES_WITH_STARLINK_MEASUREMENTS.value)
            self._conn.commit()

    def _check_date(self, cur: cursor, date_to_process: date, skip_inserted_dates: bool = False) -> ExecutionDecision:
        cur.execute(processed_date_select_query, (date_to_process.strftime("%Y-%m-%d"),))
        if cur.fetchone():
            if not skip_inserted_dates:
                raise InvalidDateError(f"Data for {date_to_process} has already been processed. Please choose a different date.")
            logger.warning(f"Data for {date_to_process} has already been processed. Continuing without inserting.")
            return ExecutionDecision.SKIP
        logger.info(f"Date {date_to_process.strftime('%Y-%m-%d')} is valid for processing.")
        return ExecutionDecision.OK

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
    ) -> DataFrame:
        df: DataFrame = self._client.query(download_query).to_dataframe()
        logger.info(f"Downloaded {len(df)} rows from BigQuery from {dataset_name}.")
        df.replace('', None, inplace=True)
        df = df.astype(object).where(df.notnull(), None)
        data_tuples = [tuple(x) for x in df.to_records(index=False)]
        execute_values(cur, insert_query, data_tuples)
        logger.info(f"Inserted {len(data_tuples)} rows into the database from {dataset_name}.")
        return df

    def _get_top_asns(self, cur: cursor) -> str:
        cur.execute(top_five_isps_countries_with_starlink_select_query)
        row = cur.fetchone()
        if not row:
            logger.warning("No ISPs found in the database. Using default ISPs.")
            return "14593"
        isps_str: str = row[0]
        return isps_str
