import gc
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

from __init__ import data_dir, logger
from caida_api_queries import fetch_asn_data
from enums import CsvFiles, Tables
from logger import LogUtils
import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection, cursor
from psycopg2.extras import execute_values
from sql.create_queries import (
    airports_create_query,
    caida_asn_create_table_query,
    cf_best_servers_create_query,
    cf_temp_create_query,
    cities_create_query,
    countries_with_starlink_measurements_create_query,
    ndt_best_servers_create_query,
    ndt_temp_create_query,
    processed_dates_create_query,
    unified_telemetry_create_query,
)
from sql.delete_queries import delete_all_from_table_query
from sql.drop_queries import drop_tables_query
from sql.insert_queries import (
    airport_insert_query,
    caida_asn_insert_query,
    cf_best_server_insert_query,
    cities_insert_query,
    countries_with_starlink_measurements_insert_query,
    ndt_best_server_insert_query,
)
from utils import delete_files, download_file, generate_cities_csv

from source.sql.delete_queries import airport_codes_standardize_cities_query

InsertTuple = Tuple[sql.SQL, sql.SQL | None, str, Optional[Callable[[pd.DataFrame], None]]]
InsertData = Dict[Tables, InsertTuple]


class TableInitializer:
    def __init__(self, conn: connection) -> None:
        self._conn = conn
        self._create_queries = [
            processed_dates_create_query,
            caida_asn_create_table_query,
            countries_with_starlink_measurements_create_query,
            cities_create_query,
            airports_create_query,
            ndt_best_servers_create_query,
            cf_best_servers_create_query,
            cf_temp_create_query,
            ndt_temp_create_query,
            unified_telemetry_create_query,
        ]

        self._insert_data: InsertData = {
            Tables.CITIES: (cities_insert_query, None, CsvFiles.CITIES.value, None),
            Tables.AIRPORT_CODES: (
                airport_insert_query,
                airport_codes_standardize_cities_query,
                CsvFiles.AIRPORT_CODES.value,
                TableInitializer._clean_airport_codes,
            ),
            Tables.NDT_BEST_SERVERS: (
                ndt_best_server_insert_query,
                None,
                CsvFiles.NDT_BEST_SERVERS.value,
                None,
            ),
            Tables.CF_BEST_SERVERS: (
                cf_best_server_insert_query,
                None,
                CsvFiles.CF_BEST_SERVERS.value,
                TableInitializer._clean_cf_servers,
            ),
            Tables.AS_STATISTICS: (caida_asn_insert_query, None, CsvFiles.ASNS.value, None),
            Tables.COUNTRIES_WITH_STARLINK: (
                countries_with_starlink_measurements_insert_query,
                None,
                CsvFiles.COUNTRIES_WITH_STARLINK.value,
                None,
            ),
        }

    @LogUtils.log_function
    def initialize_tables(self) -> None:
        with self._conn.cursor() as cur:
            for query in self._create_queries:
                cur.execute(query)
            for insert_record in self._insert_data.values():
                self._process_and_insert_data(cur, insert_record)
            self._conn.commit()
            logger.info("All tables created and data inserted successfully.")

    @LogUtils.log_function
    def update_isns(self) -> None:
        fetch_asn_data(CsvFiles.ASNS.value)
        with self._conn.cursor() as cur:
            self._clean_and_insert_data(cur, Tables.AS_STATISTICS)

    @LogUtils.log_function
    def update_airport_codes(self) -> None:
        download_file('https://datahub.io/core/airport-codes/_r/-/data/airport-codes.csv', CsvFiles.AIRPORT_CODES.value)
        with self._conn.cursor() as cur:
            self._clean_and_insert_data(cur, Tables.AIRPORT_CODES)

    @LogUtils.log_function
    def update_cities(self) -> None:
        download_file('https://download.geonames.org/export/dump/cities1000.zip', 'cities.txt', unzip=True)
        download_file('https://download.geonames.org/export/dump/admin1CodesASCII.txt', 'regions.txt')
        generate_cities_csv('cities.txt', 'regions.txt', CsvFiles.CITIES.value)
        delete_files(['cities.txt', 'regions.txt'])
        with self._conn.cursor() as cur:
            self._clean_and_insert_data(cur, Tables.CITIES)

    def _clean_and_insert_data(self, cur: cursor, table: Tables) -> None:
        delete_query = delete_all_from_table_query(table.value)
        cur.execute(delete_query)
        self._process_and_insert_data(cur, self._insert_data[table])

    def _process_and_insert_data(self, cur: cursor, insert_tuple: InsertTuple) -> None:
        insert_query, post_insert_query, csv_file_name, clean_dataframe = insert_tuple
        csv_file_path = data_dir / csv_file_name
        self._insert_data_from_csv(cur, csv_file_path, insert_query, clean_dataframe)
        if post_insert_query:
            cur.execute(post_insert_query)
            logger.info(f"Executed post-insert query for {csv_file_name}")

    @LogUtils.log_function
    def drop_tables(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(drop_tables_query)
            self._conn.commit()

    def _insert_data_from_csv(
        self,
        cur: cursor,
        csv_file_path: Path,
        insert_query: sql.SQL,
        clean_dataframe: Callable[[pd.DataFrame], None] | None = None,
    ) -> None:
        df = None
        try:
            df = pd.read_csv(csv_file_path, dtype=str, na_values=[""], keep_default_na=False)
            df = df.where(pd.notnull(df), None)
            if clean_dataframe:
                clean_dataframe(df)
            data_tuples = [tuple(x) for x in df.to_records(index=False)]
            execute_values(cur, insert_query, data_tuples)
            logger.info(f"Successfully inserted data from {csv_file_path}")

        except Exception as e:
            logger.error(f"Error inserting data from {csv_file_path}: {e}")
            raise e
        finally:
            if df is not None:
                del df
            gc.collect()

    @staticmethod
    def _clean_airport_codes(df: pd.DataFrame) -> None:
        df.dropna(subset=["iata_code"], inplace=True)
        for col in df.columns:
            if col not in ["iso_country", "municipality", "iata_code"]:
                df.drop(columns=col, inplace=True)
        df.rename(
            columns={"iso_country": "country_code", "municipality": "airport_city", "iata_code": "airport_code"},
            inplace=True,
        )

    @staticmethod
    def _clean_cf_servers(df: pd.DataFrame) -> None:
        df.rename(
            columns={
                "clientCity": "client_city",
                "clientCountry": "client_country",
                "serverPoP": "server_airport_code",
            },
            inplace=True,
        )
        mask = df["client_country"].str.len().ne(2) | df["server_airport_code"].str.len().ne(3)
        df.drop(index=df[mask].index, inplace=True)
