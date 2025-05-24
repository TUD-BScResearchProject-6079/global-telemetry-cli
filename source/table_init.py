import gc
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from __init__ import logger
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
    unified_telemetry_create_query,
)
from sql.drop_queries import drop_tables_query
from sql.insert_queries import (
    airport_insert_query,
    caida_asn_insert_query,
    cf_best_server_insert_query,
    cities_insert_query,
    countries_with_starlink_measurements_insert_query,
    ndt_best_server_insert_query,
)

InsertData = List[Tuple[sql.SQL, Path, Optional[Callable[[pd.DataFrame], None]]]]


class TableInitializer:
    def __init__(self, conn: connection):
        self._conn = conn
        self._create_queries = [
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

        data_dir = (Path(__file__).parent / '..' / "data").resolve()
        self._insert_data: InsertData = [
            (cities_insert_query, data_dir / 'cities.csv', None),
            (
                airport_insert_query,
                data_dir / 'airport-codes.csv',
                TableInitializer._clean_airport_codes,
            ),
            (
                ndt_best_server_insert_query,
                data_dir / 'ndt-servers-per-country.csv',
                TableInitializer._clean_ndt7_servers,
            ),
            (
                cf_best_server_insert_query,
                data_dir / 'cf-servers-per-country.csv',
                TableInitializer._clean_cf_servers,
            ),
            (caida_asn_insert_query, data_dir / 'asns_query_results.csv', None),
            (
                countries_with_starlink_measurements_insert_query,
                data_dir / 'countries_w_starlink_measurements.csv',
                None,
            ),
        ]

    @LogUtils.log_function
    def initialize_tables(self) -> None:
        with self._conn.cursor() as cur:
            for query in self._create_queries:
                cur.execute(query)
            self._conn.commit()
            for insert_query, csv_file_path, clean_dataframe in self._insert_data:
                self._insert_data_from_csv(cur, csv_file_path, insert_query, clean_dataframe)

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
            self._conn.commit()
            logger.info(f"Successfully inserted data from {csv_file_path}")

        except Exception as e:
            logger.error(f"Error inserting data from {csv_file_path}: {e}")
            self._conn.rollback()
        finally:
            if df is not None:
                del df
            gc.collect()

    @staticmethod
    def _clean_airport_codes(df: pd.DataFrame) -> None:
        df.dropna(subset=["iata_code"], inplace=True)
        for col in df.columns:
            if col not in ["iso_country", "iata_code"]:
                df.drop(columns=col, inplace=True)
        df.rename(
            columns={"iso_country": "country_code", "iata_code": "airport_code"},
            inplace=True,
        )

    @staticmethod
    def _clean_ndt7_servers(df: pd.DataFrame) -> None:
        df.dropna(subset=["client_country", "server_city", "server_country"], inplace=True)
        df.drop(df[df["client_country"] == df["server_country"]].index, inplace=True)

    @staticmethod
    def _clean_cf_servers(df: pd.DataFrame) -> None:
        df.rename(
            columns={
                "clientCountry": "client_country",
                "serverPoP": "server_airport_code",
            },
            inplace=True,
        )
        mask = df["client_country"].str.len().gt(2) | df["server_airport_code"].str.len().gt(3)
        df.drop(index=df[mask].index, inplace=True)
