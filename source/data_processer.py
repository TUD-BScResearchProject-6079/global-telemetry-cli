from __init__ import logger
from logger import LogUtils
from psycopg2.extensions import connection
from sql.insert_queries import (
    global_telemetry_from_cf_insert_query,
    global_telemetry_from_ndt_insert_query,
)
from sql.update_and_delete_queries import (
    cf_delete_query,
    cf_temp_delete_abnormal_servers_query,
    cf_temp_standardize_cities_query,
    ndt7_delete_query,
    ndt_temp_delete_abnormal_servers_query,
    ndt_temp_standardize_cities_query,
)


class DataProcesser:
    def __init__(self, conn: connection) -> None:
        self._conn = conn

    @LogUtils.log_function
    def process_data(self) -> None:
        with self._conn.cursor() as cur:
            logger.info("Starting data processing...")
            cur.execute(ndt_temp_delete_abnormal_servers_query)
            logger.info("Deleted abnormal NDT7 servers.")
            cur.execute(ndt_temp_standardize_cities_query)
            logger.info("Standardized NDT7 client cities.")
            cur.execute(global_telemetry_from_ndt_insert_query)
            logger.info("Inserted global telemetry from NDT7 into the database.")
            cur.execute(ndt7_delete_query)
            logger.info("Deleted NDT7 temporary data after processing.")

            cur.execute(cf_temp_delete_abnormal_servers_query)
            logger.info("Deleted abnormal Cloudflare servers.")
            cur.execute(cf_temp_standardize_cities_query)
            logger.info("Standardized Cloudflare client cities.")
            cur.execute(global_telemetry_from_cf_insert_query)
            logger.info("Inserted global telemetry from Cloudflare into the database.")
            cur.execute(cf_delete_query)
            logger.info("Deleted Cloudflare temporary data after processing.")
            self._conn.commit()
            logger.info("Data processing completed successfully.")
