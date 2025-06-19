from psycopg2.extensions import connection

from .config import logger
from .enums import Tables
from .logger import LogUtils
from .sql.delete_queries import (
    cf_temp_delete_invalid_servers_query,
    delete_all_from_table_query,
    ndt_temp_delete_invalid_servers_query,
)
from .sql.insert_queries import (
    global_telemetry_from_cf_insert_query,
    global_telemetry_from_ndt_insert_query,
)
from .sql.update_queries import (
    cf_temp_standardize_cities_query,
    ndt_temp_standardize_client_cities_query,
    ndt_temp_standardize_server_cities_query,
)


class DataProcesser:
    def __init__(self, conn: connection) -> None:
        self._conn = conn

    @LogUtils.log_function
    def process_data(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(ndt_temp_delete_invalid_servers_query)
            logger.info("Deleted invalid NDT7 servers.")
            cur.execute(ndt_temp_standardize_client_cities_query)
            logger.info("Standardized NDT7 client cities.")
            cur.execute(ndt_temp_standardize_server_cities_query)
            logger.info("Standardized NDT7 server cities.")
            cur.execute(global_telemetry_from_ndt_insert_query)
            logger.info("Inserted global telemetry from NDT7 into the database.")
            ndt7_delete_query = delete_all_from_table_query(Tables.NDT7_TEMP.value)
            cur.execute(ndt7_delete_query)
            logger.info("Deleted NDT7 temporary data after processing.")

            cur.execute(cf_temp_delete_invalid_servers_query)
            logger.info("Deleted invalid Cloudflare servers.")
            cur.execute(cf_temp_standardize_cities_query)
            logger.info("Standardized Cloudflare client cities.")
            cur.execute(global_telemetry_from_cf_insert_query)
            logger.info("Inserted global telemetry from Cloudflare into the database.")
            cf_delete_query = delete_all_from_table_query(Tables.CF_TEMP.value)
            cur.execute(cf_delete_query)
            logger.info("Deleted Cloudflare temporary data after processing.")

            self._conn.commit()
            logger.info("Data processing completed successfully.")
