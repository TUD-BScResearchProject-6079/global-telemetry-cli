from psycopg2 import sql

drop_tables_query = sql.SQL(
    """
    DROP TABLE IF EXISTS as_statistics, countries_with_starlink_measurements,
    cities, airport_country, ndt_server_for_country, cf_server_for_country, cf_temp, ndt7_temp,
    unified_telemetry, processed_dates CASCADE;
    """
)
