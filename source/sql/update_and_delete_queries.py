from psycopg2 import sql

ndt_temp_delete_abnormal_servers_query = sql.SQL(
    """
    DELETE FROM ndt7_temp n
    WHERE n.client_country_code <> n.server_country_code
        AND (n.server_city, n.server_country_code) NOT IN (
            SELECT DISTINCT server_city, server_country
            FROM ndt_server_for_country sv
            WHERE sv.client_country = n.client_country_code
            );
"""
)

cf_temp_delete_abnormal_servers_query = sql.SQL(
    """
    DELETE FROM cf_temp cf
    USING
        airport_country ac
    WHERE
        cf.server_airport_code = ac.airport_code
        AND cf.client_country_code <> ac.country_code
        AND cf.server_airport_code NOT IN (
            SELECT DISTINCT sv.server_airport_code
            FROM cf_server_for_country sv
            WHERE cf.client_country_code = sv.client_country
        );
"""
)

ndt_temp_standardize_cities_query = sql.SQL(
    """
    UPDATE ndt7_temp n
    SET
        client_city = c.asciiname,
        client_region = c.region
    FROM cities c
    WHERE
        n.client_city IS NOT NULL
        AND n.client_city <> ''
        AND n.client_city IN (c.name, c.asciiname, c.name1, c.name2, c.name3, c.name4)
        AND n.client_country_code = c.country_code;
"""
)

cf_temp_standardize_cities_query = sql.SQL(
    """
    UPDATE cf_temp cf
    SET
        client_city = c.asciiname,
        client_region = c.region
    FROM cities c
    WHERE
        cf.client_city IS NOT NULL
        AND cf.client_city <> ''
        AND cf.client_city IN (c.name, c.asciiname, c.name1, c.name2, c.name3, c.name4)
        AND cf.client_country_code = c.country_code;
"""
)

cf_delete_query = sql.SQL(
    """
    DELETE FROM cf_temp;
"""
)

ndt7_delete_query = sql.SQL(
    """
    DELETE FROM ndt7_temp;
"""
)
