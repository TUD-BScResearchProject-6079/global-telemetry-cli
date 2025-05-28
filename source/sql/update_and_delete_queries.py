from psycopg2 import sql

ndt_temp_delete_invalid_servers_query = sql.SQL(
    """
    DELETE FROM ndt7_temp n
    WHERE (
        EXISTS (
            SELECT 1
            FROM ndt_server_for_city sv
            WHERE sv.client_city = n.client_city
            AND sv.client_country_code = n.client_country_code
        )
        AND NOT EXISTS (
            SELECT 1
            FROM ndt_server_for_city sv
            WHERE sv.client_city = n.client_city
            AND sv.client_country_code = n.client_country_code
            AND sv.server_city = n.server_city
            AND sv.server_country_code = n.server_country_code
        )
    )
    OR (
        NOT EXISTS (
            SELECT 1
            FROM ndt_server_for_city sv
            WHERE sv.client_city = n.client_city
            AND sv.client_country_code = n.client_country_code
        )
        AND EXISTS (
            SELECT 1
            FROM ndt_server_for_city sv
            WHERE sv.client_country_code = n.client_country_code
        )
        AND NOT EXISTS (
            SELECT 1
            FROM ndt_server_for_city sv
            WHERE sv.client_country_code = n.client_country_code
            AND sv.server_city = n.server_city
            AND sv.server_country_code = n.server_country_code
        )
    )
"""
)

cf_temp_delete_invalid_servers_query = sql.SQL(
    """
    DELETE FROM cf_temp c
    WHERE (
        EXISTS (
            SELECT 1
            FROM cf_server_for_city sv
            WHERE sv.client_city = c.client_city
            AND sv.client_country_code = c.client_country_code
        )
        AND NOT EXISTS (
            SELECT 1
            FROM cf_server_for_city sv
            WHERE sv.client_city = c.client_city
            AND sv.client_country_code = c.client_country_code
            AND sv.server_airport_code = c.server_airport_code
        )
    )
    OR (
        NOT EXISTS (
            SELECT 1
            FROM cf_server_for_city sv
            WHERE sv.client_city = c.client_city
            AND sv.client_country_code = c.client_country_code
        )
        AND EXISTS (
            SELECT 1
            FROM cf_server_for_city sv
            WHERE sv.client_country_code = c.client_country_code
        )
        AND NOT EXISTS (
            SELECT 1
            FROM cf_server_for_city sv
            WHERE sv.client_country_code = c.client_country_code
            AND sv.server_airport_code = c.server_airport_code
        )
    )
"""
)

airport_codes_standardize_cities_query = sql.SQL(
    """
    UPDATE airport_country ac
    SET
        airport_city = c.asciiname
    FROM cities c
    WHERE
        ac.airport_city IS NOT NULL
        AND ac.airport_city <> ''
        AND ac.airport_city IN (c.name, c.asciiname, c.name1, c.name2, c.name3, c.name4)
        AND ac.country_code = c.country_code;
"""
)

ndt_temp_standardize_client_cities_query = sql.SQL(
    """
    WITH matched_cities AS (
        SELECT DISTINCT ON (n.uuid)
            n.uuid,
            c.asciiname,
            c.region
        FROM ndt7_temp n
        JOIN cities c ON n.client_country_code = c.country_code
            AND (n.client_city = c.name OR n.client_city = c.asciiname OR n.client_city = c.name1 OR
                n.client_city = c.name2 OR n.client_city = c.name3 OR n.client_city = c.name4)
        WHERE n.client_city IS NOT NULL AND n.client_city <> ''
    )
    UPDATE ndt7_temp n
    SET client_city = m.asciiname,
        client_region = m.region
    FROM matched_cities m
    WHERE n.uuid = m.uuid;
"""
)

ndt_temp_standardize_server_cities_query = sql.SQL(
    """
    WITH matched_cities AS (
        SELECT DISTINCT ON (n.uuid)
            n.uuid,
            c.asciiname
        FROM ndt7_temp n
        JOIN cities c ON n.server_country_code = c.country_code
            AND (n.server_city = c.name OR n.server_city = c.name1 OR n.server_city = c.name2 OR n.server_city = c.name3 OR n.server_city = c.name4)
        WHERE n.server_city IS NOT NULL AND n.server_city <> ''
    )
    UPDATE ndt7_temp n
    SET server_city = m.asciiname
    FROM matched_cities m
    WHERE n.uuid = m.uuid;
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
