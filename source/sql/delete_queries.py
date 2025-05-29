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


def delete_all_from_table_query(table_name: str) -> sql.SQL:
    query = f"DELETE FROM {table_name};"
    return sql.SQL(query)
