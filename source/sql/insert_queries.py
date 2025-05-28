from psycopg2 import sql

processed_date_insert_query = sql.SQL(
    """
    INSERT INTO processed_dates (processed_date) VALUES %s
"""
)


cities_insert_query = sql.SQL(
    """
    INSERT INTO cities (name, asciiname, name1, name2, name3, name4, region, country_code)
    VALUES %s
"""
)

ndt_temp_insert_query = sql.SQL(
    """
            INSERT INTO ndt7_temp (
                uuid,
                test_time,
                client_city,
                client_region,
                client_country_code,
                server_city,
                server_country_code,
                asn,
                packet_loss_rate,
                download_throughput_mbps,
                download_latency_ms,
                download_jitter_ms,
                upload_throughput_mbps,
                upload_latency_ms,
                upload_jitter_ms
            ) VALUES %s
        """
)

cf_temp_insert_query = sql.SQL(
    """
    INSERT INTO cf_temp (
        uuid,
        test_time,
        client_city,
        client_region,
        client_country_code,
        server_airport_code,
        asn,
        packet_loss_rate,
        download_throughput_mbps,
        download_latency_ms,
        download_jitter_ms,
        upload_throughput_mbps,
        upload_latency_ms,
        upload_jitter_ms
    ) VALUES %s
"""
)

airport_insert_query = sql.SQL(
    """
    INSERT INTO airport_country (country_code, airport_city, airport_code)
    VALUES %s
"""
)

ndt_best_server_insert_query = sql.SQL(
    """
    INSERT INTO ndt_server_for_city (client_city, client_country_code, server_city, server_country_code)
    VALUES %s
"""
)

cf_best_server_insert_query = sql.SQL(
    """
    INSERT INTO cf_server_for_city (client_city, client_country_code, server_airport_code)
    VALUES %s
"""
)

caida_asn_insert_query = sql.SQL(
    """
    INSERT INTO as_statistics (asn, asn_name, rank, country_code, country_name) VALUES %s
"""
)

countries_with_starlink_measurements_insert_query = sql.SQL(
    """
    INSERT INTO countries_with_starlink_measurements (country_code) VALUES %s
"""
)

global_telemetry_from_cf_insert_query = sql.SQL(
    """
    INSERT INTO unified_telemetry (uuid, test_time, client_city, client_region, client_country_code, server_city, server_country_code, asn, packet_loss_rate, download_throughput_mbps, download_latency_ms, download_jitter_ms, upload_throughput_mbps, upload_latency_ms, upload_jitter_ms)
    SELECT uuid, test_time, client_city, client_region, client_country_code, ac.airport_city AS server_city, ac.country_code AS server_country_code, asn, packet_loss_rate, download_throughput_mbps, download_latency_ms, download_jitter_ms, upload_throughput_mbps, upload_latency_ms, upload_jitter_ms
    FROM cf_temp JOIN airport_country ac ON cf_temp.server_airport_code = ac.airport_code;
"""
)

global_telemetry_from_ndt_insert_query = sql.SQL(
    """
    INSERT INTO unified_telemetry (uuid, test_time, client_city, client_region, client_country_code, server_city, server_country_code, asn, packet_loss_rate, download_throughput_mbps, download_latency_ms, download_jitter_ms, upload_throughput_mbps, upload_latency_ms, upload_jitter_ms)
    SELECT uuid, test_time, client_city, client_region, client_country_code, server_city, server_country_code, asn, packet_loss_rate, download_throughput_mbps, download_latency_ms, download_jitter_ms, upload_throughput_mbps, upload_latency_ms, upload_jitter_ms
    FROM ndt7_temp;
"""
)
