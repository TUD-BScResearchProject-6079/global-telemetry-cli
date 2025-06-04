def get_cf_formatted_query(date: str, top_asns: str) -> str:
    return f"""
    SELECT
        measurementUUID AS uuid,
        DATETIME(TIMESTAMP(measurementTime), "UTC") AS test_time,
        clientCity AS client_city,
        clientRegion AS client_region,
        clientCountry AS client_country_code,
        serverPoP AS server_airport_code,
        clientASN AS asn,
        ROUND(packetLoss.lossRatio, 5) AS packet_loss_rate,

        ROUND((SELECT PERCENTILE_DISC(bps, 0.5) OVER () FROM UNNEST(download.bps) AS bps LIMIT 1) / 1000000, 5) AS download_throughput_mbps,
        CAST(ROUND((SELECT PERCENTILE_DISC(ltc, 0.5) OVER () FROM UNNEST(loadedLatencyMs.download) AS ltc LIMIT 1), 0) AS INT) AS download_latency_ms,
        ROUND(loadedJitterMs.download, 5) AS download_jitter_ms,

        ROUND((SELECT PERCENTILE_DISC(bps, 0.5) OVER () FROM UNNEST(upload.bps) AS bps LIMIT 1) / 1000000, 5) AS upload_throughput_mbps,
        CAST(ROUND((SELECT PERCENTILE_DISC(ltc, 0.5) OVER () FROM UNNEST(loadedLatencyMs.upload) AS ltc LIMIT 1), 0) AS INT) AS upload_latency_ms,
        ROUND(loadedJitterMs.upload, 5) AS upload_jitter_ms
    FROM `measurement-lab.cloudflare.speedtest_speed1`
    WHERE
        date = '{date}'
        AND clientCountry IS NOT NULL AND clientCountry <> ''
        AND clientASN IN ({top_asns});"""


def get_ndt_formatted_query(date: str, top_asns: str) -> str:
    return f"""
    SELECT
      a.UUID as uuid,
      DATETIME(TIMESTAMP(a.TestTime), "UTC") AS test_time,
      client.Geo.City AS client_city,
      client.Geo.Region AS client_region,
      client.Geo.CountryCode AS client_country_code,
      server.Geo.City AS server_city,
      server.Geo.CountryCode AS server_country_code,
      client.Network.ASNumber AS asn,
      ROUND(a.LossRate, 5) AS packet_loss_rate,
      CASE
        WHEN raw.Download.ServerMeasurements[SAFE_OFFSET(ARRAY_LENGTH(raw.Download.ServerMeasurements) - 1)].TCPInfo.RTT IS NOT NULL
        THEN ROUND(a.MeanThroughputMbps, 5)
        ELSE NULL
      END AS download_throughput_mbps,
      CAST(ROUND(raw.Download.ServerMeasurements[SAFE_OFFSET(ARRAY_LENGTH(raw.Download.ServerMeasurements) - 1)].TCPInfo.RTT / 1000, 0) AS INT) AS download_latency_ms,
      ROUND(raw.Download.ServerMeasurements[SAFE_OFFSET(ARRAY_LENGTH(raw.Download.ServerMeasurements) - 1)].TCPInfo.RTTVar / 1000, 5) AS download_jitter_ms,
      CASE
        WHEN raw.Upload.ServerMeasurements[SAFE_OFFSET(ARRAY_LENGTH(raw.Upload.ServerMeasurements) - 1)].TCPInfo.RTT IS NOT NULL
        THEN ROUND(a.MeanThroughputMbps, 5)
        ELSE NULL
      END AS upload_throughput_mbps,
      CAST(ROUND(raw.Upload.ServerMeasurements[SAFE_OFFSET(ARRAY_LENGTH(raw.Upload.ServerMeasurements) - 1)].TCPInfo.RTT / 1000, 0) AS INT) AS upload_latency_ms,
      ROUND(raw.Upload.ServerMeasurements[SAFE_OFFSET(ARRAY_LENGTH(raw.Upload.ServerMeasurements) - 1)].TCPInfo.RTTVar / 1000, 5) AS upload_jitter_ms
    FROM `measurement-lab.ndt.ndt7`
    WHERE
      date = '{date}'
      AND client.Geo.CountryCode IS NOT NULL AND client.Geo.CountryCode <> ''
      AND a.MeanThroughputMbps IS NOT NULL AND a.MeanThroughputMbps <> 0.0
      AND (
        raw.Download.ServerMeasurements[SAFE_OFFSET(ARRAY_LENGTH(raw.Download.ServerMeasurements) - 1)].TCPInfo.RTT IS NOT NULL
          OR
        raw.Upload.ServerMeasurements[SAFE_OFFSET(ARRAY_LENGTH(raw.Upload.ServerMeasurements) - 1)].TCPInfo.RTT IS NOT NULL
      )
      AND client.Network.ASNumber IN ({top_asns});"""


def get_cf_best_servers_query(date_from: str, date_to: str, top_asns: str) -> str:
    return f"""
    WITH city_servers AS (
      SELECT
        clientCity,
        clientCountry,
        serverPoP,
        (SELECT PERCENTILE_DISC(ltc, 0.5) OVER() FROM UNNEST(loadedLatencyMs.download) AS ltc LIMIT 1) AS download_latency_ms,
        (SELECT PERCENTILE_DISC(ltc, 0.5) OVER() FROM UNNEST(loadedLatencyMs.upload) AS ltc LIMIT 1) AS upload_latency_ms,
      FROM `measurement-lab.cloudflare.speedtest_speed1`
      WHERE
        measurementTime >= '{date_from}'
        AND measurementTime <= '{date_to}'
        AND clientCity IS NOT NULL
        AND clientCity <> ''
        AND clientCountry IS NOT NULL
        AND clientCountry <> ''
        AND serverPoP IS NOT NULL
        AND serverPoP <> ''
        AND clientASN IN  ({top_asns})
    )

    SELECT DISTINCT
      clientCity,
      clientCountry,
      serverPoP
    FROM city_servers cs
    WHERE
      download_latency_ms IS NOT NULL
      AND download_latency_ms > 0
      AND download_latency_ms = (
        SELECT MIN(download_latency_ms)
        FROM city_servers
        WHERE cs.clientCountry = clientCountry AND cs.clientCity = clientCity
      )
      OR
      upload_latency_ms IS NOT NULL
      AND upload_latency_ms > 0
      AND upload_latency_ms = (
        SELECT MIN(upload_latency_ms)
        FROM city_servers
        WHERE cs.clientCountry = clientCountry AND cs.clientCity = clientCity
      )
    """


def get_ndt_best_servers_query(date_from: str, date_to: str, top_asns: str) -> str:
    return f"""
    WITH server_for_client AS (
      SELECT
        client.Geo.City AS client_city,
        client.Geo.CountryCode AS client_country,
        server.Geo.City AS server_city,
        server.Geo.CountryCode AS server_country,
        raw.Download.ServerMeasurements[OFFSET(ARRAY_LENGTH(raw.Download.ServerMeasurements) - 1)].TCPInfo.RTT AS download_latency_ms,
        raw.Upload.ServerMeasurements[OFFSET(ARRAY_LENGTH(raw.Upload.ServerMeasurements) - 1)].TCPInfo.RTT AS upload_latency_ms
      FROM `measurement-lab.ndt.ndt7`
      WHERE date >= '{date_from}'
        AND date <= '{date_to}'
        AND client.Geo.CountryCode IS NOT NULL
        AND client.Geo.CountryCode <> ''
        AND client.Geo.City IS NOT NULL
        AND client.Geo.City <> ''
        AND a.MeanThroughputMbps <> 0.0
        AND client.Network.ASNumber IN ({top_asns})
    ),

    min_latencies AS (
      SELECT
        client_city,
        client_country,
        MIN(download_latency_ms) AS min_download_latency_ms,
        MIN(upload_latency_ms) AS min_upload_latency_ms
      FROM server_for_client
      WHERE download_latency_ms IS NOT NULL OR upload_latency_ms IS NOT NULL
      GROUP BY client_city, client_country
    )

    SELECT DISTINCT
      s.client_city,
      s.client_country,
      s.server_city,
      s.server_country
    FROM server_for_client s
    JOIN min_latencies m
      ON s.client_city = m.client_city AND s.client_country = m.client_country
    WHERE
      (s.download_latency_ms IS NOT NULL AND s.download_latency_ms = m.min_download_latency_ms)
      OR
      (s.upload_latency_ms IS NOT NULL AND s.upload_latency_ms = m.min_upload_latency_ms)
    """


def get_countries_with_starlink_query(date_from: str, date_to: str) -> str:
    return f"""
    (SELECT client.Geo.CountryCode AS country_code
    FROM `measurement-lab.ndt.ndt7`
    WHERE
      date >= '{date_from}'
      AND date <= '{date_to}'
      AND client.Network.ASNumber = 14593)
    UNION DISTINCT
    (SELECT clientCountry AS country_code
    FROM `cloudflare.speedtest_speed1`
    WHERE
      measurementTime >= '{date_from}'
      AND measurementTime <= '{date_to}'
      AND clientASN = 14593)
    """
