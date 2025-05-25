def get_cf_formatted_query(date: str, top_isns: str) -> str:
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
        AND clientASN IN ({top_isns});"""


def get_ndt_formatted_query(date: str, top_isns: str) -> str:
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
      AND client.Network.ASNumber IN ({top_isns});"""
