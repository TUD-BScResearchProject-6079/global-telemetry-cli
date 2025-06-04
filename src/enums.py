from enum import Enum


class CsvFiles(Enum):
    CITIES = "cities.csv"
    AIRPORT_CODES = "airport-codes.csv"
    NDT_BEST_SERVERS = "ndt-best-servers.csv"
    CF_BEST_SERVERS = "cf-best-servers.csv"
    ASNS = "asns.csv"
    COUNTRIES_WITH_STARLINK_MEASUREMENTS = "countries_w_starlink_measurements.csv"


class Tables(Enum):
    CITIES = 'cities'
    AIRPORT_CODES = 'airport_country'
    NDT_BEST_SERVERS = 'ndt_server_for_city'
    CF_BEST_SERVERS = 'cf_server_for_city'
    AS_STATISTICS = 'as_statistics'
    COUNTRIES_WITH_STARLINK_MEASUREMENTS = 'countries_with_starlink_measurements'
    NDT7_TEMP = 'ndt7_temp'
    CF_TEMP = 'cf_temp'
    UNIFIED_TELEMETRY = 'unified_telemetry'


class UpdateChoices(Enum):
    ASN_DATE = "asn"
    AIRPORT_CODES = "airport"
    CITIES = "cities"

class ExecutionDecision(Enum):
    OK = 0
    SKIP = 1
