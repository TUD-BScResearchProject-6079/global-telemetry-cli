from datetime import date, datetime, timezone
import io
import zipfile

from __init__ import data_dir, logger
import pandas as pd
import requests


def download_file(url: str, file_name: str, unzip: bool = False) -> None:
    file_path = data_dir / file_name
    response = requests.get(url)
    response.raise_for_status()
    if unzip:
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(data_dir)
            extracted_files = z.namelist()
            logger.info(f"Extracted files: {extracted_files} to {data_dir}")
            if len(extracted_files) == 1:
                original_path = data_dir / extracted_files[0]
                original_path.rename(file_path)
                logger.info(f"Renamed {original_path} to {file_path}")
            else:
                logger.warning("Multiple files extracted; skipping rename.")
    else:
        with open(file_path, "wb") as f:
            f.write(response.content)
        logger.info(f"File saved to: {file_path}")


def delete_files(file_names: list[str]) -> None:
    for file_name in file_names:
        file_path = data_dir / file_name
        if file_path.exists():
            file_path.unlink()
            logger.info(f"Deleted file: {file_path}")
        else:
            logger.warning(f"File not found for deletion: {file_path}")


def _extract_alt_names(alt_str: str) -> list[str]:
    if not alt_str:
        return [""] * 4
    parts = alt_str.split(",")
    return parts[:4] + [""] * (4 - len(parts[:4]))


def generate_cities_csv(cities_txt: str, regions_txt: str, final_file_name: str) -> None:
    column_names = [
        "geonameid",
        "name",
        "asciiname",
        "alternatenames",
        "latitude",
        "longitude",
        "feature_class",
        "feature_code",
        "country_code",
        "cc2",
        "admin1_code",
        "admin2_code",
        "admin3_code",
        "admin4_code",
        "population",
        "elevation",
        "dem",
        "timezone",
        "modification_date",
    ]
    cities_path = data_dir / cities_txt
    cities_df = pd.read_csv(
        cities_path,
        sep="\t",
        header=None,
        names=column_names,
        dtype=str,
        keep_default_na=False,
    )
    cities_df = cities_df[
        [
            "country_code",
            "name",
            "asciiname",
            "alternatenames",
            "admin1_code",
            "admin2_code",
        ]
    ]

    alt_names = cities_df["alternatenames"].apply(_extract_alt_names).apply(pd.Series)
    alt_names.columns = pd.Index(["name1", "name2", "name3", "name4"])

    regions_path = data_dir / regions_txt
    regions_df = pd.read_csv(
        regions_path,
        sep="\t",
        header=None,
        names=["admin1_full_code", "admin1_name", "admin1_ascii", "geonameid"],
        dtype=str,
        keep_default_na=False,
    )

    regions_df[["admin1_country", "admin1_code"]] = regions_df["admin1_full_code"].str.split(".", expand=True)
    cities_df = cities_df.merge(
        regions_df[["admin1_country", "admin1_code", "admin1_ascii"]],
        left_on=["country_code", "admin1_code"],
        right_on=["admin1_country", "admin1_code"],
        how="left",
    )

    cities_df.rename(columns={"admin1_ascii": "region"}, inplace=True)

    final_df = pd.concat(
        [
            cities_df["name"],
            cities_df["asciiname"],
            alt_names,
            cities_df["region"],
            cities_df["country_code"],
        ],
        axis=1,
    )

    final_df.columns = pd.Index(
        [
            "name",
            "asciiname",
            "name1",
            "name2",
            "name3",
            "name4",
            "region",
            "country_code",
        ]
    )

    final_df.to_csv(data_dir / final_file_name, index=False)
    logger.info(f"Cities csv successfully created {len(final_df)} records from {cities_txt} and {regions_txt}.")


def parse_date_range(date_range: str) -> tuple[date, date]:
    if ':' in date_range:
        start_date, end_date = date_range.split(':', 1)
        return (
            datetime.strptime(start_date.strip(), "%Y-%m-%d").date(),
            datetime.strptime(end_date.strip(), "%Y-%m-%d").date(),
        )
    else:
        return datetime.strptime(date_range.strip(), "%Y-%m-%d").date(), datetime.now(timezone.utc).date()
