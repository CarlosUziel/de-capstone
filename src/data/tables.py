from pathlib import Path
from typing import Any, Dict, Iterable, Union

import pyspark.sql.types as T

from etl import (
    extract_dim_airports,
    extract_dim_cities,
    extract_fact_immigration,
    extract_fact_temps,
    extract_fact_us_demogr,
)
from utils.io import process_config

# I94 Immigration Data
I94_ON_LOAD_FIELDS: Dict[str, T.DataType] = {
    "cicid": (T.FloatType(), True),
    "i94yr": (T.FloatType(), True),
    "i94mon": (T.FloatType(), True),
    "i94cit": (T.FloatType(), True),
    "i94res": (T.FloatType(), True),
    "i94port": (T.StringType(), True),
    "arrdate": (T.FloatType(), True),
    "i94mode": (T.FloatType(), True),
    "i94addr": (T.StringType(), True),
    "depdate": (T.FloatType(), True),
    "i94bir": (T.FloatType(), True),
    "i94visa": (T.FloatType(), True),
    "count": (T.FloatType(), True),
    "dtadfile": (T.FloatType(), True),
    "visapost": (T.StringType(), True),
    "occup": (T.StringType(), True),
    "entdepa": (T.StringType(), True),
    "entdepd": (T.StringType(), True),
    "entdepu": (T.StringType(), True),
    "matflag": (T.StringType(), True),
    "biryear": (T.FloatType(), True),
    "dtaddto": (T.FloatType(), True),
    "gender": (T.StringType(), True),
    "insnum": (T.FloatType(), True),
    "airline": (T.StringType(), True),
    "admnum": (T.FloatType(), True),
    "fltno": (T.StringType(), True),
    "visatype": (T.StringType(), True),
}
I94_ON_LOAD_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in I94_ON_LOAD_FIELDS.items()]
)

# U.S. City Demographic Data
USDEMO_ON_LOAD_FIELDS: Dict[str, T.DataType] = {
    "City": (T.StringType(), True),
    "State": (T.StringType(), True),
    "Median Age": (T.FloatType(), True),
    "Male Population": (T.FloatType(), True),
    "Female Population": (T.FloatType(), True),
    "Total Population": (T.IntegerType(), True),
    "Number of Veterans": (T.FloatType(), True),
    "Foreign-born": (T.FloatType(), True),
    "Average Household Size": (T.FloatType(), True),
    "State Code": (T.StringType(), True),
    "Race": (T.StringType(), True),
    "Count": (T.IntegerType(), True),
}
USDEMO_ON_LOAD_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in USDEMO_ON_LOAD_FIELDS.items()]
)

# Airport Code Table
AIRCODES_ON_LOAD_FIELDS: Dict[str, T.DataType] = {
    "ident": (T.StringType(), True),
    "type": (T.StringType(), True),
    "name": (T.StringType(), True),
    "elevation_ft": (T.FloatType(), True),
    "continent": (T.StringType(), True),
    "iso_country": (T.StringType(), True),
    "iso_region": (T.StringType(), True),
    "municipality": (T.StringType(), True),
    "gps_code": (T.StringType(), True),
    "iata_code": (T.StringType(), True),
    "local_code": (T.StringType(), True),
    "coordinates": (T.StringType(), True),
}
AIRCODES_ON_LOAD_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in AIRCODES_ON_LOAD_FIELDS.items()]
)

# World Temperature Data
TEMP_ON_LOAD_FIELDS: Dict[str, T.DataType] = {
    "dt": (T.StringType(), True),
    "AverageTemperature": (T.FloatType(), True),
    "AverageTemperatureUncertainty": (T.FloatType(), True),
    "City": (T.StringType(), True),
    "Country": (T.StringType(), True),
    "Latitude": (T.StringType(), True),
    "Longitude": (T.StringType(), True),
}
TEMP_ON_LOAD_SCHEMA = T.StructType(
    [T.StructField(k, v[0], v[1]) for k, v in TEMP_ON_LOAD_FIELDS.items()]
)

# Dictionary of staging tables' schemas
ON_LOAD_TABLES_SCHEMA: Dict[str, T.StructType] = {
    "i94_immigration": I94_ON_LOAD_SCHEMA,
    "us_demographics": USDEMO_ON_LOAD_SCHEMA,
    "airport_codes": AIRCODES_ON_LOAD_SCHEMA,
    "world_temperature": TEMP_ON_LOAD_SCHEMA,
}

# Tables files location
DATA_PATH: Path = Path(__file__).parents[2].joinpath("data")
ON_LOAD_TABLES_FILES: Dict[str, Union[str, Iterable[str]]] = {
    "i94_immigration": sorted(
        list(DATA_PATH.joinpath("i94_immigration_data_2016").glob("*_2016.csv.bz2"))
    ),
    "us_demographics": DATA_PATH.joinpath("us_cities_demographics.csv.bz2"),
    "airport_codes": DATA_PATH.joinpath("airport_codes.csv.bz2"),
    "world_temperature": DATA_PATH.joinpath("global_land_temperature_by_city.csv.bz2"),
}

# Dictionary of tables cleaning args
ON_LOAD_TABLES_CLEANING_ARGS: Dict[str, Dict[str, Any]] = {
    "i94_immigration": {
        "table_paths": ON_LOAD_TABLES_FILES["i94_immigration"],
        "table_schema": ON_LOAD_TABLES_SCHEMA["i94_immigration"],
        "drop_na_cols": [
            "cicid",
            "i94yr",
            "i94mon",
            "i94cit",
            "i94res",
            "arrdate",
            "i94mode",
            "i94visa",
            "i94bir",
            "gender",
        ],
        "drop_duplicates_cols": ["cicid"],
        "parquet_partition_cols": ("i94mon",),
    },
    "us_demographics": {
        "table_paths": ON_LOAD_TABLES_FILES["us_demographics"],
        "table_schema": ON_LOAD_TABLES_SCHEMA["us_demographics"],
        "drop_na_cols": ["City", "State", "State Code", "Race"],
        "drop_duplicates_cols": ["City", "State", "State Code", "Race"],
        "parquet_partition_cols": ("State",),
    },
    "airport_codes": {
        "table_paths": ON_LOAD_TABLES_FILES["airport_codes"],
        "table_schema": ON_LOAD_TABLES_SCHEMA["airport_codes"],
        "drop_na_cols": ["ident", "name", "iso_country", "iso_region", "municipality"],
        "drop_duplicates_cols": ["ident"],
        "parquet_partition_cols": ("iso_country",),
    },
    "world_temperature": {
        "table_paths": ON_LOAD_TABLES_FILES["world_temperature"],
        "table_schema": ON_LOAD_TABLES_SCHEMA["world_temperature"],
        "drop_na_cols": [
            "dt",
            "AverageTemperature",
            "City",
            "Country",
        ],
        "drop_duplicates_cols": None,
        "parquet_partition_cols": ("Country",),
    },
}

# Dictionary of tables cleaning args
dl_config = process_config(Path(__file__).parents[2].joinpath("dl.cfg"))
S3_BUCKET_PREFIX = dl_config.get("S3", "BUCKET_NAME")
STAR_EXTRACT_TABLES_ARGS: Dict[str, Dict[str, Any]] = {
    "dim_cities": {
        "python_callable": extract_dim_cities,
        "op_kwargs": {
            "us_demographics_path": f"s3a://{S3_BUCKET_PREFIX}/clean/us_demographics",
            "airport_codes_path": f"s3a://{S3_BUCKET_PREFIX}/clean/airport_codes",
            "s3_save_path": f"s3a://{S3_BUCKET_PREFIX}/star/dim_cities",
        },
    },
    "dim_airports": {
        "python_callable": extract_dim_airports,
        "op_kwargs": {
            "airport_codes_path": f"s3a://{S3_BUCKET_PREFIX}/clean/airport_codes",
            "dim_cities_path": f"s3a://{S3_BUCKET_PREFIX}/star/dim_cities",
            "s3_save_path": f"s3a://{S3_BUCKET_PREFIX}/star/dim_airports",
        },
    },
    "fact_temps": {
        "python_callable": extract_fact_temps,
        "op_kwargs": {
            "world_temperature_path": (
                f"s3a://{S3_BUCKET_PREFIX}/clean/world_temperature"
            ),
            "dim_cities_path": f"s3a://{S3_BUCKET_PREFIX}/star/dim_cities",
            "s3_save_path": f"s3a://{S3_BUCKET_PREFIX}/star/fact_temps",
        },
    },
    "fact_us_demogr": {
        "python_callable": extract_fact_us_demogr,
        "op_kwargs": {
            "us_demographics_path": f"s3a://{S3_BUCKET_PREFIX}/clean/us_demographics",
            "dim_cities_path": f"s3a://{S3_BUCKET_PREFIX}/star/dim_cities",
            "s3_save_path": f"s3a://{S3_BUCKET_PREFIX}/star/fact_us_demogr",
        },
    },
    "fact_immigration": {
        "python_callable": extract_fact_immigration,
        "op_kwargs": {
            "i94_immigration_path": f"s3a://{S3_BUCKET_PREFIX}/clean/i94_immigration",
            "dim_cities_path": f"s3a://{S3_BUCKET_PREFIX}/star/dim_cities",
            "data_dictionary_json": str(
                Path(__file__)
                .resolve()
                .parents[2]
                .joinpath("data/i94_immigration_data_2016/schema.json")
            ),
            "s3_save_path": f"s3a://{S3_BUCKET_PREFIX}/star/fact_immigration",
        },
    },
}
