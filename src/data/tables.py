from pathlib import Path
from typing import Any, Dict, Iterable, Union

import pyspark.sql.types as T

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
        "data_paths": ON_LOAD_TABLES_FILES["i94_immigration"],
        "data_schema": ON_LOAD_TABLES_SCHEMA["i94_immigration"],
        "table_name": "i94_immigration",
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
        "parquet_partition_cols": ("i94mon", "i94cit"),
    },
    "us_demographics": {
        "data_paths": ON_LOAD_TABLES_FILES["us_demographics"],
        "data_schema": ON_LOAD_TABLES_SCHEMA["us_demographics"],
        "table_name": "us_demographics",
        "drop_na_cols": [
            "City",
            "State",
        ],
        "drop_duplicates_cols": ["State Code"],
        "parquet_partition_cols": ("State", "City"),
    },
    "airport_codes": {
        "data_paths": ON_LOAD_TABLES_FILES["airport_codes"],
        "data_schema": ON_LOAD_TABLES_SCHEMA["airport_codes"],
        "table_name": "airport_codes",
        "drop_na_cols": ["ident", "name", "iso_country", "iso_region", "municipality"],
        "drop_duplicates_cols": ["ident"],
        "parquet_partition_cols": ("iso_country", "iso_region"),
    },
    "world_temperature": {
        "data_paths": ON_LOAD_TABLES_FILES["world_temperature"],
        "data_schema": ON_LOAD_TABLES_SCHEMA["world_temperature"],
        "table_name": "world_temperature",
        "drop_na_cols": [
            "dt",
            "AverageTemperature",
            "City",
            "Country",
        ],
        "drop_duplicates_cols": None,
        "parquet_partition_cols": ("Country", "City"),
    },
}
