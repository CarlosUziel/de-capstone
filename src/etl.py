import logging
from itertools import chain
from pathlib import Path

import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession


def extract_dim_cities(
    spark: SparkSession,
    us_demographics_path: Path,
    airport_codes_path: Path,
    s3_save_path: str,
):
    """
    Extract dimensional table `dim_cities` from `us_demographics` and `airport_codes`.

    Args:
        spark: SparkSession object.
        us_demographics_path: Path pointint to parquet files of staging US demographics
            table.
        airport_codes_path: Path pointint to parquet files of staging airport codes
            table.
        s3_save_path: S3 bucket prefix to store `dim_cities` in.
    """
    # 1. Load staging tables necessary to extract `dim_cities`
    us_demographics_df = spark.read.parquet(us_demographics_path)
    airport_codes_df = spark.read.parquet(airport_codes_path)

    # 2. Extract cities from `us_demographics`
    dim_cities_0 = (
        us_demographics_df.select(
            [
                F.col(c).alias(c.lower().replace(" ", "_"))
                for c in ["City", "State", "State Code"]
            ]
        )
        .dropDuplicates()
        .withColumn("country", F.lit("United States"))
        .withColumn("country_code", F.lit("US"))
    )

    # 3. Expand cities from `airport_codes`
    airport_codes_cols_map = {
        "municipality": "city",
        "iso_region": "iso_region",
        "iso_country": "country_code",
    }
    dim_cities_1 = (
        airport_codes_df.filter(airport_codes_df["iso_country"] == "US")
        .select([F.col(c).alias(c_new) for c, c_new in airport_codes_cols_map.items()])
        .dropDuplicates(["city", "iso_region"])
        .withColumn("country", F.lit("United States"))
    )

    # 3.1. Get state code for each city
    get_state_code = F.udf(lambda x: x.split("-")[1])
    dim_cities_1 = dim_cities_1.withColumn(
        "state_code", get_state_code(dim_cities_1["iso_region"])
    )

    # 3.2. Get state name for each city
    state_code_map = {row["state_code"]: row["state"] for row in dim_cities_0.collect()}
    state_code_map = F.create_map([F.lit(x) for x in chain(*state_code_map.items())])
    dim_cities_1 = dim_cities_1.withColumn(
        "state",
        state_code_map[dim_cities_1["state_code"]],
    )
    dim_cities_1 = dim_cities_1.drop("iso_region")

    # 4. Form final table and save to S3
    final_cols = ["city", "state", "state_code", "country", "country_code"]
    dim_cities = (
        dim_cities_0.select(final_cols)
        .union(dim_cities_1.select(final_cols))
        .dropDuplicates()
        .fillna("Unknown", subset="state")
        .sort("state_code")
        .withColumn("city_id", F.monotonically_increasing_id())
        .select(["city_id"] + final_cols)
    )

    # 5. Save `dim_cities` to S3 bucket
    logging.info(f"dim_cities has {dim_cities.count()} records")
    partition_cols = ["state_code"]
    dim_cities.repartition(*[F.col(c) for c in partition_cols]).write.parquet(
        s3_save_path, partitionBy=partition_cols, mode="overwrite"
    )

    return dim_cities


def extract_dim_airports(
    spark: SparkSession,
    airport_codes_path: Path,
    dim_cities_path: Path,
    s3_save_path: str,
):
    """
    Extract dimensional table `dim_airports` from `airport_codes` and `dim_cities`

    Args:
        spark: SparkSession object.
        airport_codes_path: Path pointint to parquet files of staging airport codes.
        dim_cities_path: Path pointint to parquet files of `dim_cities` table.
        s3_save_path: S3 bucket prefix to store `dim_airports` in.
    """
    # 1. Load staging tables necessary to extract `dim_cities`
    airport_codes_df = spark.read.parquet(airport_codes_path)
    dim_cities_df = spark.read.parquet(dim_cities_path)

    # 2. Rename and modfiy columns to join with `dim_cities`
    get_state_code = F.udf(lambda x: x.split("-")[1])
    airport_codes_df = (
        airport_codes_df.filter(airport_codes_df["iso_country"] == "US")
        .withColumn("iso_region", get_state_code(airport_codes_df["iso_region"]))
        .withColumnRenamed("municipality", "city")
        .withColumnRenamed("iso_country", "country_code")
        .withColumnRenamed("iso_region", "state_code")
    )

    # 3. Join with `dim_cities` to get `city_id` field
    dim_airports = (
        airport_codes_df.join(
            dim_cities_df.select(["city_id", "city", "state_code", "country_code"]),
            (airport_codes_df["city"] == dim_cities_df["city"])
            & (airport_codes_df["state_code"] == dim_cities_df["state_code"])
            & (airport_codes_df["country_code"] == dim_cities_df["country_code"]),
        )
        .dropDuplicates(subset=["city", "state_code", "country_code"])
        .drop("city", "state_code", "country_code")
    )

    # 4. Save `dim_airports` to S3 bucket
    logging.info(f"dim_airports has {dim_airports.count()} records")
    partition_cols = ["type"]
    dim_airports.repartition(*[F.col(c) for c in partition_cols]).write.parquet(
        s3_save_path, partitionBy=partition_cols, mode="overwrite"
    )

    return dim_airports


def extract_fact_temps(
    spark: SparkSession,
    world_temperature_path: Path,
    dim_cities_path: Path,
    s3_save_path: str,
):
    """
    Extract dimensional table `fact_temps` from `world_temperature` and `dim_cities`.

    Args:
        spark: SparkSession object.
        world_temperature_path: Path pointint to parquet files of staging world
            temperature table.
        dim_cities_path: Path pointint to parquet files of `dim_cities` table.
        s3_save_path: S3 bucket prefix to store `fact_temps` in.
    """
    # 1. Load staging tables necessary to extract `dim_cities`
    world_temperature_df = spark.read.parquet(world_temperature_path)
    dim_cities_df = spark.read.parquet(dim_cities_path)

    # 2. Keep only US country, convert `dt` field to date
    world_temperature_df = world_temperature_df.filter(
        world_temperature_df["Country"] == "United States"
    ).withColumn("dt", F.to_date(world_temperature_df["dt"], "yyyy-MM-dd"))

    # 3. Average the temperatures of the last 25 available years
    min_keep_date = world_temperature_df.select(
        F.max(world_temperature_df["dt"])
    ).first()[0] - relativedelta(years=25)
    world_temperature_df = world_temperature_df.filter(
        world_temperature_df["dt"] > min_keep_date
    )
    world_temperature_df = world_temperature_df.groupBy(
        ["City", "Country", "Latitude", "Longitude"]
    ).avg()

    # 4. Rename all columns
    world_temperature_cols_map = {
        "City": "city",
        "Country": "country",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "avg(AverageTemperature)": "avg_temperature",
        "avg(AverageTemperatureUncertainty)": "avg_temperature_uncertainty",
    }
    fact_temps = world_temperature_df.select(
        [F.col(c).alias(c_new) for c, c_new in world_temperature_cols_map.items()]
    )

    # 5. Join with `dim_cities` to get `city_id`
    fact_temps = (
        fact_temps.join(
            dim_cities_df.select(["city_id", "city", "country"]),
            (fact_temps["city"] == dim_cities_df["city"])
            & (fact_temps["country"] == dim_cities_df["country"]),
        )
        .dropDuplicates(subset=["city", "country"])
        .drop("city", "country", "latitude", "longitude")
    )

    # 4. Save `fact_temps` to S3 bucket
    logging.info(f"fact_temps has {fact_temps.count()} records")
    fact_temps.write.parquet(s3_save_path, mode="overwrite")

    return fact_temps


def extract_fact_us_demogr(
    spark: SparkSession,
    us_demographics_path: Path,
    dim_cities_path: Path,
    s3_save_path: str,
):
    """
    Extract dimensional table `fact_us_demogr` from `us_demographics` and `dim_cities`.

    Args:
        spark: SparkSession object.
        us_demographics_path: Path pointint to parquet files of staging US demographics
            table.
        dim_cities_path: Path pointint to parquet files of `dim_cities` table.
        s3_save_path: S3 bucket prefix to store `fact_temps` in.
    """
    # 1. Load staging tables necessary to extract `dim_cities`
    us_demographics_df = spark.read.parquet(us_demographics_path)
    dim_cities_df = spark.read.parquet(dim_cities_path)

    # 2. Group by/pivot race and rename columns
    us_demographics_df = (
        us_demographics_df.groupBy(
            [c for c in us_demographics_df.columns if c not in ["Race", "Count"]]
        )
        .pivot("Race")
        .sum("Count")
        .withColumnRenamed("sum(Count)", "Count")
    )
    us_demographics_df = us_demographics_df.select(
        [
            F.col(c).alias(c.lower().replace(" ", "_").replace("-", "_"))
            for c in us_demographics_df.columns
        ]
    )

    # 3. Join with `dim_cities` to get `city_id` field.
    fact_us_demogr = (
        us_demographics_df.join(
            dim_cities_df.select(["city_id", "city", "state", "state_code"]),
            (us_demographics_df["city"] == dim_cities_df["city"])
            & (us_demographics_df["state"] == dim_cities_df["state"])
            & (us_demographics_df["state_code"] == dim_cities_df["state_code"]),
        )
        .dropDuplicates(subset=["city", "state", "state_code"])
        .drop("city", "state", "state_code")
    )

    # 4. Save `fact_us_demogr` to S3 bucket
    logging.info(f"fact_us_demogr has {fact_us_demogr.count()} records")
    fact_us_demogr.write.parquet(s3_save_path, mode="overwrite")

    return fact_us_demogr
