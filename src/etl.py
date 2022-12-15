from itertools import chain
from pathlib import Path

import pyspark.sql.functions as F
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
    us_demographics_cols_map = {
        "City": "city",
        "State": "state",
        "State Code": "state_code",
    }
    dim_cities_0 = (
        us_demographics_df.select(
            [F.col(c).alias(c_new) for c, c_new in us_demographics_cols_map.items()]
        )
        .dropDuplicates(["city", "state"])
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
        airport_codes_df.filter(airport_codes_df.iso_country == "US")
        .select([F.col(c).alias(c_new) for c, c_new in airport_codes_cols_map.items()])
        .dropDuplicates(["city", "iso_region"])
        .withColumn("country", F.lit("United States"))
    )

    # 3.1. Get state code for each city
    get_state_code = F.udf(lambda x: x.split("-")[1])
    dim_cities_1 = dim_cities_1.withColumn(
        "state_code", get_state_code(dim_cities_1.iso_region)
    )

    # 3.2. Get state name for each city
    state_code_map = {row["state_code"]: row["state"] for row in dim_cities_0.collect()}
    state_code_map = F.create_map([F.lit(x) for x in chain(*state_code_map.items())])
    dim_cities_1 = dim_cities_1.withColumn(
        "state",
        state_code_map[dim_cities_1.state_code],
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
    partition_cols = ["state_code"]
    dim_cities.repartition(*[F.col(c) for c in partition_cols]).write.parquet(
        s3_save_path, partitionBy=partition_cols, mode="overwrite"
    )
