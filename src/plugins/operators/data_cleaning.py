import logging
from pathlib import Path
from typing import Iterable, Optional, Union

import pyspark.sql.functions as F
import pyspark.sql.types as T
from airflow.models import BaseOperator
from airflow.utils.context import Context
from pyspark.sql import SparkSession


class DataCleaningOperator(BaseOperator):
    """Perform multiple data cleaning steps using Spark on locally stored data.

    Args:
        spark: Spark session.
        data_paths: One or more Paths pointing to the data files.

    """

    ui_color = "#89DA59"

    def __init__(
        self,
        spark: SparkSession,
        data_paths: Union[Path, Iterable[Path]],
        data_schema: T.StructType,
        table_name: str,
        s3_bucket_prefix: str,
        drop_na_cols: Optional[Iterable[str]] = None,
        drop_duplicates_cols: Optional[Iterable[str]] = None,
        parquet_partition_cols: Optional[Iterable[str]] = None,
        *args,
        **kwargs,
    ):
        super(DataCleaningOperator, self).__init__(*args, **kwargs)
        self.spark = spark
        self.data_paths = data_paths
        self.data_schema = data_schema
        self.table_name = table_name
        self.s3_bucket_prefix = s3_bucket_prefix
        self.drop_na_cols = drop_na_cols
        self.drop_duplicates_cols = drop_duplicates_cols
        self.parquet_partition_cols = parquet_partition_cols

    def execute(self, context: Context):
        # 1. Load data with schema
        table_df = self.spark.read.csv(
            (
                str(self.data_paths)
                if not isinstance(self.data_paths, Iterable)
                else [str(p) for p in self.data_paths]
            ),
            schema=self.data_schema,
            header=True,
        )

        # 2. Remove rows with only NaNs.
        table_df = table_df.dropna(how="all")

        # 3. [Optional] Remove rows with NaNs in any user-provided columns.
        if self.drop_na_cols is not None and len(self.drop_na_cols) > 0:
            table_df = table_df.dropna(how="any", subset=self.drop_na_cols)

        # 4. Remove duplicate rows (considering all columns).
        table_df = table_df.distinct()

        # 5. [Optional] Remove duplicate rows in user-provided columns.
        if self.drop_duplicates_cols is not None and len(self.drop_duplicates_cols) > 0:
            table_df = table_df.dropDuplicates(subset=self.drop_duplicates_cols)

        # 6. Store clean data in S3 as parquet files.
        logging.info(
            f"{self.table_name} has {table_df.count()} records after the cleaning"
            " procedure."
        )
        save_path = f"s3a://{self.s3_bucket_prefix}/{self.table_name}"
        if (
            self.parquet_partition_cols is not None
            and len(self.parquet_partition_cols) > 0
        ):
            table_df.repartition(
                *[F.col(c) for c in self.parquet_partition_cols]
            ).write.parquet(save_path, mode="overwrite")
        else:
            table_df.write.parquet(save_path, mode="overwrite")
