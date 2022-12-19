import logging
from configparser import ConfigParser

import boto3
from botocore.client import ClientError


def create_s3_bucket(user_config: ConfigParser, dl_config: ConfigParser) -> bool:
    """
    Create S3 bucket if it doesn't exist.
    """
    # 1. Get S3 client
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=user_config.get("AWS", "AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=user_config.get("AWS", "AWS_SECRET_ACCESS_KEY"),
        region_name=dl_config.get("GENERAL", "REGION"),
    )

    # 2. Create bucket if it doesn't exist
    bucket_name = dl_config.get("S3", "BUCKET_NAME")

    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        # If 404 error, then the bucket does not exist.
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    "LocationConstraint": dl_config.get("GENERAL", "REGION")
                },
            )
            return True
        else:
            logging.error(f"Bucket {bucket_name} could not be created.\n{e}")
            return False
    else:
        logging.info(f"Bucket {bucket_name} already exists.")

        # Output the bucket names
        logging.info("Available buckets: {}".format(list(s3.buckets.all())))
        return True


def delete_s3_bucket(user_config: ConfigParser, dl_config: ConfigParser) -> bool:
    """
    Delete S3 bucket if it exists.
    """
    # 1. Get S3 client
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=user_config.get("AWS", "AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=user_config.get("AWS", "AWS_SECRET_ACCESS_KEY"),
        region_name=dl_config.get("GENERAL", "REGION"),
    )

    # 2. Create bucket if it doesn't exist
    bucket_name = dl_config.get("S3", "DEST_BUCKET_NAME")
    bucket = s3.Bucket(bucket_name)
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        # If 404 error, then the bucket does not exist.
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            logging.error(f"Bucket {bucket_name} does not exist.\n{e}")
            return True
        else:
            logging.error(f"Bucket {bucket_name} could not be deleted.\n{e}")
            return False
    else:
        for key in bucket:
            key.delete()
        bucket.delete()

        logging.info(f"Bucket {bucket_name} deleted.")
        return True
