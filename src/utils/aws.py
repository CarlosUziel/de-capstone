import json
import logging
from configparser import ConfigParser
from time import sleep
from typing import Any, Dict

import boto3
from botocore.client import ClientError


def create_attach_role(iam_client: Any, dl_config: ConfigParser) -> str:
    """Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)

    Args:
        iam_client: client to access AWS IAM service.
        dl_config: a ConfigParser containing necessary parameters.
    """
    # 1. Create the role
    try:
        iam_client.create_role(
            Path="/",
            RoleName=dl_config.get("DWH", "DWH_IAM_ROLE_NAME"),
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        logging.error(e)

    # 2. Attach role
    try:
        iam_client.attach_role_policy(
            RoleName=dl_config.get("DWH", "DWH_IAM_ROLE_NAME"),
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        )["ResponseMetadata"]["HTTPStatusCode"]
    except Exception as e:
        logging.error(e)

    # 3. Return IAM role ARN
    return iam_client.get_role(RoleName=dl_config.get("DWH", "DWH_IAM_ROLE_NAME"))[
        "Role"
    ]["Arn"]


def create_redshift_cluster(
    redshift_client: Any, dl_config: ConfigParser, iamArn: str
) -> Dict:
    """Create a Redshift cluster with given parameters and appropiate role.

    Args:
        iam_client: client to access AWS Redshift service.
        dl_config: a ConfigParser containing necessary parameters.
        iamArn: IAM role.
    """
    # 1. Create cluster
    try:
        redshift_client.create_cluster(
            # HW
            ClusterType=dl_config.get("DWH", "DWH_CLUSTER_TYPE"),
            NodeType=dl_config.get("DWH", "DWH_NODE_TYPE"),
            NumberOfNodes=int(dl_config.get("DWH", "DWH_NUM_NODES")),
            # Identifiers & Credentials
            DBName=dl_config.get("DWH", "DWH_DB"),
            ClusterIdentifier=dl_config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
            MasterUsername=dl_config.get("DWH", "DWH_DB_USER"),
            MasterUserPassword=dl_config.get("DWH", "DWH_DB_PASSWORD"),
            # Roles (for s3 access)
            IamRoles=[iamArn],
        )

    except Exception as e:
        logging.error(e)

    # 2. Wait for cluster to be available
    logging.info("Waiting for Redshift cluster to become available...")
    while True:
        cluster_props = redshift_client.describe_clusters(
            ClusterIdentifier=dl_config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
        )["Clusters"][0]

        if cluster_props["ClusterStatus"] == "available":
            break
        else:
            sleep(30)
    logging.info("Redshift cluster is ready to be used!")

    return cluster_props, redshift_client


def open_db_port(user_config: ConfigParser, dl_config: ConfigParser):
    """Open an incoming TCP port to access the cluster endpoint"""

    # 1. Get EC2 client
    ec2 = boto3.resource(
        "ec2",
        aws_access_key_id=user_config.get("AWS", "AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=user_config.get("AWS", "AWS_SECRET_ACCESS_KEY"),
        region_name=dl_config.get("GENERAL", "REGION"),
    )

    # 2. Open DB port
    try:
        vpc = ec2.Vpc(id=dl_config.get("DWH", "DWH_VPC_ID"))
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(dl_config.get("DWH", "DWH_DB_PORT")),
            ToPort=int(dl_config.get("DWH", "DWH_DB_PORT")),
        )
    except Exception as e:
        print(e)


def delete_cluster(redshift_client: Any, dl_config: ConfigParser):
    """Delete an Amazon Redshift cluster

    Args:
        redshift_client: A boto3 Redshift client
        dl_config: DB configuration parameters.
    """
    redshift_client.delete_cluster(
        ClusterIdentifier=dl_config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
        SkipFinalClusterSnapshot=True,
    )


def delete_iam_roles(iam_client: Any, dl_config: ConfigParser):
    """Delete IAM roles

    Args:
        iam_client: A boto3 IAM client
        dl_config: DB configuration parameters.
    """
    iam_client.detach_role_policy(
        RoleName=dl_config.get("DWH", "DWH_IAM_ROLE_NAME"),
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )
    iam_client.delete_role(RoleName=dl_config.get("DWH", "DWH_IAM_ROLE_NAME"))


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
