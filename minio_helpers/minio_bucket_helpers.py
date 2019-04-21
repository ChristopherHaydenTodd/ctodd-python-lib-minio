"""
    Purpose:
        Minio Object Storage Bucket Helpers.

        This library is used to interact with Minio object storage. Will handle
        functions used to interact with buckets (creating, downloading, finding, etc)
"""

# Python Library Imports
import logging
from minio import Minio
from minio.error import ResponseError

# Local Library Imports
from minio_helpers.minio_exceptions import BucketAlreadyExists, BucketDoesntExist


###
# Bucket Getter Helpers
###


def get_buckets(minio_client):
    """
    Purpose:
        Get a list of buckets that exist in the Minio Client
    Args:
        minio_client (minio client Obj): Client obj connection to Minio
    Returns:
        buckets (List of Bucket Objs): List of Bucket OBJs in Minio
    """

    buckets = []

    try:
        buckets = list(minio_client.list_buckets())
    except ResponseError as con_err:
        logging.error(f"Error Connecting to Minio: {con_err}")
        raise err
    except Exception as err:
        logging.error(f"Error Listing Buckets: {err}")
        raise err

    return buckets


def get_bucket_names(minio_client):
    """
    Purpose:
        Get a list of buckets that exist in the Minio Client
    Args:
        minio_client (minio client Obj): Client obj connection to Minio
    Returns:
        bucket_names (List of Strings): List of Buckets in Minio
    """

    bucket_names = []

    try:
        bucket_names = [bucket.name for bucket in minio_client.list_buckets()]
    except ResponseError as con_err:
        logging.error(f"Error Connecting to Minio: {con_err}")
        raise err
    except Exception as err:
        logging.error(f"Error Listing Buckets: {err}")
        raise err

    return bucket_names


###
# Bucket Manipulation Helpers
###


def create_bucket(minio_client, bucket_name):
    """
    Purpose:
        Create a specified Bucket by name
    Args:
        minio_client (minio client Obj): Client obj connection to Minio
        bucket_name (String): Name of bucket to create
    Returns:
        N/A
    """
    logging.info(f"Creating Bucket {bucket_name}")

    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(
                bucket_name, location="us-east-1"
            )
        else:
            raise BucketAlreadyExists(f"{bucket_name} Already Exists in Minio")
    except ResponseError as con_err:
        logging.error(f"Error Connecting to Minio: {con_err}")
        raise con_err
    except Exception as err:
        logging.error(f"Error Creating Bucket {bucket_name}: {err}")
        raise err


def delete_bucket(minio_client, bucket_name):
    """
    Purpose:
        Delete a specified Bucket by name
    Args:
        minio_client (minio client Obj): Client obj connection to Minio
        bucket_name (String): Name of bucket to delete
    Returns:
        N/A
    """
    logging.info(f"Deleting Bucket {bucket_name}")

    try:
        if minio_client.bucket_exists(bucket_name):
            minio_client.remove_bucket(bucket_name)
        else:
            raise BucketDoesntExist(f"{bucket_name} Doesn't Exist in Minio")
    except ResponseError as con_err:
        logging.error(f"Error Connecting to Minio: {con_err}")
        raise con_err
    except Exception as err:
        logging.error(f"Error Deleting Bucket: {err}")
        raise err
