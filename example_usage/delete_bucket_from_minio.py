#!/usr/bin/env python3
"""
    Purpose:
        Delete Bucket from Minio

    Steps:
        - Connect to Minio
        - Delete a Bucket

    function call:python3 delete_bucket_from_minio.py {--access-key=access_key} \
        {--secret-key=secret_key} {--minio-host=minio_host} {--minio-port=minio_port} \
        {--bucket-name=bucket_name}
"""

# Python Library Imports
import logging
import os
import sys
from argparse import ArgumentParser

# Local Library Imports
from minio_helpers import minio_connection_helpers, minio_bucket_helpers
from minio_helpers.minio_exceptions import BucketDoesntExist


def main():
    """
    Purpose:
        Read an .avro File
    """
    logging.info("Starting Create Bucket in Minio")

    opts = get_options()

    minio_url =\
        minio_connection_helpers.build_minio_url(opts.minio_host, opts.minio_port)

    minio_client = minio_connection_helpers.connect_to_minio(
        minio_url, opts.access_key, opts.secret_key
    )

    buckets = minio_bucket_helpers.get_buckets(minio_client)
    bucket_names = minio_bucket_helpers.get_bucket_names(minio_client)
    if opts.bucket_name not in bucket_names:
        logging.info(f"{opts.bucket_name} doesn't exist, expecting an exception")

    try:
        minio_bucket_helpers.delete_bucket(minio_client, opts.bucket_name)
    except BucketDoesntExist as ba_err:
        logging.error(f"Got expected Error: {ba_err}")
    except Exception as err:
        logging.error(f"Unexpected Error Deleting Bucket: {err}")
        raise err

    import pdb; pdb.set_trace()

    logging.info("Create Bucket in Minio Complete")


###
# General/Helper Methods
###


def get_options():
    """
    Purpose:
        Parse CLI arguments for script
    Args:
        N/A
    Return:
        N/A
    """

    parser = ArgumentParser(description="Delete Bucket From Minio")
    required = parser.add_argument_group('Required Arguments')
    optional = parser.add_argument_group('Optional Arguments')

    # Optional Arguments
    # N/A

    # Required Arguments
    required.add_argument(
        "--access-key",
        dest="access_key",
        help="Access Key for Minio",
        required=True,
    )
    required.add_argument(
        "--secret-key",
        dest="secret_key",
        help="Secret Key for Minio",
        required=True,
    )
    required.add_argument(
        "--minio-host",
        dest="minio_host",
        help="Host for Minio",
        required=True,
    )
    required.add_argument(
        "--minio-port",
        dest="minio_port",
        help="Port for Minio",
        required=True,
    )
    required.add_argument(
        "--bucket-name",
        dest="bucket_name",
        help="Bucket Name to Create",
        required=True,
    )

    return parser.parse_args()


if __name__ == "__main__":

    log_level = logging.INFO
    logging.getLogger().setLevel(log_level)
    logging.basicConfig(
        stream=sys.stdout,
        level=log_level,
        format="[create_bucket_in_minio] %(asctime)s %(levelname)s %(message)s",
        datefmt="%a, %d %b %Y %H:%M:%S"
    )

    try:
        main()
    except Exception as err:
        print(
            "{0} failed due to error: {1}".format(os.path.basename(__file__), err)
        )
        raise err
