#!/usr/bin/env python3
"""
    Purpose:
        Get all Objects (Or a Specific Object) From a Bucket in Minio

    Steps:
        - Connect to Minio
        - Get the Bucket Obj
        - Get objects from Minio and store locally

    function call:python3 get_objects_from_Bucket.py {--access-key=access_key} \
        {--secret-key=secret_key} {--minio-host=minio_host} {--minio-port=minio_port} \
        {--bucket-name=bucket_name} {--object-name=object_name}
"""

# Python Library Imports
import logging
import os
import sys
from argparse import ArgumentParser

# Local Library Imports
from minio_helpers import minio_connection_helpers, minio_object_helpers
from minio_helpers.minio_exceptions import BucketDoesntExist


def main():
    """
    Purpose:
        Read an .avro File
    """
    logging.info("Starting Get Objects from Buckets in Minio")

    opts = get_options()

    minio_url =\
        minio_connection_helpers.build_minio_url(opts.minio_host, opts.minio_port)

    minio_client = minio_connection_helpers.connect_to_minio(
        minio_url, opts.access_key, opts.secret_key
    )

    if not opts.object_names:
        objects = minio_object_helpers.get_objects(minio_client, opts.bucket_name)
        object_names = minio_object_helpers.get_object_names(minio_client, opts.bucket_name)
    else:
        object_names = opts.object_names

    for object_name in object_names:
        object_exists = minio_object_helpers.is_object_in_bucket(
            minio_client, opts.bucket_name, object_name
        )

        if not object_exists:
            logging.error(f"{object_name} doesnt exist in {opts.bucket_name}")
            continue


        if opts.download_to_memory:
            minio_object = minio_object_helpers.download_object_to_memory(
                minio_client, opts.bucket_name, object_name
            )
            import pdb; pdb.set_trace()
        else:
            minio_object_helpers.download_object_to_file(
                minio_client,
                opts.bucket_name,
                object_name,
                filename=f"{opts.download_dir}/{object_name}",
            )

    import pdb; pdb.set_trace()

    logging.info("Get Objects from Buckets in Minio Complete")


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

    parser = ArgumentParser(description="Get Objects from Minio")
    required = parser.add_argument_group('Required Arguments')
    optional = parser.add_argument_group('Optional Arguments')

    # Optional Arguments
    required.add_argument(
        "--object-name",
        dest="object_names",
        default=[],
        action="append",
        help="Objects to download",
        required=False,
    )
    required.add_argument(
        "--download-dir",
        dest="download_dir",
        default="./data",
        help="Where to Download the Files",
        required=False,
    )
    required.add_argument(
        "--download-to-memory",
        dest="download_to_memory",
        default=False,
        action="store_true",
        help="Should we download to memory?",
        required=False,
    )

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
