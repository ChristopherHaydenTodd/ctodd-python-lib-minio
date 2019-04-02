#!/usr/bin/env python3
"""
    Purpose:
        Connecting to Minio

    Steps:
        - Connect to Minio

    function call:python3 connect_to_minio {--access-key=access_key} \
        {--secret-key=secret_key} {--minio-host=minio_host} {--minio-port=minio_port}
"""

# Python Library Imports
import logging
import os
import sys
from argparse import ArgumentParser

# Local Library Imports
from minio_helpers import minio_client
from minio_helpers import minio_connection_helpers


def main():
    """
    Purpose:
        Read an .avro File
    """
    logging.info("Starting Connect to Minio")

    opts = get_options()

    minio_client_1 = minio_client.MinioClient(
        opts.minio_host, opts.access_key, opts.secret_key, minio_port=opts.minio_port
    )

    minio_url =\
        minio_connection_helpers.build_minio_url(opts.minio_host, opts.minio_port)
    minio_client_2 = minio_connection_helpers.connect_to_minio(
        minio_url, opts.access_key, opts.secret_key
    )

    import pdb; pdb.set_trace()

    logging.info("Connect to Minio Complete")


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

    parser = ArgumentParser(description="Connect to Minio")
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

    return parser.parse_args()


if __name__ == "__main__":

    log_level = logging.INFO
    logging.getLogger().setLevel(log_level)
    logging.basicConfig(
        stream=sys.stdout,
        level=log_level,
        format="[connect_to_minio] %(asctime)s %(levelname)s %(message)s",
        datefmt="%a, %d %b %Y %H:%M:%S"
    )

    try:
        main()
    except Exception as err:
        print(
            "{0} failed due to error: {1}".format(os.path.basename(__file__), err)
        )
        raise err
