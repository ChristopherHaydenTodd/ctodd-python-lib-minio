"""
    Purpose:
        MinioClient Class for interacting with minio object store. Objects will be
        created connected to Minio
"""

# Python Library Imports
import simplejson as json
import logging

# Local Library Imports
from minio_helpers import minio_connection_helpers


class MinioClient(object):
    """
        MinioClient Class. Class objects will hold connection to the
        Minio service and can be used to interact with buckets and objects
    """

    ###
    # Class Lifecycle Methods
    ###

    def __init__(self, minio_host, access_key, secret_key, minio_port=9000):
        """
        Purpose:
            Initilize the MinioClient Class.
        Args:
            minio_host (String): Host for Minio
            access_key (String): Access Key for Minio
            secret_key (String): Secret Key for Minio
            minio_port (Int): Port for Minio (Defaults to 9000)
        Returns:
            N/A
        """
        logging.info(
            f"Initializing MinioClient Object Connected to {minio_host}:{minio_port}"
        )

        self.minio_host = minio_host
        self.minio_port = minio_port
        self.access_key = access_key
        self.secret_key = secret_key
        self.minio_url = f"http://{minio_host}:{minio_port}"

        self.minio_url = minio_connection_helpers.build_minio_url(
            self.minio_host, self.minio_port
        )
        self.minio_client = minio_connection_helpers.connect_to_minio(
            self.minio_url, self.access_key, self.secret_key
        )
