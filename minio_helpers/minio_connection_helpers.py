"""
    Purpose:
        Minio Object Storage Connection Helpers.

        This library is used to interact with Minio object storage. Functions establish
        a connection to the Minio service that can be used to interact with the service
        and pass to the other helper functions
"""

# Python Library Imports
import logging
from minio import Minio
from minio.error import ResponseError


###
# Minio Connection Helpers
###


def connect_to_minio(minio_url, access_key=None, secret_key=None, secure=False):
    """
    Purpose:
        Connect to Minio and return the minio_client of minio lib
    Args:
        minio_url (String): URL of Minio
        access_key (String): Access Key for Minio
        secret_key (String): Secret Key for Minio
    Returns:
        minio_client (minio client Obj): Client obj connection to Minio
    """
    logging.info(f"Connecting to Minio: {minio_url}")

    try:
        if access_key:
            minio_client = Minio(
                minio_url,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
        else:
            minio_client = Minio(minio_url, secure=secure)
    except ResponseError as con_err:
        logging.exception(f"Can't Connect to Minio URL ({minio_url}): {con_err}")
        raise con_err
    except Exception as err:
        logging.exception(f"Exception connecting to minio: {err}")
        raise err

    return minio_client


def build_minio_url(minio_host, minio_port=9000):
    """
    Purpose:
        Create the Minio URL from host and port
    Args:
        minio_host (String): Host of Minio
        minio_host (Int): Port of Minio (Defaults to 9000)
    Returns:
        minio_url (String): URL of Minio
    """

    return f"{minio_host}:{minio_port}"
