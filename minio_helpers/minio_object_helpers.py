"""
    Purpose:
        Minio Object Storage Object Helpers.

        This library is used to interact with Minio object storage. Will handle
        functions used to interact with objects (uploading, downloading, finding, etc)
"""

# Python Library Imports
import logging
import simplejson as json
from time import strftime
from minio import Minio
from minio.error import ResponseError, NoSuchKey

# Local Library Imports
from minio_helpers.minio_exceptions import ObjectAlreadyExists, ObjectDoesntExist, \
    ObjectDecodingNotSupported


###
# Object Getter Helpers
###


def get_objects(minio_client, bucket_name):
    """
    Purpose:
        Get a list of objects that exist in the Minio Client
    Args:
        minio_client (minio client Obj): Client obj connection to Minio
        bucket_name (String): Name of the bucket to get objects for
    Returns:
        objects (List of Object Objs): List of Object OBJs in Minio
    """

    objects = []

    try:
        objects = list(minio_client.list_objects(bucket_name))
    except ResponseError as con_err:
        logging.error(f"Error Connecting to Minio: {con_err}")
        raise err
    except Exception as err:
        logging.error(f"Error Listing Objects: {err}")
        raise err

    return objects


def get_object_names(minio_client, bucket_name):
    """
    Purpose:
        Get a list of objects that exist in the Minio Client
    Args:
        minio_client (minio client Obj): Client obj connection to Minio
        bucket_name (String): Name of the bucket to get objects for
    Returns:
        object_names (List of Strings): List of Objects in Minio
    """

    object_names = []

    try:
        object_names =\
            [object.object_name for object in minio_client.list_objects(bucket_name)]
    except ResponseError as con_err:
        logging.error(f"Error Connecting to Minio: {con_err}")
        raise err
    except Exception as err:
        logging.error(f"Error Listing Objects: {err}")
        raise err

    return object_names


def is_object_in_bucket(minio_client, bucket_name, object_name):
    """
    Purpose:
        Check if Object exists in Bucket
    Args:
        minio_client (minio client Obj): Client obj connection to Minio
        bucket_name (String): Name of the bucket to check for object
        object_name (String): Name of object to check for in Minio
    Returns:
        object_exists (Boolean): Boolean if the object exists or not
    """

    return True if object_name in get_object_names(minio_client, bucket_name) else False


def get_object_stats(minio_client, bucket_name, object_name):
    """
    Purpose:
        Get Stats of the Object
    Args:
        minio_client (minio client Obj): Client obj connection to Minio
        bucket_name (String): Name of the bucket to check for object
        object_name (String): Name of object to get stats for in Minio
    Returns:
        object_stats (Dict):Dict of stats about the object
    """

    object_stats = {}

    try:
        object_stats_obj = minio_client.stat_object(bucket_name, object_name)
        object_stats = {
            "bucket_name": object_stats_obj.bucket_name,
            "content_type": object_stats_obj.content_type,
            "etag": object_stats_obj.etag,
            "is_dir": object_stats_obj.is_dir,
            "last_modified_obj": object_stats_obj.last_modified,
            "last_modified_readable":
                strftime("%a, %d %b %Y %H:%M:%S", object_stats_obj.last_modified),
            "last_modified_int":
                int(strftime("%Y%m%d%H%M%S", object_stats_obj.last_modified)),
            "object_name": object_stats_obj.object_name,
            "metadata": object_stats_obj.metadata,
            "size": object_stats_obj.size,
        }
    except ResponseError as con_err:
        logging.error(f"Error Connecting to Minio: {con_err}")
        raise err
    except Exception as err:
        logging.error(f"Error Listing Objects: {err}")
        raise err

    return object_stats


###
# Object Manipulation Helpers
###


def download_object_to_memory(minio_client, bucket_name, object_name, encoding="utf-8"):
    """
    Purpose:
        Download an Object from Mino into memory (if supported)
    Args:
        minio_client (minio client Obj): Client obj connection to Minio
        bucket_name (String): Name of the bucket to get object from
        filename (String): Location (And Path) of file to upload
        object_name (String): Name of object to upload in Minio
    Returns:
        parsed_object (Obj, depending on extension): Object parsed from Minio from the
            extension of the file. Current supported = .txt -> str, .json -> Dict/JSON
    """
    logging.info(f"Downloading Object {bucket_name}/{object_name} into Memory")

    parsed_object = None

    try:
        minio_object = minio_client.get_object(bucket_name, object_name)

        file_extension = object_name.split(".")[-1]
        if file_extension == "txt":
            decoded_object = minio_object.read().decode("utf-8")
            parsed_object = decoded_object
        elif file_extension == "json":
            decoded_object = minio_object.read().decode("utf-8")
            parsed_object = json.loads(decoded_object)
        else:
            error_msg =\
                f"File Extension {file_extension} Does Not Support Download into Memory"
            logging.error(error_msg)
            raise ObjectDecodingNotSupported(error_msg)
    except ResponseError as con_err:
        logging.error(f"Error Connecting to Minio: {con_err}")
        raise con_err
    except NoSuchKey as no_key_err:
        logging.error(f"Key Doesn't Exist in Minio: {no_key_err}")
        raise no_key_err
    except Exception as err:
        logging.error(f"Error Creating Object {object_name}: {err}")
        raise err

    return parsed_object


def download_object_to_file(minio_client, bucket_name, object_name, filename=None):
    """
    Purpose:
        Download a file from Minio to local storage
    Args:
        minio_client (minio client Obj): Client obj connection to Minio
        bucket_name (String): Name of the bucket to get object from
        filename (String): Location (And Path) of file to upload
        object_name (String): Name of object to upload in Minio
    Returns:
        N/A
    """
    logging.info(f"Downloading Object {bucket_name}/{object_name} to {filename}")

    if not filename:
        filename = f"./{object_name}"

    try:
        minio_object = minio_client.fget_object(bucket_name, object_name, filename)
    except ResponseError as con_err:
        logging.error(f"Error Connecting to Minio: {con_err}")
        raise con_err
    except NoSuchKey as no_key_err:
        logging.error(f"Key Doesn't Exist in Minio: {no_key_err}")
        raise no_key_err
    except Exception as err:
        logging.error(f"Error Creating Object {object_name}: {err}")
        raise err



def upload_object(minio_client, bucket_name, filename, object_name=None):
    """
    Purpose:
        Uploading a local file to Minio
    Args:
        minio_client (minio client Obj): Client obj connection to Minio
        bucket_name (String): Name of the bucket to get to upload object to
        filename (String): Location (And Path) of file to upload
        object_name (String): Name of object to upload in Minio
    Returns:
        N/A
    """
    logging.info(f"Uploading Object {filename} to {bucket_name}/{object_name}")

    if not object_name:
        object_name = filename.split("/")[-1]

    import pdb; pdb.set_trace()

    # try:
    #     if not minio_client.object_exists(object_name):
    #         minio_client.make_object(
    #             object_name, location="us-east-1"
    #         )
    #     else:
    #         raise ObjectAlreadyExists(f"{object_name} Already Exists in Minio")
    # except ResponseError as con_err:
    #     logging.error(f"Error Connecting to Minio: {con_err}")
    #     raise con_err
    # except Exception as err:
    #     logging.error(f"Error Creating Object {object_name}: {err}")
    #     raise err


def delete_object(minio_client, bucket_name, object_name):
    """
    Purpose:
        Delete a specified Object by name
    Args:
        minio_client (minio client Obj): Client obj connection to Minio
        object_name (String): Name of object in Minio to delete
    Returns:
        N/A
    """
    logging.info(f"Deleting Object {object_name}")

    # try:
    #     if minio_client.object_exists(object_name):
    #         minio_client.remove_object(object_name)
    #     else:
    #         raise ObjectDoesntExist(f"{object_name} Doesn't Exist in Minio")
    # except ResponseError as con_err:
    #     logging.error(f"Error Connecting to Minio: {con_err}")
    #     raise con_err
    # except Exception as err:
    #     logging.error(f"Error Deleting Object: {err}")
    #     raise err
