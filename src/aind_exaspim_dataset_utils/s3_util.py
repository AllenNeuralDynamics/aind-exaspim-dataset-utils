"""
Created on Thur July 24 14:00:00 2025

@author: Anna Grim
@email: anna.grim@alleninstitute.org

Helper routines for working with images stored in an S3 bucket.

"""

from botocore import UNSIGNED
from botocore.config import Config

import boto3
import numpy as np
import os
import s3fs
import zarr

from aind_exaspim_dataset_utils import io_util


# --- General ---
def exists_in_prefix(bucket_name, prefix, target_name):
    """
    Checks if a subprefix or file with named "target_name" is in the given
    prefix.

    Parameters
    ----------
    bucket_name : str
        Name of the S3 bucket to search.
    prefix : str
        S3 prefix to search within.
    target_name : str
        Name to search for.

    Returns
    -------
    bool
        Indiciation of whether a given file is in a prefix.
    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
    for page in pages:
        # Check for files directly under the prefix
        for obj in page.get("Contents", []):
            name = obj["Key"].split("/")[-1]
            if name == target_name:
                return True

        # Check for immediate subdirectories
        for common_prefix in page.get("CommonPrefixes", []):
            name = common_prefix["Prefix"].rstrip("/").split("/")[-1]
            if name == target_name:
                return True
    return False


def list_bucket_prefixes(bucket_name, keyword=None):
    """
    Lists all top-level prefixes (directories) in an S3 bucket, optionally
    filtering by a keyword.

    Parameters
    -----------
    bucket_name : str
        Name of the S3 bucket to search.
    keyword : str, optional
        Keyword used to filter the prefixes.

    Returns
    --------
    List[str]
        Top-level prefixes (directories) in the S3 bucket. If a keyword is
        provided, only the matching prefixes are returned.
    """
    prefixes = list()
    continuation_token = None
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    while True:
        # Call the list_objects_v2 API
        list_kwargs = {"Bucket": bucket_name, "Delimiter": "/"}
        if continuation_token:
            list_kwargs["ContinuationToken"] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)

        # Collect the top-level prefixes
        if "CommonPrefixes" in response:
            for prefix in response["CommonPrefixes"]:
                if keyword and keyword in prefix["Prefix"].lower():
                    prefixes.append(prefix["Prefix"])
                elif keyword is None:
                    prefixes.append(prefix["Prefix"])

        # Check if there are more pages to fetch
        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break
    return prefixes


def list_prefixes(bucket_name, prefix):
    """
    Lists all immediate subdirectories of a given S3 path (prefix).

    Parameters
    -----------
    bucket_name : str
        Name of the S3 bucket to search.
    prefix : str
        S3 prefix (path) to search within.

    Returns:
    --------
    List[str]
        Immediate subdirectories under the specified prefix.
    """
    # Check prefix ending
    if not prefix.endswith("/"):
        prefix += "/"

    # Call the list_objects_v2 API
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    response = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=prefix, Delimiter="/"
    )
    if "CommonPrefixes" in response:
        return [cp["Prefix"] for cp in response["CommonPrefixes"]]
    else:
        return list()


# --- Image Prefix Search ---
def get_img_prefix(brain_id, prefix_lookup_path=None):
    """
    Gets the image prefix path for a given brain ID.

    Parameters
    ----------
    brain_id : str
        Identifier for the brain dataset.
    prefix_lookup_path : str or None
        Optional path to a JSON file that caches brain ID to prefix mappings.

    Returns
    -------
    str
        Image path for the given brain ID.
    """
    # Check prefix lookup
    if prefix_lookup_path:
        prefix_lookup = io_util.read_json(prefix_lookup_path)
        if brain_id in prefix_lookup:
            return prefix_lookup[brain_id]

    # Find prefix path
    result = find_img_prefix(brain_id)
    if len(result) == 1:
        prefix = result[0] + "/"
        if prefix_lookup_path:
            prefix_lookup[brain_id] = prefix
            io_util.write_json(prefix_lookup_path, prefix_lookup)
        return prefix

    raise Exception(f"Image Prefixes Found - {result}")


def find_img_prefix(brain_id):
    """
    Finds the image prefix path for a given brain ID.

    Parameters
    ----------
    brain_id : str
        Brain ID used to find image prefix.

    Returns
    -------
    str
        Image prefix corresponding to the given brain ID.
    """
    # Initializations
    bucket_name = "aind-open-data"
    keyword = f"exaspim_{brain_id}"
    prefixes = list_bucket_prefixes(bucket_name, keyword=keyword)

    # Get possible prefixes
    valid_prefixes = list()
    for prefix in prefixes:
        # Check for new naming convention
        if exists_in_prefix(bucket_name, prefix, "fusion"):
            prefix = os.path.join(prefix, "fusion")

        # Check if prefix is valid
        if is_valid_img_prefix(bucket_name, prefix, brain_id):
            prefix = os.path.join("s3://aind-open-data", prefix, "fused.zarr")

            # Check if shape is plausible
            if is_shape_plausible(prefix):
                valid_prefixes.append(prefix)
    return valid_prefixes


def is_valid_img_prefix(bucket_name, prefix, brain_id):
    """
    Determines whether a given image prefix is valid for a specific brain ID.
    This function performs the following checks:
        1. Ensures the prefix contains the correct "brain_id".
        2. Rejects any prefix that contains the string "test".
        3. Verifies that the prefix contains a "fused.zarr" directory.
        4. Confirms that all expected multiscale image levels (0–7) are
           present under the "fused.zarr" prefix.

    Parameters
    ----------
    bucket_name : str
        Name of the S3 bucket containing the given prefix.
    prefix : str
        Path prefix to check, relative to the root of the bucket.
    brain_id : str or int
        Brain ID used to determine if the image prefix is valid.

    Returns
    -------
    bool
        True if the prefix meets all criteria for a valid image dataset,
        False otherwise.
    """
    # Quick checks
    is_test = "test" in prefix.lower()
    has_correct_id = str(brain_id) in prefix
    if not has_correct_id or is_test:
        return False

    # Check if prefix is function
    if exists_in_prefix(bucket_name, prefix, "fused.zarr"):
        img_prefix = os.path.join(prefix, "fused.zarr")
        multiscales = list_prefixes(bucket_name, img_prefix)
        multiscales = [s.split("/")[-2] for s in multiscales]
        for s in map(str, range(0, 8)):
            if s not in multiscales:
                return False
    return True


def is_shape_plausible(prefix):
    """
    Checks if the highest resolution (level 0) contains image data with
    plausible dimensions.

    Parameters
    ----------
    prefix : str
        Image prefixe to be checked.

    Returns
    -------
    bool
        Indication of whether the shape is plausible.
    """
    try:
        root = os.path.join(prefix, str(0))
        store = s3fs.S3Map(root=root, s3=s3fs.S3FileSystem(anon=True))
        img = zarr.open(store, mode="r")        
        if np.max(img.shape) > 25000:
            return True
    except Exception as e:
        pass
    return False
