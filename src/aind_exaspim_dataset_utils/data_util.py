"""
Created on Thur July 24 14:00:00 2025

@author: Anna Grim
@email: anna.grim@alleninstitute.org

Helper routines for loading ExaSPIM related data.

"""

import ast
import pandas as pd

from aind_exaspim_dataset_utils import s3_util


# --- Load Soma Locations ---
def load_soma_locations(brain_id):
    """
    Loads soma location coordinates for a given brain ID from S3.

    Parameters
    ----------
    brain_id : str
        Unique identifier for the whole-brain dataset.

    Returns
    -------
    List[Tuple[int]] or None
        Physical coordinates representing soma locations if data is found;
        otherwise, returns None.
    """
    # Find soma results for brain_id
    bucket_name = 'aind-msma-morphology-data'
    prefix = f"exaspim_soma_detection/{brain_id}"
    prefix_list = s3_util.list_prefixes(bucket_name, prefix)

    # Find most recent result
    if prefix_list:
        dirname = find_most_recent_dirname(prefix_list)
        path = f"s3://{bucket_name}/{prefix}/{dirname}/somas-{brain_id}.csv"
        return list(pd.read_csv(path)["xyz"].apply(ast.literal_eval))
    else:
        return None


def find_most_recent_dirname(results_prefix_list):
    """
    Find the most recent results directory name from a list of S3 prefixes.

    Parameters
    ----------
    results_prefix_list : List[str]
        S3 prefix strings, each containing a directory name formatted as
        "results_YYYYMMDD".

    Returns
    -------
    str
        Directory name with the most recent date.
    """
    dates = list()
    for prefix in results_prefix_list:
        dirname = prefix.split("/")[-2]
        dates.append(dirname.replace("results_", ""))
    return "results_" + sorted(dates)[-1]
