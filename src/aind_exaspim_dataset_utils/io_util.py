"""
Created on Thur July 24 14:00:00 2025

@author: Anna Grim
@email: anna.grim@alleninstitute.org

Helper routines used in this repo.

"""

import json


# --- Read ---
def read_json(path):
    """
    Reads JSON file located at the given path.

    Parameters
    ----------
    path : str
        Path to JSON file to be read.

    Returns
    -------
    dict
        Contents of JSON file.
    """
    with open(path, "r") as f:
        return json.load(f)


# --- Write ---
def write_json(path, contents):
    """
    Writes "contents" to a JSON file at "path".

    Parameters
    ----------
    path : str
        Path that txt file is written to.
    contents : dict
        Contents to be written to JSON file.

    Returns
    -------
    None
    """
    with open(path, "w") as f:
        json.dump(contents, f)
