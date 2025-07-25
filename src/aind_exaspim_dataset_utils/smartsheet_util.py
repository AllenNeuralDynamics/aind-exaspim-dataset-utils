"""
Created on Mon July 25 14:00:00 2025

@author: Anna Grim
@email: anna.grim@alleninstitute.org

Helper routines for working with ExaSPIM SmartSheets.

"""

from collections import defaultdict

import ast
import pandas as pd
import smartsheet


class SmartSheetClient:
    """
    A client interface for accessing and interacting with Smartsheet sheets.

    Attributes
    ----------
    client : smartsheet.Smartsheet
        Smartsheet API client object.
    sheet_name : str
        Name of the sheet to retrieve.
    sheet_id : int
        ID of the retrieved sheet.
    sheet : smartsheet.models.Sheet
        Loaded sheet object.
    column_name_to_id : dict
        Dictionary mapping column names (titles) to their corresponding IDs.
    """

    def __init__(self, access_token, sheet_name, is_workspace_sheet=False):
        """
        Instantiates a SmartSheetClient object.

        Parameters
        ----------
        access_token : str
            Personal access token (PAT) used to authenticate with the
            Smartsheet API.
        sheet_name : str
            Name of the sheet to retrieve.
        is_workspace_sheet : bool, optional
            Indication of whether to look for the sheet within a workspace.
            Default is False.

        Returns
        -------
        None
        """
        # Instance attributes
        self.client = smartsheet.Smartsheet(access_token)
        self.sheet_name = sheet_name

        # Open sheet
        if is_workspace_sheet:
            self.sheet_id = self.find_workspace_sheet_id()
        else:
            self.sheet_id = self.find_sheet_id()
        self.sheet = self.client.Sheets.get_sheet(self.sheet_id)
        print(self.sheet)

        # Lookups
        self.column_name_to_id = {c.title: c.id for c in self.sheet.columns}

    # --- Lookup Routines ---
    def find_workspace_sheet_id(self):
        """
        Searches all accessible workspaces for a sheet with the given name.

        Parameters
        ----------
        None

        Returns
        -------
        int
            ID of the matching sheet.
        """
        for ws in self.client.Workspaces.list_workspaces().data:
            workspace = self.client.Workspaces.get_workspace(ws.id)
            for sheet in workspace.sheets:
                if sheet.name == self.sheet_name:
                    return sheet.id
        raise Exception(f"Sheet Not Found - sheet_name={self.sheet_name}")

    def find_sheet_id(self):
        """
        Searches all user-accessible sheets (not in workspaces) for a sheet
        with the given name.

        Parameters
        ----------
        None

        Returns
        -------
        int
            ID of the matched sheet.
        """
        response = self.client.Sheets.list_sheets()
        for sheet in response.data:
            if sheet.name == self.sheet_name:
                return sheet.id
        raise Exception(f"Sheet Not Found - sheet_name={self.sheet_name}")

    def find_row_id(self, keyword):
        """
        Locates the row ID of the first row containing a cell with the given
        keyword.

        Parameters
        ----------
        keyword : str
            Display value to search for in the sheet's cells.

        Returns
        -------
        int
            ID of the row containing the keyword.
        """
        for row in self.sheet.rows:
            for cell in row.cells:
                if cell.display_value == keyword:
                    return row.id
        raise Exception(f"Row Not Found - keyword={keyword}")

    # --- Getters ---
    def get_children_map(self):
        """
        Builds a mapping of parent row indices to their child row indices.

        Parameters
        ----------
        None

        Returns
        -------
        dict[int, List[int]]
            Dictionary mapping parent row indices to lists of child row
            indices.
        """
        children_map = defaultdict(list)
        idx_lookup = {row.id: idx for idx, row in enumerate(self.sheet.rows)}
        for row in self.sheet.rows:
            if row.parent_id:
                parent_idx = idx_lookup[row.parent_id]
                child_idx = idx_lookup[row.id]
                children_map[parent_idx].append(child_idx)
        return children_map

    def get_rows_in_column_with(self, column_name, row_value):
        """
        Finds row indices where a specified column matches a given value.

        Parameters
        ----------
        column_name : str
            Name of the column to search.
        row_value : str
            Value to match (case-insensitive).

        Returns
        -------
        List[int]
            Row indices where the column value matches 'row_value'.
        """
        row_idxs = list()
        col_id = self.column_name_to_id[column_name]
        for idx, row in enumerate(self.sheet.rows):
            cell = next((c for c in row.cells if c.column_id == col_id), None)
            value = cell.display_value or cell.value
            if isinstance(value, str):
                if value.lower() == row_value.lower():
                    row_idxs.append(idx)
        return row_idxs

    def get_value(self, row_idx, column_name):
        """
        Retrieve the value of a cell in the given row and column.

        Parameters
        ----------
        row_idx : int
            Index of the row to access.
        column_name : str
            Name of the column from which to retrieve the value.

        Returns
        -------
        str or any
            Cell's display value if available, otherwise the raw value.
        """
        row = self.sheet.rows[row_idx]
        col_id = self.column_name_to_id[column_name]
        cell = next((c for c in row.cells if c.column_id == col_id), None)
        return cell.display_value or cell.value

    # --- Miscellaneous ---
    def to_dataframe(self):
        """
        Converts the entire sheet into a pandas DataFrame.

        Parameters
        ----------
        None

        Returns
        -------
        pd.DataFrame
            A DataFrame containing all rows and columns from the sheet.
            Column names are based on the Smartsheet column titles.
        """
        # Extract column titles
        columns = list(self.column_name_to_id.keys())

        # Extract row data
        data = []
        for row in self.sheet.rows:
            row_data = []
            for cell in row.cells:
                val = cell.value if cell.display_value else cell.display_value
                row_data.append(val)
            data.append(row_data)
        return pd.DataFrame(data, columns=columns)

    def update_rows(self, updated_row):
        """
        Pushes updates for a row to the Smartsheet.

        Parameters
        ----------
        updated_row : smartsheet.models.Row
            Aow object with updated values to be written to the sheet.

        Returns
        -------
        None
        """
        self.client.Sheets.update_rows(self.sheet_id, [updated_row])


# --- ExaSPIM Merge Locations ---
def extract_merge_sites(smartsheet_client, verbose=True):
    """
    Extracts confirmed merge sites from the 'ExaSPIM Merge Locations'
    Smartsheet.

    Parameters
    ----------
    smartsheet_client : SmartSheetClient
        Instance of SmartSheetClient that provides access to the sheet.
    verbose : bool, optional
        Indication of whether to printout merge sites stats while loading.

    Returns
    -------
    pd.DataFrame
        DataFrame containing merge site coordinates and associated metadata,
        including 'brain_id' and 'segmentation_id'.
    """
    children_map = smartsheet_client.get_children_map()
    merge_site_dfs = list()
    n_merge_sites, n_reviewed_sites = 0, 0
    for parent_idx, child_idxs in children_map.items():
        # Extract information
        sample_name = smartsheet_client.get_value(parent_idx, "Sample")
        brain_id, segmentation_id = sample_name.split("_", 1)
        sites, n = find_confirmed_merge_sites(smartsheet_client, child_idxs)

        # Compile results
        if len(sites["xyz"]) > 0:
            results = {
                "brain_id": len(sites["xyz"]) * [brain_id],
                "segmentation_id": len(sites["xyz"]) * [segmentation_id]
            }
            results.update(sites)
            merge_site_dfs.append(pd.DataFrame(results))

            n_reviewed_sites += n
            n_merge_sites += len(sites["xyz"])
            success_rate = len(sites["xyz"]) / n
            if verbose:
                print(f"{brain_id} - Success Rate:", success_rate)

    # Report results
    if verbose:
        print("\nOverall Success Rate:", n_merge_sites / n_reviewed_sites)
        print("# Confirmed Merge Sites:", n_merge_sites)
    return pd.concat(merge_site_dfs, ignore_index=True)


def find_confirmed_merge_sites(smartsheet_client, idxs):
    """
    Identifies confirmed merge sites from a list of Smartsheet row indices.

    Parameters
    ----------
    smartsheet_client : SmartSheetClient
        Instance of the SmartSheetClient used to fetch cell values.
    idxs : list of int
        Row indices to evaluate for merge confirmations.

    Returns
    -------
    tuple
        A pair containing:
        - dict with keys:
            - "segment_id": list of segmentation IDs
            - "groundtruth_id": list of ground truth IDs
            - "xyz": list of (x, y, z) coordinates
        - int: number of reviewed sites (regardless of confirmation)
    """
    sites = {"segment_id": [], "groundtruth_id": [], "xyz": []}
    n_reviewed_sites = 0
    for i in idxs:
        is_merge = smartsheet_client.get_value(i, "Merge Confirmation")
        is_reviewed = smartsheet_client.get_value(i, "Reviewed?")
        if is_merge and is_reviewed:
            sites["segment_id"].append(
                smartsheet_client.get_value(i, "Segmentation ID")
            )
            sites["groundtruth_id"].append(
                smartsheet_client.get_value(i, "Ground Truth ID")
            )
            sites["xyz"].append(
                read_xyz(smartsheet_client.get_value(i, "World Coordinates"))
            )
        if is_reviewed:
            n_reviewed_sites += 1
    return sites, n_reviewed_sites


# --- Helpers ---
def read_xyz(xyz_str):
    """
    Parses a string representation of a 3D coordinate into a Python tuple.

    Parameters
    ----------
    xyz_str : str
        String representation of a 3D coordinate.

    Returns
    -------
    Tuple[float]
        3D coordinate.
    """
    return ast.literal_eval(xyz_str)
