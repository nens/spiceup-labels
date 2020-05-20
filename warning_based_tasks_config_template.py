"""
Config data for the warning based tasks labeltype
"""

import dask_geomodeling as dg
import gspread
import numpy as np
import pandas as pd
from dask_geomodeling.raster import *
from dask_geomodeling.geometry import *
from dask_geomodeling.geometry.aggregate import AggregateRaster
from dask_geomodeling.geometry.base import GetSeriesBlock, SetSeriesBlock
from oauth2client.service_account import ServiceAccountCredentials

labeltype_uuid = "lizard_uuid"  # e.g. "025a748d-4507-4b13-98af-ecae696bbeac"

# ----------------------------------------------------------
# Load online spreadsheet "Items & Properties on the App Ui/x", specifically the warning based tasks
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("client_secret.json", scope)
client = gspread.authorize(creds)
tabel_suci = client.open("Items & Properties on the App Ui/x")
ws = tabel_suci.worksheet("Warning based tasks")
warning_tasks = pd.DataFrame(columns=ws.row_values(1))
rowcount = len(ws.col_values(1)) - 1  # First row is header
for i in range(len(warning_tasks.columns)):
    colname = warning_tasks.columns[i]
    col = ws.col_values(i + 1)[1:]
    dif = rowcount - len(col)
    if dif > 0:
        for i in range(dif):
            col.append(np.nan)
    warning_tasks[colname] = col

# Make warning_tasks columns numeric where possible
cols = warning_tasks.columns  # .drop('id')
warning_tasks[cols] = warning_tasks[cols].apply(pd.to_numeric, errors="ignore")
warning_tasks = warning_tasks.replace(np.nan, "", regex=True)  # remove NaN
warning_tasks = warning_tasks[
    warning_tasks["task_id"] != "WiP"
]  # drop work in progress

# Load rasters for warning based tasks
lizard_rasters = {
    "raster_name_1": "raster_uuid_1",  # e.g. "heavy_rain_warning": "b1b78cc9-bed9-4ac9-b4db-0e6dba626d56",
    "raster_name_2": "raster_uuid_2",  # e.g. "irrigate_01_warning": "8ac70cab-ab4f-4416-9f8e-3a809a0311bd",
}

# Load parcels locally. Mimic Lizard data, we replace it later with Lizard parcels
parcels = sources.GeometryFileSource("parcels.geojson")

# mimic adding fields to parcels locally, replace it later with AddDjangoFields (lizard specific block)
labelparameters = GeometryFileSource("labelparameters.geojson", id_field="parcel_id")

# List general labelparams & warning labelparams (separate because of different labeltype uuids)
general_labelparams = [
    "labelparam_1",
    "labelparam_2",
]  # e.g. ["farm_area", "number_trees"]

warning_labelparams = sorted(list(set(warning_tasks["labelparameter"].to_list())))
labelparams = general_labelparams + warning_labelparams

lp_seriesblocks = {}
for lp in labelparams:
    lp_sb = f"{lp}_sb"
    parcels_l = GeometryFileSource("labelparameters.geojson", id_field=lp)
    lp_parcels = MergeGeometryBlocks(parcels, parcels_l, how="left")
    lp_seriesblocks[lp_sb] = GetSeriesBlock(lp_parcels, lp)
parcels_labeled = MergeGeometryBlocks(parcels, labelparameters, how="left")

# ----------------------------------------------------------
# Load parcels & add labelparameters on initial conditions (e.g. farm_area)
# in: parcels, out: parcels_labeled
labeled_parcels = {
    "parcels": [
        "geoblocks.geometry.sources.GeoDjangoSource",
        "hydra_core",
        "parcel",
        {"id": "object_id", "code": "Plot", "name": "Farm", "external_id": "Farmer"},
        "geometry",
    ]
}

add_to = "parcels"
for lp in labelparams:
    if lp == labelparams[-1]:
        key = "parcels_labeled"
    else:
        key = f"parcels_add_{lp}"
    if lp in general_labelparams:
        param_labeltype_uuid = "3ab1addf-00e5-47b0-849e-ba55cd3024b9"
    else:  # labelparam is in warning_labelparams
        param_labeltype_uuid = (
            labeltype_uuid  # i.e. "025a748d-4507-4b13-98af-ecae696bbeac"
        )
    labeled_parcels[key] = [
        "geoblocks.geometry.sources.AddDjangoFields",
        add_to,
        "lizard_nxt",
        "labelparameter",
        {"label_type__uuid": param_labeltype_uuid, "name": lp},
        {"object_id": "object_id"},
        {"value": lp},
        "start",
        "end",
    ]
    add_to = key

# # Option to write warning tasks to file
# condition_cols = [col for col in warning_tasks.columns if 'condition' in col][:-1]
# warning_tasks['conditions'] = warning_tasks[condition_cols].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
# warning_tasks[["labelparameter", "warning", "conditions"]]
# tasks_table = warning_tasks[["labelparameter", "warning", "conditions"]]
# tasks_table.to_html('warning_tasks_summary.html', index=False)
# tasks_table.to_csv('warning_tasks_summary.csv', index=False)
