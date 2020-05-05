"""
Config data for the crop calendar tasks labeltype
"""

import dask_geomodeling as dg
import gspread
import pandas as pd
from dask_geomodeling.raster import *
from dask_geomodeling.geometry import *
from dask_geomodeling.geometry.aggregate import AggregateRaster
from dask_geomodeling.geometry.base import GetSeriesBlock, SetSeriesBlock
from oauth2client.service_account import ServiceAccountCredentials

labeltype_uuid = "lizard_uuid"  # e.g. "3d77fb10-1a2c-40ef-8396-f2bc2cd638e1"

# ----------------------------------------------------------
# Load online spreadsheet "Items & Properties on the App Ui/x", specifically the crop calendar tasks
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("client_secret.json", scope)
client = gspread.authorize(creds)
tabel_suci = client.open("Items & Properties on the App Ui/x")
ws = tabel_suci.worksheet("Crop calendar tasks")
calendar_tasks = pd.DataFrame(columns=ws.row_values(1))
rowcount = len(ws.col_values(1)) - 1  # First row is header
for i in range(len(calendar_tasks.columns)):
    colname = calendar_tasks.columns[i]
    col = ws.col_values(i + 1)[1:]
    dif = rowcount - len(col)
    if dif > 0:
        for i in range(dif):
            col.append(np.nan)
    calendar_tasks[colname] = col

# Make calendar_tasks columns numeric where possible
cols = calendar_tasks.columns  # .drop('id')
calendar_tasks[cols] = calendar_tasks[cols].apply(pd.to_numeric, errors="ignore")

# Add next task columns to calendar_tasks
calendar_tasks = calendar_tasks.sort_values(
    by=["month", "task_id"]
)  # sort by task_id, month

# ----------------------------------------------------------
# Load rasters for calendar based tasks
lizard_rasters = {
    "raster_name_1": "raster_uuid_1",  # e.g. "n_01y_lampung_tree": "7706d60e-e9a2-4841-8018-138cf5aea535"
    "raster_name_2": "raster_uuid_2",  # e.g. "n_01y_lampung_pole": "ef2a7670-e924-4676-a50c-4bb4f6c2cb1d",
}

# Load parcels locally. Mimic Lizard data, we replace it later with Lizard parcels
parcels = sources.GeometryFileSource("parcels.geojson")

# Add labelparameters
labelparams = [
    "labelparam_1",
    "labelparam_2",
]  # e.g. ["farm_area", "number_trees"]
wkt = "POLYGON((1 2, 2 3, 2 3, 1 2))"

# mimic adding fields to parcels locally, replace it later with AddDjangoFields (lizard specific block)
labelparameters = GeometryFileSource("labelparameters.geojson", id_field="parcel_id")
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

    labeled_parcels[key] = [
        "geoblocks.geometry.sources.AddDjangoFields",
        add_to,
        "lizard_nxt",
        "labelparameter",
        {"label_type__uuid": "3ab1addf-00e5-47b0-849e-ba55cd3024b9", "name": lp},
        {"object_id": "object_id"},
        {"value": lp},
        "start",
        "end",
    ]
    add_to = key

fertilizer_ids_dict = {
    1: ["seriesblock_numeric_1", "seriesblock_numeric_2", "seriesblock_numeric_3"]  # ,
    # e.g. 1: ["n_01y_lampung_tree_sb", "p_01y_lampung_tree_sb", "k_01y_lampung_tree_sb"],
    # e.g. 2: ["n_01y_lampung_pole_sb", "p_01y_lampung_pole_sb", "k_01y_lampung_pole_sb"],
    # e.g. 3: ["n_01y_lungpuk_tree_sb", "p_01y_lungpuk_tree_sb", "k_01y_lungpuk_tree_sb"],
    # e.g. 4: ["n_01y_lungpuk_pole_sb", "p_01y_lungpuk_pole_sb", "k_01y_lungpuk_pole_sb"],
    # e.g. 5: ["n_13y_lampung_tree_sb", "p_13y_lampung_tree_sb", "k_13y_lampung_tree_sb"]
}

# Option to write calendar tasks to file
# tasks_table = calendar_tasks_monthly[["month", "id_days_start", "task", "task_IND"]]
# tasks_table.to_html('all_tasks.html', index=False)
# tasks_table.to_csv('all_tasks.csv', index=False)
