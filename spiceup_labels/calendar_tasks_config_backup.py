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
import numpy as np
labeltype_uuid = "3d77fb10-1a2c-40ef-8396-f2bc2cd638e1"

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
        for j in range(dif):
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
fertilizer_rasters = {
    "n_01y_lampung_tree": "7706d60e-e9a2-4841-8018-138cf5aea535",
    "n_01y_lampung_pole": "ef2a7670-e924-4676-a50c-4bb4f6c2cb1d",
    "n_01y_lungpuk_tree": "abed5c7b-200c-4775-ba12-d431da849a4c",
    "n_01y_lungpuk_pole": "7b88ae1d-2e33-455e-a413-3642cc0b01df",
    "n_13y_lampung_tree": "e8d3a345-4668-4ddf-9f21-ff2b166c8d7a",
    "n_13y_lampung_pole": "f5a07db9-6fa2-4dcc-89b5-a3571b81a930",
    "n_13y_lungpuk_tree": "43b3b3ed-f4e5-4138-bc9e-b6ce123fa18a",
    "n_13y_lungpuk_pole": "443ff09b-503d-4a74-b905-76b7357fbb71",
    "n_3py_lampung_tree": "80de539e-8890-4961-b8d8-c3c76a7ff28d",
    "n_3py_lampung_pole": "f7758894-1f3c-4914-8c11-e2cc3839158d",
    "n_3py_lungpuk_tree": "94adeb0e-1b5c-493c-8e8c-714f31478627",
    "n_3py_lungpuk_pole": "0670b6fa-59d1-4ddc-9fc3-f9f8183719eb",
    "p_01y_lampung_tree": "58b3d215-1c3c-4577-b2d7-744e54fdf14f",
    "p_01y_lampung_pole": "e41d059a-4bad-4708-bfbe-77cd361493d7",
    "p_01y_lungpuk_tree": "27d1cc96-5348-414f-8b68-d4bc1c482754",
    "p_01y_lungpuk_pole": "fa543881-6273-4665-8d6f-9ce8ee03098e",
    "p_13y_lampung_tree": "be836df1-6fec-4f08-98db-ba994f8e315b",
    "p_13y_lampung_pole": "3dea7cc1-300f-41c2-9dc3-1d329d07269b",
    "p_13y_lungpuk_tree": "de677491-d12e-4d37-872e-422e8dc55e77",
    "p_13y_lungpuk_pole": "8d096110-f89e-493d-b81b-56a6fa5c50df",
    "p_3py_lampung_tree": "e618eb61-6d50-4dab-b92f-a401979c361e",
    "p_3py_lampung_pole": "28cc41c7-8fb8-4844-bdd8-fb6c61d9357d",
    "p_3py_lungpuk_tree": "54df6041-ebd7-4b5a-95fb-000b52773725",
    "p_3py_lungpuk_pole": "d684ead3-251f-4a53-b4c0-b79515697d71",
    "k_01y_lampung_tree": "c8c639b3-d603-479e-be18-efe142733611",
    "k_01y_lampung_pole": "86f503da-0898-4e61-85f3-ecc3c58f6599",
    "k_01y_lungpuk_tree": "5f313e8b-21a1-42ba-aa75-df888d68d921",
    "k_01y_lungpuk_pole": "8a3b3845-0685-484e-8415-a7aebd7b2536",
    "k_13y_lampung_tree": "ea51741d-db9a-447b-84db-29bcb6b18e04",
    "k_13y_lampung_pole": "f164a9b8-1983-4910-a98f-cd25c85edd60",
    "k_13y_lungpuk_tree": "940f0fcc-03f4-4375-b899-f0c097245465",
    "k_13y_lungpuk_pole": "6b57a117-766d-420e-a002-79c58e018e15",
    "k_3py_lampung_tree": "5a31c623-2b3c-4b7a-a469-959d82af607d",
    "k_3py_lampung_pole": "82928d83-aded-4d76-9ee5-9328656ec9c9",
    "k_3py_lungpuk_tree": "862c1c24-72d9-4537-bc5f-a8ec41969567",
    "k_3py_lungpuk_pole": "56e78316-35ef-4e25-8ce3-d274bd8f3c69",
}

season_rasters = {
    "days_since_epoch_raster": "6bd5cfd0-0f74-456c-8dd3-6c4e6b0dad2e",
    "doy_start_dry_season_raster": "e322f94d-96f4-4d61-8965-e9bf05e082e2",
    "doy_start_rainy_season_raster": "7d6f44aa-d419-4f21-ab8f-8f81f7c01c79",
}
lizard_rasters = {**fertilizer_rasters, **season_rasters}


# Load parcels locally. Mimic Lizard data, we replace it later with Lizard parcels
parcels = sources.GeometryFileSource("parcels.geojson")

# Add labelparameters
labelparams = [
    "farm_area",
    "number_trees",
    "days_plant_age",
    "days_since_epoch",
    "pepper_variety",
    "live_support",
]
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
labelparams = [
    "farm_area",
    "number_trees",
    "days_plant_age",
    "days_since_epoch",
    "pepper_variety",
    "live_support",
]
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
    1: ["n_01y_lampung_tree_sb", "p_01y_lampung_tree_sb", "k_01y_lampung_tree_sb"],
    2: ["n_01y_lampung_pole_sb", "p_01y_lampung_pole_sb", "k_01y_lampung_pole_sb"],
    3: ["n_01y_lungpuk_tree_sb", "p_01y_lungpuk_tree_sb", "k_01y_lungpuk_tree_sb"],
    4: ["n_01y_lungpuk_pole_sb", "p_01y_lungpuk_pole_sb", "k_01y_lungpuk_pole_sb"],
    5: ["n_13y_lampung_tree_sb", "p_13y_lampung_tree_sb", "k_13y_lampung_tree_sb"],
    6: ["n_13y_lampung_pole_sb", "p_13y_lampung_pole_sb", "k_13y_lampung_pole_sb"],
    7: ["n_13y_lungpuk_tree_sb", "p_13y_lungpuk_tree_sb", "k_13y_lungpuk_tree_sb"],
    8: ["n_13y_lungpuk_pole_sb", "p_13y_lungpuk_pole_sb", "k_13y_lungpuk_pole_sb"],
    9: ["n_3py_lampung_tree_sb", "p_3py_lampung_tree_sb", "k_3py_lampung_tree_sb"],
    10: ["n_3py_lampung_pole_sb", "p_3py_lampung_pole_sb", "k_3py_lampung_pole_sb"],
    11: ["n_3py_lungpuk_tree_sb", "p_3py_lungpuk_tree_sb", "k_3py_lungpuk_tree_sb"],
    12: ["n_3py_lungpuk_pole_sb", "p_3py_lungpuk_pole_sb", "k_3py_lungpuk_pole_sb"],
}

# Option to write calendar tasks to file
# tasks_table = calendar_tasks_monthly[["month", "id_days_start", "task", "task_IND"]]
# tasks_table.to_html('all_tasks.html', index=False)
# tasks_table.to_csv('all_tasks.csv', index=False)
