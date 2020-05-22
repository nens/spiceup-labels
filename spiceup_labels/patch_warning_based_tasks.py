# -*- coding: utf-8 -*-
"""Configure labeltype model for the warning based tasks of the SpiceUp mobile app.
Used to calculate farm specific tasks from parcel location, plant age, local measurements and raster data.
Warning based tasks are generated with a Lizard labeltype. This labeltype triggers warning based tasks per plot.
Daily label computation (GET compute from mobile app) ensures that warnings reach users.

"""

import argparse
import logging
import dask_geomodeling as dg
import gspread
import logging
import numpy as np
import pandas as pd
import requests
from dask_geomodeling.raster import *
from dask_geomodeling.geometry import *
from dask_geomodeling.geometry.aggregate import AggregateRaster
from dask_geomodeling.geometry.base import GetSeriesBlock, SetSeriesBlock
from oauth2client.service_account import ServiceAccountCredentials
from localsecret import username, password

logger = logging.getLogger(__name__)

from warning_based_tasks_config import (
    labeltype_uuid,  # the uuid of the model
    warning_tasks,  # warning based tasks as specified in  online spreadsheet "Items & Properties on the App Ui/x"
    lizard_rasters,  # Raster data: heavy rain and irrigation warnings dict with names and uuids of the rasters used
    parcels,  # Load parcels locally. Mimic Lizard data
    parcels_labeled,  # parcels with labels locally
    labeled_parcels,  # parcels with labels in lizard
    labelparams,  # List with farm input data: age, location, variety, live support & warning params
    lp_seriesblocks,  # seriesblocks of labelparams per parcel
)
from spiceup_labels.config_lizard import (
    mimic_rasters,
    raster_seriesblocks,
    get_labeltype_source,
    patch_labeltype,
)

# List tasks
def get_tasks_seriesblock(warning_tasks):
    # raster manipulations (TODO create LizardRasterSource when decent alternative for below is available)
    soil_moisture_wet = misc.Reclassify(
        soil_moisture_p90_dry_1_optimal_3_wet_5, [[5, 1]], True
    )
    shade_warning = soil_moisture_wet * 1  # TODO improve
    pests_warning = misc.Mask(soil_moisture_wet, 0)  #  * 1 # TODO improve
    dg_rasters_result = {"shade_warning": shade_warning, "pests_warning": pests_warning}
    sb_objects_results = raster_seriesblocks(dg_rasters_result, parcels_labeled)
    globals().update(sb_objects_results)
    # calculate localized input data to determine if there is a warning
    plant_age = days_since_epoch_raster_sb - days_since_epoch_sb + days_plant_age_sb
    plant_age_01 = plant_age < 365.25
    plant_age_13 = (plant_age > 365.25) * (plant_age < (365.25 * 3))
    plant_age_3p = plant_age > (365.25 * 3)
    irrigate_01 = irrigate_01_warning_sb * plant_age_01
    irrigate_13 = irrigate_13_warning_sb * plant_age_13
    irrigate_3p = irrigate_3p_warning_sb * plant_age_3p
    irrigate_warning = irrigate_01 + irrigate_13 + irrigate_3p
    irrigate_numeric = field_operations.Mask(irrigate_warning, (irrigation_sb == 1), 0)
    shade_task = field_operations.Mask(shade_warning_sb, (shade_sb == 1), 0)
    pests_task = field_operations.Mask(pests_warning_sb, (pests_sb == 1), 0)
    heavy_rain_task = field_operations.Mask(
        heavy_rain_warning_sb, (drainage_sb == 1), 0
    )
    very_heavy_rain_task = field_operations.Mask(
        very_heavy_rain_warning_sb, (drainage_sb == 1), 0
    )

    # calc valid task ids, return 0 or task_id
    # TODO improve P&D risk model
    # the following vars are used in: task_id_lp = eval(f"task_id_{lp_df}")
    task_id_irrigation = field_operations.Classify(
        irrigate_numeric, [30, 50, 100], [0, 2001, 2002, 2003]
    )
    task_id_shade = field_operations.Classify(shade_task, [1], [0, 2004])
    task_id_pests = field_operations.Classify(pests_task, [1], [0, 2005])
    task_id_heavy_rain = field_operations.Classify(pests_task, [1], [0, 2005])
    task_id_very_heavy_rain = field_operations.Classify(pests_task, [1], [0, 2005])
    task_id_drainage = field_operations.Classify(pests_task, [1, 2], [0, 2006, 2007])
    task_id_foot_rot = field_operations.Classify(pests_task, [1], [0, 2008])
    task_id_yellow_disease = field_operations.Classify(pests_task, [1], [0, 2009])
    task_id_viral_disease = field_operations.Classify(pests_task, [1], [0, 2010])
    task_id_pepper_bug = field_operations.Classify(pests_task, [1], [0, 2011])
    task_id_pepper_stem_borer = field_operations.Classify(pests_task, [1], [0, 2012])
    task_id_tingid_bug = field_operations.Classify(pests_task, [1], [0, 2013])
    task_id_velvet_blight = field_operations.Classify(pests_task, [1], [0, 2014])

    tasks_seriesblock = [
        "_XL_",
        irrigate_numeric,
    ]

    for lp in list(warning_tasks["labelparameter"].unique()):
        df_rows = warning_tasks[warning_tasks["labelparameter"] == lp]
        lp_df = df_rows["labelparameter"].values[0]
        if len(df_rows) > 1:
            tasks_seriesblock.append(f"{lp_df}_task_id")
            task_id_lp = eval(f"task_id_{lp_df}")
            task_id_lp = task_id_lp * (task_id_lp > 0)
            tasks_seriesblock.append(task_id_lp)
            task_ids = [int(t) for t in list(df_rows.task_id)]
            for col in list(df_rows.columns[3:10]):
                result_classes = [""]
                for task_id, value in zip(task_ids, list(df_rows[col])):
                    result_classes.append(f"{task_id}_{value}")
                result_name = f"{lp_df}_{col}"
                result = field_operations.Classify(task_id_lp, task_ids, result_classes)
                tasks_seriesblock.append(result_name)
                tasks_seriesblock.append(result)
        else:
            tasks_seriesblock.append(f"{lp_df}_task_id")
            task_id_lp = eval(f"task_id_{lp_df}")
            task_id_lp = task_id_lp * (task_id_lp > 0)
            tasks_seriesblock.append(task_id_lp)
            for col in list(df_rows.columns[3:10]):
                result_name = f"{lp_df}_{col}"
                result_value = f"{list(df_rows.task_id)[0]}_{df_rows[col].values[0]}"
                result = field_operations.Classify(task_id_lp, [1], ["", result_value])
                tasks_seriesblock.append(result_name)
                tasks_seriesblock.append(result)

    return tasks_seriesblock


def get_parser():
    """Return argument parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="Verbose output",
    )
    return parser


def main():  # pragma: no cover
    """Call main command with args from parser.

    This method is called when you run 'bin/run-spiceup-labels',
    this is configured in 'setup.py'. Adjust when needed. You can have multiple
    main scripts.

    """

    options = get_parser().parse_args()
    if options.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s")
    logging.info("load and reclass warning_tasks")

    dg_rasters, graph_rasters = mimic_rasters(lizard_rasters)
    globals().update(dg_rasters)
    sb_objects = raster_seriesblocks(dg_rasters, parcels)
    globals().update(sb_objects)
    globals().update(lp_seriesblocks)

    tasks_seriesblock = get_tasks_seriesblock(warning_tasks)
    # Create seriesblock
    sb_parcels = [
        parcels_labeled,
        "label_value",
        "label",
        "----task_details-----",
        "if ..._task == null or 0: no warning, if ..._task >=1: warning",
    ]
    sb_parcels = sb_parcels + tasks_seriesblock
    result_seriesblock = SetSeriesBlock(*sb_parcels)

    logger.info("serialize model and replace local data with lizard data")
    dg_source = get_labeltype_source(result_seriesblock, graph_rasters, labeled_parcels)
    logger.info("update the labeltype model")
    response = patch_labeltype(dg_source, username, password, labeltype_uuid)
    logger.info("Labeltype update complete. Find response below")
    logger.info(response.json())


if __name__ == "__main__":
    main()
