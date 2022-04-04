# -*- coding: utf-8 -*-
"""Configure labeltype model for the warning based tasks of the SpiceUp mobile app.
Used to calculate farm specific tasks from parcel location, plant age, local measurements and raster data.
Warning based tasks are generated with a Lizard labeltype. This labeltype triggers warning based tasks per plot.
Daily label computation (GET compute from mobile app) ensures that warnings reach users.

"""

import argparse
import logging
import json
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

def check_rainy_season(
        days_since_epoch_raster_sb,
        doy_start_dry_season_raster_sb,
        doy_start_rainy_season_raster_sb
        ):
    
    doy_now_sb = Round(Modulo(days_since_epoch_raster_sb, 365.25))
    days_until_jan_1_sb = doy_now_sb * -1 + 365.25
    
    #Calculate days until dry season
    doy_start_dry_season_sb = Round(doy_start_dry_season_raster_sb)
    pos_days_until_dry_season_sb = days_until_jan_1_sb + doy_start_dry_season_sb
    days_until_dry_season_sb = Modulo(pos_days_until_dry_season_sb, 365.25)
    days_since_start_dry_season_sb = Modulo(
        (pos_days_until_dry_season_sb * -1 + 365.25), 365.25
    )

    #Calculate days until wet season
    doy_start_rainy_season_sb = Round(doy_start_rainy_season_raster_sb)
    pos_days_until_rainy_season_sb = days_until_jan_1_sb + doy_start_rainy_season_sb
    days_until_rainy_season_sb = Modulo(pos_days_until_rainy_season_sb, 365.25)
    days_since_start_rainy_season_sb = Modulo(
        (pos_days_until_rainy_season_sb * -1 + 365.25), 365.25
    )

    #true/false rainy season
    rainy_season = days_since_start_rainy_season_sb < days_since_start_dry_season_sb
    
    return rainy_season


# List tasks
def get_tasks_seriesblock(warning_tasks, rainy_season):
    # raster manipulations (TODO create LizardRasterSource when decent alternative for below is available)
    shade_warning = dry_soil_warning * 1  # TODO improve
    
    dg_rasters_result = {"shade_warning": shade_warning}
    sb_objects_results = raster_seriesblocks(dg_rasters_result, parcels_labeled)
    globals().update(sb_objects_results)
    # calculate localized input data to determine if there is a warning
    plant_age = days_since_epoch_raster_sb - days_since_epoch_sb + days_plant_age_sb
    plant_age_01 = plant_age < 365.25
    plant_age_13 = (plant_age > 365.25) * (plant_age < (365.25 * 3))
    plant_age_3p = plant_age > (365.25 * 3)

    irrigate_warning = dry_soil_warning_sb + very_dry_soil_warning_sb
    irrigate_dry_season = field_operations.Mask(irrigate_warning, rainy_season, 0)
    irrigate_numeric = field_operations.Mask(irrigate_dry_season, (irrigation_sb == 1), 0)
    shade_task = field_operations.Mask(shade_warning_sb, (shade_sb == 1), 0)

    foot_rot_disease_task = field_operations.Mask(foot_rot_disease_raster_sb, (foot_rot_disease_sb == 1), 0)
    yellow_disease_task = field_operations.Mask(yellow_disease_raster_sb, (yellow_disease_sb == 1), 0)
    viral_disease_task = field_operations.Mask(viral_disease_raster_sb, (viral_disease_sb == 1), 0)
    pepper_bug_task = field_operations.Mask(pepper_bug_raster_sb, (pepper_bug_sb == 1), 0)
    stem_borer_task = field_operations.Mask(stem_borer_raster_sb, (stem_borer_sb == 1), 0)
    tingid_bug_task = field_operations.Mask(tingid_bug_raster_sb, (tingid_bug_sb == 1), 0)
    velvet_blight_task = field_operations.Mask(velvet_blight_raster_sb, (velvet_blight_sb == 1), 0)

    heavy_rain_task = field_operations.Mask(
        heavy_rain_warning_sb, (drainage_sb == 1), 0
    )
    very_heavy_rain_task = field_operations.Mask(
        very_heavy_rain_warning_sb, (drainage_sb == 1), 0
    )
    
    drainage_task = heavy_rain_task + very_heavy_rain_task

    # calc valid task ids, return 0 or task_id
    # the following vars are used in: task_id_lp = eval(f"task_id_{lp_df}")
    task_id_irrigation = field_operations.Classify(
        irrigate_numeric, [1, 2], [0, 2001, 2002]
    )
    task_id_shade = field_operations.Classify(shade_task, [1], [0, 2004])
    #task_id_pests = field_operations.Classify(pests_task, [1], [0, 2005])
    task_id_drainage = field_operations.Classify(drainage_task, [1, 2], [0, 2006, 2007])
    task_id_foot_rot_disease = field_operations.Classify(foot_rot_disease_task, [1], [0, 2008], False)
    task_id_yellow_disease = field_operations.Classify(yellow_disease_task, [1], [0, 2009], False)
    task_id_viral_disease = field_operations.Classify(viral_disease_task, [1], [0, 2010], False)
    task_id_pepper_bug = field_operations.Classify(pepper_bug_task, [1], [0, 2011], False)
    task_id_stem_borer = field_operations.Classify(stem_borer_task, [1], [0, 2012], False)
    task_id_tingid_bug = field_operations.Classify(tingid_bug_task, [1], [0, 2013], False)
    task_id_velvet_blight = field_operations.Classify(velvet_blight_task, [1], [0, 2014], False)

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
                result = field_operations.Classify(task_id_lp, task_ids, result_classes, False)
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
    
    logging.info("Calculate if rainy season")
    rainy_season = check_rainy_season(
        days_since_epoch_raster_sb,
        doy_start_dry_season_raster_sb,
        doy_start_rainy_season_raster_sb
        )
    
    logging.info("Calculate tasks")

    tasks_seriesblock = get_tasks_seriesblock(warning_tasks, rainy_season)
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
    with open("warning_based_tasks.json", "w+") as f:
        json.dump(dg_source ,f)
    response = patch_labeltype(dg_source, username, password, labeltype_uuid)
    logger.info("Labeltype update complete. Find response below")
    logger.info(response.json())


if __name__ == "__main__":
    main()
