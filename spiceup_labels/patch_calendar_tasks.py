# -*- coding: utf-8 -*-
"""Configure labeltype model for the crop calendar tasks of the SpiceUp mobile app.
Used to calculate farm specific tasks from parcel location, plant age, local measurements and raster data.
Calendar tasks are generated with a Lizard labeltype. This labeltype generates crop calendar tasks per plot.
We save farm plots as parcels, which have a location and several initial parameters.
"""

import argparse
import json
import logging
import numpy as np
import pandas as pd
import requests
import simplejson
from copy import deepcopy
import dask_geomodeling as dg
from dask_geomodeling.raster import *
from dask_geomodeling.geometry import *
from dask_geomodeling.geometry.aggregate import AggregateRaster
from dask_geomodeling.geometry.base import GetSeriesBlock, SetSeriesBlock
from localsecret import username, password
from calendar_tasks_config import (
    labeltype_uuid,  # the uuid of the model
    calendar_tasks,  # crop calendar tasks as specified in  online spreadsheet "Items & Properties on the App Ui/x"
    lizard_rasters,  # Raster data: season onset data, fertilizer recommendations. dict with names and uuids of the rasters used
    parcels,  # Load parcels locally. Mimic Lizard data
    parcels_labeled,  # parcels with labels locally
    labeled_parcels,  # parcels with labels in lizard
    labelparams,  # List with farm input data: age, location, variety, live support
    lp_seriesblocks,  # seriesblocks of labelparams per parcel
    fertilizer_ids_dict,  # Fertilizer conditions 1-12, based on age, variety and (live) support
)
from spiceup_labels.config_lizard import (
    mimic_rasters,
    raster_seriesblocks,
    get_labeltype_source,
    patch_labeltype,
)

logger = logging.getLogger(__name__)

# ----------------------------------------------------------
# preprocess calendar based tasks (filtering)
def get_calendar_tasks_labels(calendar_tasks):
    """convert calendar tasks df (group by by month, season and fertilizer) to
    calendar_tasks_labels df and next_calendar_tasks df"""
    calendar_tasks_one = calendar_tasks[
        calendar_tasks["id_season"] == 0
    ]  # filter tasks to make the tasks independent of local conditions
    calendar_tasks_one = calendar_tasks_one[
        (calendar_tasks_one["fertilizer_data_id"] % 4 == 1)
        | (calendar_tasks_one["fertilizer_data_id"] == 0)
    ]

    calendar_tasks_next = (
        calendar_tasks_one.groupby(
            ["month"]
        )  # group by month to list tasks by plant age
        .agg(
            {
                "task": " | ".join,  # if multiple tasks at the same time, use pipe separator ' | '
                "task_IND": " | ".join,
                "month": "first",  # select first (or last) match with string (comes with pandas)
                "id_days_start": "first",
            }
        )
        .shift(-1)
        .reset_index(drop=True)
    )  # assign next task(s) to current row
    calendar_tasks_join = calendar_tasks.join(
        calendar_tasks_next, on="month", rsuffix="_next"
    )
    calendar_tasks_labels = calendar_tasks_join.iloc[:, np.r_[0:19, 24, -4:0]]
    calendar_tasks_labels = calendar_tasks_labels.sort_values(
        by=["task_id", "month"]
    )  # sort by task_id, month
    calendar_tasks_next.drop(calendar_tasks_next.tail(1).index, inplace=True)
    return calendar_tasks_labels, calendar_tasks_next


def months_n_days(calendar_tasks):
    """List months and convert to days"""
    calendar_tasks_monthly = calendar_tasks.drop_duplicates(subset=["month"])
    months = list(calendar_tasks_monthly.month)
    months.append(months[-1] + 1)
    days_months = [round(month * 365.25 / 12) for month in months]
    days_months_1521 = deepcopy(days_months)
    days_months_1521.append(days_months[-1] + 30)

    # List ages that are ideal in (normal, early, late) rainy or dry season.
    ideal_conditions = sorted(
        list(set([c.split(",")[0] for c in list(calendar_tasks_monthly["ideal_note"])]))
    )
    months_ideal = {}
    for c in ideal_conditions:
        c_months = calendar_tasks_monthly.id_month.where(
            calendar_tasks_monthly.ideal_note.str.endswith(c, False),
            other=calendar_tasks_monthly.id_month + 1000,
        ).to_list()
        c_label = f"months_ideal_{c.lower().replace(' ', '_')}"
        months_ideal[c_label] = c_months
    # add 7 to all months so they become positive
    calendar_tasks_plant_months = [month + 7 for month in months]
    return months_ideal, days_months, days_months_1521, calendar_tasks_plant_months


# ----------------------------------------------------------
def actual_plant_age(
    days_since_epoch_raster_sb,
    days_plant_age_sb,
    days_months,
    days_months_1521,
    calendar_tasks_plant_months,
):
    """calculate actual plant age per plot from epoch raster and plant age labelparameter"""
    doy_now_sb = Round(Modulo(days_since_epoch_raster_sb, 365.25))
    days_until_jan_1_sb = doy_now_sb * -1 + 365.25
    days_since_epoch_plant_sb = days_since_epoch_sb
    days_since_planting_sb = days_since_epoch_raster_sb - days_since_epoch_plant_sb
    plant_age_start_sb = days_plant_age_sb
    plant_age_sb = days_since_planting_sb + plant_age_start_sb
    # plant_month_sb = plant_age_sb / 30.4375
    calendar_tasks_plant_month_sb = Classify(
        plant_age_sb, days_months_1521, calendar_tasks_plant_months, False
    )
    calendar_tasks_plant_year__01_1__123_2__345_3_sb = Classify(
        plant_age_sb, [365, 1095], [1, 2, 3], False
    )
    calendar_tasks_plant_day_min_sb = Classify(
        calendar_tasks_plant_month_sb,
        calendar_tasks_plant_months[1:],
        days_months,
        False,
    )

    shift_days = round((365.25 / 12) * 6 + 1)  # 184
    id_plant_age = plant_age_sb + shift_days
    id_calendar_tasks_plant_day_min_sb = calendar_tasks_plant_day_min_sb + shift_days
    calendar_tasks_plant_day_next_sb = Classify(
        calendar_tasks_plant_month_sb,
        calendar_tasks_plant_months,
        days_months_1521,
        False,
    )
    # id_calendar_tasks_plant_day_next_sb = calendar_tasks_plant_day_next_sb + shift_days
    days_x_1000 = id_calendar_tasks_plant_day_min_sb * 1000
    days_until_next_task = calendar_tasks_plant_day_next_sb - plant_age_sb
    return (
        days_until_jan_1_sb,
        plant_age_sb,
        calendar_tasks_plant_month_sb,
        id_plant_age,
        days_until_next_task,
        days_x_1000,
    )


# ----------------------------------------------------------
def season_conditions(
    days_until_jan_1_sb,
    doy_start_dry_season_raster_sb,
    doy_start_rainy_season_raster_sb,
):
    """Clasify season conditions for (early / late) rainy / dry seasons"""

    # dry season
    doy_start_dry_season_sb = Round(doy_start_dry_season_raster_sb)
    pos_days_until_dry_season_sb = days_until_jan_1_sb + doy_start_dry_season_sb
    days_until_dry_season_sb = Modulo(pos_days_until_dry_season_sb, 365.25)
    days_since_start_dry_season_sb = Modulo(
        (pos_days_until_dry_season_sb * -1 + 365.25), 365.25
    )

    # rainy season
    doy_start_rainy_season_sb = Round(doy_start_rainy_season_raster_sb)
    pos_days_until_rainy_season_sb = days_until_jan_1_sb + doy_start_rainy_season_sb
    days_until_rainy_season_sb = Modulo(pos_days_until_rainy_season_sb, 365.25)
    days_since_start_rainy_season_sb = Modulo(
        (pos_days_until_rainy_season_sb * -1 + 365.25), 365.25
    )

    # plant condition given the current plant age and season progress
    # dry season
    dry_condition = Classify(
        days_since_start_dry_season_sb,  # below_0_ideal_2_above_4_sb
        [0, 120, 242, 366],
        [2, 4, 0],
        False,
    )
    dry_early_condition = Classify(
        days_since_start_dry_season_sb,  # below_01_ideal_2_above_4_sb
        [0, 14, 120, 176, 366],
        [1, 2, 4, 0],
        False,
    )
    dry_late_condition = Classify(
        days_since_start_dry_season_sb,  # below_02_ideal_3_above_4_sb
        [0, 106, 120, 295, 366],
        [2, 3, 4, 0],
        False,
    )

    # rainy season
    rainy_condition = Classify(
        days_since_start_rainy_season_sb,  # below_4_ideal_6_above_7_sb
        [0, 120, 242, 366],
        [6, 7, 4],
        False,
    )
    rainy_early_condition = Classify(
        days_since_start_rainy_season_sb,  # below_4_ideal_5_above_6_sb
        [0, 14, 190, 366],
        [5, 6, 4],
        False,
    )
    rainy_late_condition = Classify(
        days_since_start_rainy_season_sb,  # below_04_ideal_7_above_8_sb
        [0, 106, 120, 295, 366],
        [4, 7, 8, 0],
        False,
    )
    return (
        dry_condition,
        dry_early_condition,
        dry_late_condition,
        rainy_condition,
        rainy_early_condition,
        rainy_late_condition,
    )


def season_state(
    calendar_tasks_plant_month_sb, calendar_tasks_plant_months, months_ideal
):
    """Classify season states # (prefer dry rainy over early/late states)"""
    season_states = {}
    for c_label, c_months in months_ideal.items():
        season_c = c_label.replace("months_ideal_", "").replace("_season", "")
        bool_str = f"{season_c}_bool"
        season_states[bool_str] = (
            Mask(
                calendar_tasks_plant_month_sb,
                Classify(
                    calendar_tasks_plant_month_sb,
                    calendar_tasks_plant_months,
                    c_months,
                    False,
                )
                < 100,
                1,
            )
            < 1000
        ) * 1
    return season_states


def ideal_season_state(season_states, conditions_season):
    """Classify ideal season states # (prefer dry rainy over early/late states)"""
    (
        dry_condition,
        dry_early_condition,
        dry_late_condition,
        rainy_condition,
        rainy_early_condition,
        rainy_late_condition,
    ) = conditions_season

    ideal_state_season = (
        dry_bool * 2
        + dry_early_bool * 1
        + dry_late_bool * 3
        + rainy_bool * 6
        + rainy_early_bool * 5
        + rainy_late_bool * 7
    )
    state_season = (
        dry_bool * dry_condition
        + dry_early_bool * dry_early_condition
        + dry_late_bool * dry_late_condition
        + rainy_bool * rainy_condition
        + rainy_early_bool * rainy_early_condition
        + rainy_late_bool * rainy_late_condition
    )

    # string representation
    season_actual_classes = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5]
    season_actual_strings = [
        "Between rainy and dry ",
        "Dry early",
        "Dry season",
        "Dry late",
        "Between dry and rainy",
        "Rainy early",
        "Rainy season",
        "Rainy late",
        "Between rainy and dry",
    ]
    str_season_actual = Classify(
        state_season, season_actual_classes, season_actual_strings, False
    )

    # Compare season state with ideal state
    state_equals_ideal_state_1_0 = (state_season == ideal_state_season) * 1
    state_greater_ideal_state_2_0 = (state_season > ideal_state_season) * 2
    state_season_below_0_ideal_1_above_2 = (
        state_equals_ideal_state_1_0 + state_greater_ideal_state_2_0
    )
    str_season_state = Classify(
        state_season_below_0_ideal_1_above_2,
        [0, 1, 2, 3],
        ["Below ideal", "Ideal", "Above ideal"],
        False,
    )
    season_below_0_ideal_100_above_200 = state_season_below_0_ideal_1_above_2 * 100
    return season_below_0_ideal_100_above_200


# ----------------------------------------------------------
def get_task_ids(task_id_parts):
    """task id from plant age, season rasters and plot conditions"""
    (
        live_support_sb,
        pepper_variety_sb,
        season_below_0_ideal_100_above_200,
        days_x_1000,
    ) = task_id_parts
    live_support_1_2 = live_support_sb * 1
    pepper_variety_10_20 = field_operations.Classify(
        pepper_variety_sb, [6], [10, 20], False
    )

    identified_task = (
        live_support_1_2
        + pepper_variety_10_20
        + season_below_0_ideal_100_above_200
        + days_x_1000
    )
    identified_task_1 = identified_task + 10000000
    identified_task_2 = identified_task + 20000000
    identified_task_3 = identified_task + 30000000
    # identified_task_4 = identified_task + 40000000
    return identified_task_1, identified_task_2, identified_task_3


def tasks_t1_t2_t3(calendar_tasks_labels):
    """create separate dataframes for tasks that occur on the same date. 
    Each dataframe has a maximum of 1 task per date"""
    t1 = calendar_tasks_labels[
        calendar_tasks_labels.task_id < 2 * 10 ** 7
    ]  # 2*10**7 == 20000000
    t1 = t1.add_prefix("task_1_")
    t1.rename(columns={"task_1_task_id": "task_id"}, inplace=True)
    t2 = calendar_tasks_labels[
        (calendar_tasks_labels.task_id > 2 * 10 ** 7)
        & (calendar_tasks_labels.task_id < 3 * 10 ** 7)
    ]
    t2 = t2.add_prefix("task_2_")
    t2.rename(columns={"task_2_task_id": "task_id"}, inplace=True)
    t3 = calendar_tasks_labels[
        (calendar_tasks_labels.task_id > 3 * 10 ** 7)
        & (calendar_tasks_labels.task_id < 4 * 10 ** 7)
    ]
    t3 = t3.add_prefix("task_3_")
    t3.rename(columns={"task_3_task_id": "task_id"}, inplace=True)
    # t4 = calendar_tasks_labels[(calendar_tasks_labels.task_id > 4*10**7) & (calendar_tasks_labels.task_id < 5*10**7)]
    # t4 = t4.add_prefix('task_4_')
    # t4.rename(columns={"task_4_task_id": "task_id"}, inplace=True)
    return t1, t2, t3


def task_contents(task_dfs, t_identifiers):
    # Reclass task IDs to task contents
    # loop through task dataframes
    # Match possible tasks with identfied task from farm conditions
    tasks_data = {}
    for n, (df, t_identifier) in enumerate(zip(task_dfs, t_identifiers), 1):
        t_ids = df.task_id.to_list()
        t_valid = deepcopy(t_ids)
        t_ids.append(t_ids[-1] + 100)
        t_id_classified = Classify(t_identifier, t_ids, t_valid, False)
        t_diff = Subtract(t_identifier, t_id_classified)
        t_id_match = t_diff < 100
        t_identifier_validated = t_id_classified * t_id_match
        tasks_data[f"t{n}_id_validated"] = t_identifier_validated
        for col in list(df.columns)[1:-9]:
            df[col] = df["task_id"].astype(str) + "_" + df[col].astype(str)
            t_col_list = df[col].to_list()
            t_col_classify = Classify(t_identifier, t_ids, t_col_list, False)
            t_col = Where(t_col_classify, t_id_match, None)
            tasks_data[col] = t_col
    return tasks_data


def next_task_contents(tasks_data, calendar_tasks_next, id_plant_age):
    """add next task once (it is already concatenated)"""
    calendar_tasks_next.id_days_start = calendar_tasks_next.id_days_start.astype(
        "int32"
    )
    bins_start_ids_next_task = calendar_tasks_next.id_days_start.to_list()
    start_ids_next_task = deepcopy(bins_start_ids_next_task)
    bins_start_ids_next_task.insert(0, bins_start_ids_next_task[0] - 30)
    start_id_next_task_classified = Classify(
        id_plant_age, bins_start_ids_next_task, start_ids_next_task
    )
    next_task_match = (id_plant_age - start_id_next_task_classified) < 1
    start_id_next_task_validated = start_id_next_task_classified * next_task_match
    tasks_data["next_id"] = start_id_next_task_validated
    for col in list(calendar_tasks_next.columns)[:2]:
        calendar_tasks_next[col] = (
            calendar_tasks_next["id_days_start"].astype(str)
            + "_"
            + calendar_tasks_next[col].astype(str)
        )
        col_list = calendar_tasks_next[col].to_list()
        col_classify = Classify(id_plant_age, bins_start_ids_next_task, col_list, False)
        calendar_tasks_next_col = Where(col_classify, next_task_match, None)
        tasks_data[f"next_{col}"] = calendar_tasks_next_col
    return tasks_data


# ----------------------------------------------------------
def fertilizer_conditions(
    fertilizer_ids_dict, calendar_tasks_labels, identified_task_1
):
    """Fertilizer conditions binned. Check per NPK advice if it is valid (task fertilizer class Equal class).
    and sum the advices (if not valid, they become 0 and will be omitted)
    classes are 1-12, based on age, variety and (live) support"""

    fertilizer_df = calendar_tasks_labels[["task_id", "fertilizer_data_id"]]
    fertilizer_df = fertilizer_df.sort_values(by=["task_id"])  # sort by task_id
    fertilizer_tasks = fertilizer_df.values.tolist()

    f_bins, f_class_values = [0], []
    n = 0
    just_binned = False
    prev_task_id = 1
    # Defaults to True (the right side of the bin is closed so a value
    # is assigned to the bin on the left if it is exactly on a bin edge).
    for task_id, fertilizer_id in fertilizer_tasks:
        n += 0.0001
        if fertilizer_id > 0:
            if not just_binned:
                f_bins.append(prev_task_id)
                f_class_values.append(n)
                just_binned = True
                f_bins.append(task_id)
                f_class_values.append(fertilizer_id + n)
            else:
                f_bins.append(task_id)
                f_class_values.append(fertilizer_id + n)
                just_binned = True
        else:
            if just_binned:
                f_bins.append(task_id)
                f_class_values.append(n)
                just_binned = False
        prev_task_id = task_id

    # Calculate N P K advice
    fertilizer_task_id = Round(
        Classify(identified_task_1, f_bins, f_class_values, False)
    )
    n_advice = 0
    p_advice = 0
    k_advice = 0
    for c, npk in fertilizer_ids_dict.items():
        fertilizer_task_valid = fertilizer_task_id == c
        n, p, k = npk
        n_advice = eval(n) * fertilizer_task_valid + n_advice
        p_advice = eval(p) * fertilizer_task_valid + p_advice
        k_advice = eval(k) * fertilizer_task_valid + k_advice
    n_advice = Round(n_advice * 0.2) * 5  # round by 5 grams as adviced by IPB
    p_advice = Round(p_advice * 0.2) * 5
    k_advice = Round(k_advice * 0.2) * 5
    return n_advice, p_advice, k_advice


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
    # add arguments here
    # parser.add_argument(
    #     'path',
    #     metavar='FILE',
    # )
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
    logging.info("load and reclass calendar tasks")
    calendar_tasks_labels, calendar_tasks_next = get_calendar_tasks_labels(
        calendar_tasks
    )
    (
        months_ideal,
        days_months,
        days_months_1521,
        calendar_tasks_plant_months,
    ) = months_n_days(calendar_tasks)
    globals().update(months_ideal)
    logging.info(
        "load local raster, parcel and labelparameters data (mimics lizard data)"
    )
    dg_rasters, graph_rasters = mimic_rasters(lizard_rasters)
    globals().update(dg_rasters)
    sb_objects = raster_seriesblocks(dg_rasters, parcels)
    globals().update(sb_objects)
    globals().update(lp_seriesblocks)
    logging.info("determine actual, local plant and season conditions")
    actual_ages = actual_plant_age(
        days_since_epoch_raster_sb,
        days_plant_age_sb,
        days_months,
        days_months_1521,
        calendar_tasks_plant_months,
    )
    (
        days_until_jan_1_sb,
        plant_age_sb,
        calendar_tasks_plant_month_sb,
        id_plant_age,
        days_until_next_task,
        days_x_1000,
    ) = actual_ages
    conditions_season = season_conditions(
        days_until_jan_1_sb,
        doy_start_dry_season_raster_sb,
        doy_start_rainy_season_raster_sb,
    )
    season_states = season_state(
        calendar_tasks_plant_month_sb, calendar_tasks_plant_months, months_ideal
    )
    globals().update(season_states)
    season_below_0_ideal_100_above_200 = ideal_season_state(
        season_states, conditions_season
    )
    logging.info(
        "calculate task ids based on actual, local plant and season conditions"
    )
    task_id_parts = [
        live_support_sb,
        pepper_variety_sb,
        season_below_0_ideal_100_above_200,
        days_x_1000,
    ]
    t_identifiers = get_task_ids(task_id_parts)
    logging.info("get task content from calendar tasks df, aka tabel suci")
    task_dfs = tasks_t1_t2_t3(calendar_tasks_labels)
    tasks_data_tasks = task_contents(task_dfs, t_identifiers)
    logging.info("calculate next taks content too")
    tasks_data = next_task_contents(tasks_data_tasks, calendar_tasks_next, id_plant_age)
    globals().update(tasks_data)
    logging.info("calculate nutrient advices in the form of n, p and k grams per tree")
    n_advice, p_advice, k_advice = fertilizer_conditions(
        fertilizer_ids_dict, calendar_tasks_labels, t_identifiers[0]
    )
    logging.info("Set result table with parcels, labelparameters and additional labels")
    result_seriesblock = SetSeriesBlock(
        parcels_labeled,
        "label_value",
        "label",
        "----task_details_ID task (description_of_index)-----",
        "task_nr_1_id_days_start_2345_pepper_variety_6_live_support_7_id_season_state_8",
        "t1_task_id",
        t1_id_validated,
        "t1_task",
        task_1_task,
        "t1_task_IND",
        task_1_task_IND,
        "t1_recommendation",
        task_1_recommendation,
        "t1_recommendation_IND",
        task_1_recommendation_IND,
        "t1_GAP_info",
        task_1_GAP_info,
        "t1_GAP_info_IND",
        task_1_GAP_info_IND,
        "t1_GAP_chapter",
        task_1_GAP_chapter,
        "t1_image",
        task_1_image,
        "t1_image_url",
        task_1_image_url,
        "t1_input",
        (n_advice > 0) * 1,  # fertilizer advice yes 1 or no 0
        "t2_task_id",
        t2_id_validated,
        "t2_task",
        task_2_task,
        "t2_task_IND",
        task_2_task_IND,
        "t2_recommendation",
        task_2_recommendation,
        "t2_recommendation_IND",
        task_2_recommendation_IND,
        "t2_GAP_info",
        task_2_GAP_info,
        "t2_GAP_info_IND",
        task_2_GAP_info_IND,
        "t2_GAP_chapter",
        task_2_GAP_chapter,
        "t2_image",
        task_2_image,
        "t2_image_url",
        task_2_image_url,
        "t2_input",
        (n_advice > 0) * 2,  # TODO insert logic for manure input
        "t3_task_id",
        t3_id_validated,
        "t3_task",
        task_3_task,
        "t3_task_IND",
        task_3_task_IND,
        "t3_recommendation",
        task_3_recommendation,
        "t3_recommendation_IND",
        task_3_recommendation_IND,
        "t3_GAP_info",
        task_3_GAP_info,
        "t3_GAP_info_IND",
        task_3_GAP_info_IND,
        "t3_GAP_chapter",
        task_3_GAP_chapter,
        "t3_image",
        task_3_image,
        "t3_image_url",
        task_3_image_url,
        "t3_input",
        0,  # optional TODO, insert logic for other input
        "_XN_",
        n_advice,
        "_XP_",
        p_advice,
        "_XK_",
        k_advice,
        "next_task_id",
        next_id,
        "next_task",
        next_task,
        "next_task_IND",
        next_task_IND,
        "days_until_next_task",
        days_until_next_task,
    )

    logging.info("serialize model and replace local data with lizard data")
    dg_source = get_labeltype_source(result_seriesblock, graph_rasters, labeled_parcels)
    logging.info("update the labeltype model")
    response = patch_labeltype(dg_source, username, password, labeltype_uuid)
    logger.info("Labeltype update complete. Find response below")
    logger.info(response.status_code)
    return response.status_code
