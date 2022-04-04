# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 10:28:36 2020
@author: martijn.krol
"""

import numpy as np
import json
import argparse
import logging
from localsecret import username, password
from growth_health_tasks_config import (
    labeltype_uuid,  # the uuid of the model
    data,
    growth_info,
    health_info,
    periods,
    bounds,
    names,
    growth_codes,
    health,
    health_codes,
)

from spiceup_labels.config_lizard import patch_labeltype


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
    logging.info("load and reclass growth_health_tasks")

    source = data["source"]
    graph = source["graph"]

    if not (all(bounds.index == periods.index)):
        raise Exception("Indices between periods and bounds sheets do not match")

    if not (all(names.index == periods.index)):
        raise Exception("Indices between namessheet and other two sheets do not match")

    if not len(bounds.columns) == len(periods.columns):
        raise Exception("Bounds and periods sheets have a different amount of columns")

    # Add labelparameters
    labelparam_counter = 2

    labelparams = list(periods.index) + list(health.index)

    for lp in labelparams:
        labelparam_counter += 1
        if lp == labelparams[-1]:
            key = "parcels.all.labelparams"
        else:
            key = "parcels.add.labelparams.{}".format(labelparam_counter)

        query = {
            "label_type__uuid": "495706f7-0f59-4eaf-a4d8-bf65946b7c62",
            "name": lp.replace(".","_"),
            "end": None,
        }
        matchdic = {"object_id": "object_id"}
        coldic = {"value": lp.replace(".","_")}
        l = [
            "geoblocks.geometry.sources.AddDjangoFields",
            "parcels.add.labelparams.{}".format(labelparam_counter - 1),
            "lizard_nxt",
            "labelparameter",
            query,
            matchdic,
            coldic,
            "start",
            "end",
        ]

        graph[key] = l

    # Create seriesblocks
    for lp in labelparams:
        key = "{}.seriesblock".format(lp)
        l = ["geoblocks.geometry.base.GetSeriesBlock", "parcels.all.labelparams", lp.replace(".","_")]
        graph[key] = l

    # Specific for plant growth

    # Create classification columns
    for index, row in periods.iterrows():
        period_list = [month for month in row if ~np.isnan(month)]

        bounds_row = bounds.loc[index]
        lower_bounds = [
            month - 0.1
            for month in bounds_row.loc[(bounds_row.index.str.endswith("lower"))]
            if ~np.isnan(month)
        ]
        upper_bounds = [
            month
            for month in bounds_row.loc[(bounds_row.index.str.endswith("upper"))]
            if ~np.isnan(month)
        ]

        outside_lower = -1
        outside_upper = 101

        lower_bounds_complete = [outside_lower]
        upper_bounds_complete = [outside_upper]

        for bound in lower_bounds:
            outside_lower -= 1
            lower_bounds_complete.append(bound)
            lower_bounds_complete.append(outside_lower)

        for bound in upper_bounds:
            outside_upper += 1
            upper_bounds_complete.append(bound)
            upper_bounds_complete.append(outside_upper)

        lower_key = "{}.lower".format(index)
        upper_key = "{}.upper".format(index)

        lower_block = [
            "geoblocks.geometry.field_operations.Classify",
            "plant.age.months",
            period_list,
            lower_bounds_complete,
            True,
        ]

        upper_block = [
            "geoblocks.geometry.field_operations.Classify",
            "plant.age.months",
            period_list,
            upper_bounds_complete,
            True,
        ]

        graph[lower_key] = lower_block
        graph[upper_key] = upper_block

    l = [
        "geoblocks.geometry.field_operations.Classify",
        "plant.age.months",
        period_list,
        [0, 1000, 1, 2000, 2, 3000, 3],
        True,
    ]

    graph["periodscore.withlow"] = l

    l = ["geoblocks.geometry.field_operations.Less", "periodscore.withlow", 50]

    graph["periodscore.lownumbers"] = l

    l = [
        "geoblocks.geometry.field_operations.Mask",
        "periodscore.withlow",
        "periodscore.lownumbers",
        0,
    ]

    graph["periodscore"] = l

    # Create classification Geometryblock, with all classification columns
    key = "classificationblock"
    l = ["geoblocks.geometry.base.SetSeriesBlock", "parcels.all.labelparams"]
    for index, row in periods.iterrows():
        l.append("{}.lower.column".format(index))
        l.append("{}.lower".format(index))
        l.append("{}.upper.column".format(index))
        l.append("{}.upper".format(index))

    graph[key] = l

    # Classify parameters
    for index, row in periods.iterrows():
        key = "{}.classified".format(index)
        key_round = "{}.round".format(index)
        l = [
            "geoblocks.geometry.field_operations.ClassifyFromColumns",
            "classificationblock",
            index.replace(".","_"),
            ["{}.lower.column".format(index), "{}.upper.column".format(index)],
            [300, 100, 200],
            True,
        ]

        l_round = [
            "geoblocks.geometry.field_operations.Round",
            "{}.classified".format(index),
        ]
        graph[key] = l
        graph[key_round] = l_round

    for index, row in periods.iterrows():
        key = "{}.score".format(index)
        l = [
            "geoblocks.geometry.field_operations.Add",
            "{}.round".format(index),
            "periodscore",
        ]

        graph[key] = l

    for index, row in periods.iterrows():
        growth_parameter_codes = growth_codes[growth_codes["parameter"] == index]

        key = "{}.taskid.decimals".format(index)
        bins = list(growth_parameter_codes["Score"])
        labels = [0] + list(
            growth_parameter_codes["Task_id"]
        )  # Prepend zero for outliers

        l = [
            "geoblocks.geometry.field_operations.Classify",
            "{}.score".format(index),
            bins,
            labels,
            False,
        ]
        graph[key] = l

        key_round = "{}.taskid.block".format(index)
        l_round = [
            "geoblocks.geometry.field_operations.Round",
            "{}.taskid.decimals".format(index),
        ]

        graph[key_round] = l_round

    growth_return_columns = [
        "condition",
        "task",
        "task_IND",
        "recommendation",
        "recommendation_IND",
        "GAP_information",
        "GAP_information_IND",
        "GAP_chapter",
        "image",
        "image_url",
    ]

    for index, row in periods.iterrows():
        for return_column in growth_return_columns:
            key = "{}.{}.block".format(index.replace("_","."), return_column)
            bins = list(growth_info.index)
            growth_info["tempcol"] = (
                growth_info["strindex"] + "_" + growth_info[return_column]
            )
            labels = list(growth_info["tempcol"]) + ["Unknown"]
            l = [
                "dask_geomodeling.geometry.field_operations.Classify",
                "{}.taskid.block".format(index),
                bins,
                labels,
                True,
            ]
            graph[key] = l

    # Plant health specific
    # Multiply all conditions with a factor
    factors = [1000, 100, 10, 1]
    conditions = [
        "stem.appearance",
        "leaves.color",
        "vigor.appearance",
        "berry.appearance"
    ]
    
    for factor, cond in zip(factors, conditions):
    
        key = "{}.multiplied".format(cond)
        l = [
            "geoblocks.geometry.field_operations.Multiply",
            "{}.seriesblock".format(cond),
            factor,
        ]
        graph[key] = l

    # Sum all conditions
    for i in range(len(conditions) - 1):
        if i < len(conditions) - 2:
            key = "cond.add.{}".format(i + 1)
        elif i == len(conditions) - 2:
            key = "conditions.sum"

        if i == 0:
            sum_base = "{}.multiplied".format(conditions[i])
        else:
            sum_base = "cond.add.{}".format(i)

        l = [
            "geoblocks.geometry.field_operations.Add",
            sum_base,
            "{}.multiplied".format(conditions[i + 1]),
        ]

        graph[key] = l

    # Classify to task_ids
    key = "health.taskid.block"
    bins = list(health_codes["Code"])[
        1:
    ]  # Drop first value as everything lower than 1102 gets label healthy
    labels = list(health_codes["Task_id"])

    l = [
        "dask_geomodeling.geometry.field_operations.Classify",
        "conditions.sum",
        bins,
        labels,
        False,
    ]

    graph[key] = l

    health_return_columns = [
        "task",
        "task_IND",
        "recommendation",
        "recommendation_IND",
        "GAP_information",
        "GAP_information_IND",
        "GAP_chapter",
        "image",
        "image_url",
    ]

    for return_column in health_return_columns:
        key = "health.{}.block".format(return_column.replace("_","."))
        bins = list(health_info.index)
        health_info["tempcol"] = (
            health_info["strindex"] + "_" + health_info[return_column]
        )
        labels = list(health_info["tempcol"]) + ["Unknown"]
        l = [
            "dask_geomodeling.geometry.field_operations.Classify",
            "health.taskid.block",
            bins,
            labels,
            True,
        ]
        graph[key] = l

    # result block
    result_block = [
        "geoblocks.geometry.base.SetSeriesBlock",
        "parcels.all.labelparams",
        "label_value",
        "label",
        "plant age (months)",
        "plant.age.months",
        "----task_plant_growth_params-----",
        "-",
    ]

    for index, row in names.iterrows():
        result_block.append(index.replace(".","_") + "_task_id")
        result_block.append("{}.taskid.block".format(index))
        for return_column in growth_return_columns:
            result_block.append("{}_{}".format(index.replace(".","_"), return_column))
            result_block.append("{}.{}.block".format(index.replace("_","."), return_column))

    result_block.append("----task_plant_health_params-----")
    result_block.append("-")
    result_block.append("health_task_id")
    result_block.append("health.taskid.block")
    

    for return_column in health_return_columns:
        result_block.append("health_{}".format(return_column))
        result_block.append("health.{}.block".format(return_column.replace("_",".")))



    # # add graph to source
    graph["result"] = result_block
    source["graph"] = graph
    
    
    with open("growth_health_tasks.json", "w+") as f:
        json.dump(source ,f)

    r = patch_labeltype(source, username, password, labeltype_uuid)
    logging.info(r.json())


if __name__ == "__main__":
    main()
