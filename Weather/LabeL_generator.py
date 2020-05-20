# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 11:48:05 2020

@author: martijn.krol
"""

import json
import pandas as pd

with open("Labels_basis.json") as json_file:
    data = json.load(json_file)

source = data["source"]
graph = source["graph"]
result = graph["result"]

params = pd.read_csv("params.csv")

#%%


def create_lizardrastersource(code, uuid):
    key = code
    value = ["lizard_nxt.blocks.LizardRasterSource", uuid]
    return {key: value}


def create_aggregate(code):
    key = "{}_aggregate".format(code)
    method = (
        "max" if code.startswith("icon") else "mean"
    )  # Icon mag niet middelen dus pakt max
    value = [
        "geoblocks.geometry.aggregate.AggregateRaster",
        "boundaries",
        code,
        method,
        "epsg:3857",
        100,
        None,
        "{}_label".format(code),
    ]
    return {key: value}


def create_seriesblock(code):
    key = "{}_seriesblock".format(code)
    value = [
        "geoblocks.geometry.base.GetSeriesBlock",
        "{}_aggregate".format(code),
        "{}_label".format(code),
    ]
    return {key: value}


def create_shifts(code):
    dic = {}
    for day in range(1, 8):
        key = "{}_shift_{}".format(code, day)
        value = ["geoblocks.raster.temporal.Shift", code, day * -86400000]
        dic.update({key: value})
    return dic


def update_result(code, label, result):
    result.append(label)
    result.append("{}_seriesblock".format(code))
    return result


#%%

for index, row in params.iterrows():
    code = row["code"]
    uuid = row["uuid"]
    label = row["label"]
    rastersource = create_lizardrastersource(code, uuid)
    graph.update(rastersource)
    aggregate = create_aggregate(code)
    graph.update(aggregate)
    seriesblock = create_seriesblock(code)
    graph.update(seriesblock)
    result = update_result(code, "{}_t0".format(label), result)

    shifts = create_shifts(code)
    graph.update(shifts)

    for key in shifts:
        aggregate = create_aggregate(key)
        graph.update(aggregate)
        seriesblock = create_seriesblock(key)
        graph.update(seriesblock)
        shiftlabel = "{}_t{}".format(label, key.split("_")[-1])
        result = update_result(key, shiftlabel, result)

graph["result"] = result
source["graph"] = graph
data["source"] = source

with open("Label_result.json", "w+") as outfile:
    json.dump(data, outfile)
