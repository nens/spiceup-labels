# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 11:08:35 2020

@author: martijn.krol
"""

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

params = pd.read_csv("params_startup.csv")

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

graph["result"] = result
source["graph"] = graph
data["source"] = source

with open("Label_result_startup.json", "w+") as outfile:
    json.dump(data, outfile)
