# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 11:08:35 2020

@author: martijn.krol
"""

import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

if not 'weather_info' in locals():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)
    sh = client.open("Items & Properties on the App Ui/x")
    ws = sh.worksheet("Weather")
    weather_info = pd.DataFrame(ws.get_all_records())
    weather_info = weather_info[weather_info["parameter"]!="Location"]

with open("Labels_basis.json") as json_file:
    data = json.load(json_file)

source = data["source"]
graph = source["graph"]
result = graph["result"]
#%%


def create_lizardrastersource(code, uuid):
    key = code
    value = ["lizard_nxt.blocks.LizardRasterSource", uuid]
    return {key: value}


def create_aggregate(code):
    key = "{}_aggregate".format(code)
    method = "max" if code.startswith("icon") else "mean"  #Icon mag niet middelen dus pakt max
    value = ["geoblocks.geometry.aggregate.AggregateRaster","parcels",code,method,"epsg:4326",0.00001,None,"{}_label".format(code)]
    return {key:value}
        

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

for index, row in weather_info.iterrows():
    code = row["parameter"]
    code = code.replace(" ","_").replace("(","").replace(")","").lower()
    uuid = row["Raster UUID"]
    rastersource = create_lizardrastersource(code,uuid)
    graph.update(rastersource)
    aggregate = create_aggregate(code)
    graph.update(aggregate)
    seriesblock = create_seriesblock(code)
    graph.update(seriesblock)
    result = update_result(code,"{}_t0".format(code),result)


graph["result"] = result
source["graph"] = graph
data["source"] = source

with open("Label_result_startup.json", "w+") as outfile:
    json.dump(data, outfile)
