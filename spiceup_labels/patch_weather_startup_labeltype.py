# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 11:08:35 2020

@author: martijn.krol
"""

import json
import pandas as pd
import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from localsecret import username, password
from spiceup_labels.config_lizard import patch_labeltype, configure_logger

#%%


def create_lizardrastersource(code, uuid):
    key = code
    value = ["lizard_nxt.blocks.LizardRasterSource", uuid]
    return {key: value}


def create_aggregate(code):
    key = "{}_aggregate".format(code)
    method = (
        "max" if (code.startswith("icon") or code.startswith("soil_mois")) else "mean"
    )  # Icon mag niet middelen dus pakt max
    value = [
        "geoblocks.geometry.aggregate.AggregateRaster",
        "parcels",
        code,
        method,
        "epsg:4326",
        0.00001,
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
def main():
        
    labeltype_uuid = "8ef4c780-6995-4935-8bd3-73440a689fc3"
    
    configure_logger(logging.DEBUG)
    logger = logging.getLogger("labellogger")
    
    logger.info("Start creation of weather startup labeltype")
    logger.info("Reading data from Google spreadsheet")
    
    if not "weather_info" in locals():
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "client_secret.json", scope
        )
        client = gspread.authorize(creds)
        sh = client.open("Items & Properties on the App Ui/x")
        ws = sh.worksheet("Weather")
        weather_info = pd.DataFrame(ws.get_all_records())
        weather_info = weather_info[weather_info["parameter"] != "Location"]
    
    with open("Weatherconfig\Labels_basis.json") as json_file:
        data = json.load(json_file)
    
    source = data["source"]
    graph = source["graph"]
    result = graph["result"]

    logger.info("Data read succefully")
    
    logger.info("Building labeltype")
    
    for index, row in weather_info.iterrows():
        code = row["parameter"]
        code = code.replace(" ", "_").replace("(", "").replace(")", "").lower()
        uuid = row["Raster UUID"]
        rastersource = create_lizardrastersource(code, uuid)
        graph.update(rastersource)
        aggregate = create_aggregate(code)
        graph.update(aggregate)
        seriesblock = create_seriesblock(code)
        graph.update(seriesblock)
        result = update_result(code, "{}_t0".format(code), result)
    
    #Config for Soil Moisture traffic light
    code = "soil_moisture"
    rastersource = create_lizardrastersource(code, "04802788-be81-4d10-a7f3-81fcb66f3a81")
    graph.update(rastersource)
    aggregate = create_aggregate(code)
    graph.update(aggregate)
    seriesblock = create_seriesblock(code)
    graph.update(seriesblock)
    result = update_result(code, "soil_moisture_condition", result)
    
    graph["result"] = result
    source["graph"] = graph
    data["source"] = source
    
    with open("Label_result_startup.json", "w+") as outfile:
        json.dump(data, outfile)
        
    logger.info("Patching Lizard weather labeltype")
    r = patch_labeltype(source, username, password, labeltype_uuid)
    logger.debug(r.json())
    r.raise_for_status()
    logger.info("Complete!")
