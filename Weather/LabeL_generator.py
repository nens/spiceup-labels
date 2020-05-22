# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 11:48:05 2020

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

with open('Labels_basis.json') as json_file:
    data = json.load(json_file)

source = data["source"]
graph = source["graph"]
result = graph["result"]
    
#%%

def create_lizardrastersource(code,uuid):
    key = code
    value = ["lizard_nxt.blocks.LizardRasterSource",uuid]
    return {key: value}

def create_aggregate(code):
    key = "{}_aggregate".format(code)
    method = "max" if code.endswith("summary") else "mean"  #Summary mag niet middelen dus pakt max
    value = ["geoblocks.geometry.aggregate.AggregateRaster","parcels",code,method,"epsg:4326",0.00001,None,"{}_label".format(code)]
    return {key:value}
        
def create_seriesblock(code):
    key = "{}_seriesblock".format(code)
    value = ["geoblocks.geometry.base.GetSeriesBlock","{}_aggregate".format(code),"{}_label".format(code)]
    return {key:value}

def create_shifts(code):
    dic = {}
    for day in range(1,8):
        key = "{}_shift_{}".format(code,day)
        value = ["geoblocks.raster.temporal.Shift",code,day*-86400000]
        dic.update({key:value})
    return dic
    
def update_result(code,label,result):
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
    
    shifts = create_shifts(code)
    graph.update(shifts)
    
    for key in shifts:
        aggregate = create_aggregate(key)
        graph.update(aggregate)
        seriesblock = create_seriesblock(key)
        graph.update(seriesblock)
        shiftlabel = "{}_t{}".format(code,key.split("_")[-1])
        result = update_result(key,shiftlabel,result)

graph["result"]=result  
source["graph"]=graph
data["source"]=source

with open("Label_result.json","w+") as outfile:
    json.dump(data, outfile)
