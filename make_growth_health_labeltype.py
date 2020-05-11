# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 10:28:36 2020

@author: martijn.krol
"""

import pandas as pd
import json
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials

#%% Algemeen

if not 'growth_info' in locals():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)
    sh = client.open("Items & Properties on the App Ui/x")
    ws = sh.worksheet("Plant Growth")
    growth_info = pd.DataFrame(columns=ws.row_values(1))
    rowcount = len(ws.col_values(1))-1 #First row is header
    for i in range(len(growth_info.columns)):
        colname = growth_info.columns[i]
        col = ws.col_values(i+1)[1:]
        dif = rowcount - len(col)
        if dif>0:
            for i in range(dif):
                col.append(np.nan)
        growth_info[colname] = col
    
    growth_info["task_id"] = growth_info["task_id"].replace("N/A","0")
    growth_info["strindex"] = growth_info["task_id"]
    growth_info = growth_info.replace("",np.nan)
    growth_info["task_id"] = pd.to_numeric(growth_info["task_id"])
    growth_info.set_index("task_id",inplace=True)
    growth_info = growth_info.loc[~growth_info.index.duplicated(keep='first')] #Delere duplicates
    growth_info = growth_info.replace(np.nan,"")

if not 'health_info' in locals():
    ws = sh.worksheet("Plant Health")
    health_info = pd.DataFrame(columns=ws.row_values(1))
    rowcount = len(ws.col_values(1))-1 #First row is header
    for i in range(len(health_info.columns)):
        colname = health_info.columns[i]
        col = ws.col_values(i+1)[1:]
        dif = rowcount - len(col)
        if dif>0:
            for i in range(dif):
                col.append(np.nan)
        health_info[colname] = col
        
    health_info["task_id"] = health_info["task_id"].replace("WiP","0")
    health_info["strindex"] = health_info["task_id"]
    health_info = health_info.replace("",np.nan)
    health_info["task_id"] = pd.to_numeric(health_info["task_id"])
    health_info.set_index("task_id",inplace=True)
    health_info = health_info.replace(np.nan,"")    
    health_info = health_info.loc[~(health_info.index==0)]        

periods = pd.read_excel("growth_params.xlsx", sheet_name="periods", index_col="parameter")
bounds = pd.read_excel("growth_params.xlsx", sheet_name="bounds", index_col="parameter")
names = pd.read_excel("growth_params.xlsx", sheet_name="names", index_col="parameter")
growthcodes = pd.read_excel("growth_params.xlsx", sheet_name="codes")

health = pd.read_excel("health_params.xlsx",sheet_name="numbers",index_col="param")
healthcodes = pd.read_excel("health_params.xlsx",sheet_name="healthcodes")

with open("growth_health_base.json") as json_file:
    data = json.load(json_file)
    
source = data["source"]
graph = source["graph"]

if not(all(bounds.index == periods.index)):
    raise Exception("Indices between periods and bounds sheets do not match")
    
if not(all(names.index == periods.index)):
    raise Exception("Indices between namessheet and other two sheets do not match")

if not len(bounds.columns) == len(periods.columns):
    raise Exception("Bounds and periods sheets have a different amount of columns")

#Voeg labelparameters toe
labelparamcounter = 2

labelparams = list(periods.index)+list(health.index)

for lp in labelparams:
    labelparamcounter += 1
    if lp == labelparams[-1]:
        key = "parcels_all_labelparams"
    else:
        key = "parcels_add_labelparams_{}".format(labelparamcounter)
    
    query = {
        "label_type__uuid": "495706f7-0f59-4eaf-a4d8-bf65946b7c62",
        "name": lp,
        "end": None
        }
    matchdic = {
        "object_id": "object_id"
        }
    coldic = {
        "value": lp    
        }
    l = ["geoblocks.geometry.sources.AddDjangoFields",
         "parcels_add_labelparams_{}".format(labelparamcounter-1),
         "lizard_nxt",
         "labelparameter",
         query,
         matchdic,
         coldic,
         "start",
         "end"
         ]
    
    graph[key] = l
    
#Maak seriesblocks aan
for lp in labelparams:
    key = "{}_seriesblock".format(lp)
    l = ["geoblocks.geometry.base.GetSeriesBlock",
         "parcels_all_labelparams",
         lp
        ]
    graph[key] = l
    
#%% Specifiek voor plant growth 

#Maak de classificatiekolommen aan
for index, row in periods.iterrows(): 
    periodlist = [month for month in row if ~np.isnan(month)]
    
    boundsrow = bounds.loc[index]
    lowerbounds = [month-0.1 for month in boundsrow.loc[(boundsrow.index.str.endswith("lower"))] if ~np.isnan(month)]
    upperbounds = [month for month in boundsrow.loc[(boundsrow.index.str.endswith("upper"))] if ~np.isnan(month)]
    
    outside_lower = -1
    outside_upper = 101
    
    lowerbounds_complete = [outside_lower]
    upperbounds_complete = [outside_upper]
    
    for bound in lowerbounds:
        outside_lower -= 1
        lowerbounds_complete.append(bound)
        lowerbounds_complete.append(outside_lower)
        
    for bound in upperbounds:
        outside_upper += 1
        upperbounds_complete.append(bound)
        upperbounds_complete.append(outside_upper)
    
    lowerkey = "{}_lower".format(index)
    upperkey = "{}_upper".format(index)
    
    
    lowerblock = [
            "geoblocks.geometry.field_operations.Classify",
            "plant_age_months",
            periodlist,
            lowerbounds_complete
            ]
    
    upperblock = [
            "geoblocks.geometry.field_operations.Classify",
            "plant_age_months",
            periodlist,
            upperbounds_complete
            ]
    
    graph[lowerkey] = lowerblock
    graph[upperkey] = upperblock

l = [ "geoblocks.geometry.field_operations.Classify",
       "plant_age_months",
       periodlist,
       [0,1000,1,2000,2,3000,3]
    ]

graph["periodscore_withlow"] = l

l = ["geoblocks.geometry.field_operations.Less",
     "periodscore_withlow",
     50
     ]

graph["periodscore_lownumbers"] = l

l = ["geoblocks.geometry.field_operations.Mask",
     "periodscore_withlow",
     "periodscore_lownumbers",
     0
     ]

graph["periodscore"] = l


#Maak het classification Geometryblock met alle classificatiekolommen
key = "classificationblock"
l = ["geoblocks.geometry.base.SetSeriesBlock","parcels_all_labelparams"]
for index, row in periods.iterrows(): 
    l.append("{}_lower_column".format(index))
    l.append("{}_lower".format(index))
    l.append("{}_upper_column".format(index))
    l.append("{}_upper".format(index))
    
graph[key] = l

#Voer de classificatie uit voor alle parameters
for index, row in periods.iterrows():
    key = "{}_classified".format(index)
    key_round = "{}_round".format(index)
    l = ["geoblocks.geometry.field_operations.ClassifyFromColumns",
         "classificationblock",
         index,
         ["{}_lower_column".format(index),"{}_upper_column".format(index)],
         [300,100,200]
         ]
    
    l_round = ["geoblocks.geometry.field_operations.Round",
               "{}_classified".format(index)]
    graph[key] = l
    graph[key_round] = l_round
    
for index, row in periods.iterrows():
    key = "{}_score".format(index)
    l = ["geoblocks.geometry.field_operations.Add",
         "{}_round".format(index),
         "periodscore"]
    
    graph[key] = l

for index, row in periods.iterrows():
    paramcodes = growthcodes[growthcodes["parameter"]==index]
    
    key = "{}_taskid_decimals".format(index)
    bins = list(paramcodes["Score"])
    labels = [0]+list(paramcodes["Task_id"]) #Prepend zero for outliers
    
    l = ["geoblocks.geometry.field_operations.Classify",
     "{}_score".format(index),
     bins,
     labels,
     False
     ]
    graph[key] = l    
    
    key_round = "{}_taskid_block".format(index)
    l_round = ["geoblocks.geometry.field_operations.Round",
               "{}_taskid_decimals".format(index)]
    
    graph[key_round] = l_round
    
growthreturncolumns = ["condition","task","task_IND","recommendation","recommendation_IND","GAP_information","GAP_information_IND","GAP_chapter","image","image_url"]

for index, row in periods.iterrows():
    for returncolumn in growthreturncolumns:
        key = "{}_{}_block".format(index,returncolumn)
        bins = list(growth_info.index)
        growth_info["tempcol"]=growth_info["strindex"]+"_"+growth_info[returncolumn]
        labels = list(growth_info["tempcol"])+["Unknown"]
        l = ["dask_geomodeling.geometry.field_operations.Classify",
         "{}_taskid_block".format(index),
         bins,
         labels,
         True
         ]
        graph[key] = l

    
#%% Specifiek voor plant health
#Vermenigvuldig alle conditites met een factor
factor = [1000, 100, 0, 1]
conditions = ["stem_appearance","leaves_color","hanging_rigidly","vigor_appearance"]
counter = 0
for cond in conditions:
    key = "{}_multiplied".format(cond)
    l = ["geoblocks.geometry.field_operations.Multiply",
         "{}_seriesblock".format(cond),
         factor[counter]
         ]
    counter +=1
    graph[key] = l
    
# Tel nu alle factoren bij elkaar op
for i in range(len(conditions)-1):
    if i < len(conditions)-2:
        key = "cond_add_{}".format(i+1)
    elif i==len(conditions)-2:
        key = "conditions_sum"
    
    if i==0:
        sumbase = "{}_multiplied".format(conditions[i])
    else:
        sumbase = "cond_add_{}".format(i)
        
    l = ["geoblocks.geometry.field_operations.Add",
         sumbase,
         "{}_multiplied".format(conditions[i+1])
        ]
            
    graph[key]=l

#Classify naar task_ids
key = "health_taskid_block"
bins = list(healthcodes["Code"])[1:] #Drop first value as everything lower than 1102 gets label healthy
labels = list(healthcodes["Task_id"])

l = ["dask_geomodeling.geometry.field_operations.Classify",
     "conditions_sum",
     bins,
     labels,
     False
     ]
    
graph[key] = l

healthreturncolumns = ["task","task_IND","recommendation","recommendation_IND","GAP_information","GAP_information_IND","GAP_chapter","image","image_url"]

for returncolumn in healthreturncolumns:
    key = "health_{}_block".format(returncolumn)
    bins = list(health_info.index)
    health_info["tempcol"]=health_info["strindex"]+"_"+health_info[returncolumn]
    labels = list(health_info["tempcol"])+["Unknown"]
    l = ["dask_geomodeling.geometry.field_operations.Classify",
     "health_taskid_block",
     bins,
     labels,
     True
     ]
    graph[key] = l
#%% Afronding
resultblock = [
        "geoblocks.geometry.base.SetSeriesBlock",
        "parcels_all_labelparams",
        "label_value",
        "label",
    	"plant age (months)",
    	"plant_age_months",
        "----task_plant_growth_params-----",
        "-",
        ]
    
for index, row in names.iterrows():
    resultblock.append(index+"_task_id")
    resultblock.append("{}_taskid_block".format(index))
    for returncolumn in growthreturncolumns:
        resultblock.append("{}_{}".format(index,returncolumn))
        resultblock.append("{}_{}_block".format(index,returncolumn))
 
resultblock.append("----task_plant_health_params-----")
resultblock.append("-")
resultblock.append("health_task_id")
resultblock.append("health_taskid_block")

for returncolumn in healthreturncolumns:
    resultblock.append("health_{}".format(returncolumn))
    resultblock.append("health_{}_block".format(returncolumn))

graph["result"] = resultblock

#stop graph in output en schrijf weg
source["graph"]=graph
data["source"]=source

with open("growth_health_output.json","w+") as outfile:
    json.dump(data, outfile)
