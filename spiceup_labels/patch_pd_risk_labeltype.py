# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 11:48:05 2020

@author: martijn.krol
"""

import json
import logging
import pandas as pd
from localsecret import username, password
from spiceup_labels.config_lizard import patch_labeltype, configure_logger

#%%

def create_labelparam(name, labelparam_uuid, prev, number):
    key = f"labelparams.{number}"
    value = [
            "django_geoblocks.blocks.sources.AddDjangoFields",
            prev,
            "lizard_nxt",
            "labelparameter",
            {
                "name": name,
                "label_type__uuid": labelparam_uuid
            },
            {
                "object_id": "object_id"
            },
            {
                "value": "{}_reports".format(name)
            },
            "start",
            "end"
        ]
    
    return {key: value}
    
    

def create_lizardrastersource(code, uuid):
    key = code
    value = ["lizard_nxt.blocks.LizardRasterSource", uuid]
    return {key: value}


def create_aggregate(code):
    key = "{}.aggregate".format(code)
    method = "max"
    value = [
        "geoblocks.geometry.aggregate.AggregateRaster",
        "parcels",
        code,
        method,
        "epsg:4326",
        0.00001,
        None,
        "{}.label".format(code),
    ]
    return {key: value}


def create_seriesblock(code):
    key = "{}.seriesblock".format(code)
    value = [
        "geoblocks.geometry.base.GetSeriesBlock",
        "{}.aggregate".format(code),
        "{}.label".format(code),
    ]
    return {key: value}

def create_seriesblock_lp(code, sourceblock):
    key = "{}.report.seriesblock".format(code)
    value = [
        "geoblocks.geometry.base.GetSeriesBlock",
        sourceblock,
        "{}_reports".format(code.replace(".","_")),
    ]    
    return {key: value}

def create_less(code):
    key = "{}.less".format(code)
    value = [
        "dask_geomodeling.geometry.field_operations.Less",
        "{}.report.seriesblock".format(code),
        10
        ]
    return {key: value}

def create_invert(code):
    key = "{}.invert".format(code)
    value = [
        "dask_geomodeling.geometry.field_operations.Invert",
        "{}.less".format(code)
        ]
    return {key: value}

def create_mask(code):
    key = "{}.mask".format(code)
    value = [
        "dask_geomodeling.geometry.field_operations.Mask",
        "{}.report.seriesblock".format(code),
        "{}.invert".format(code),
        0
        ]
    return {key: value}

def add_sum(i, current, prev):
    key = "sum.{}".format(i)
    value = [
        "dask_geomodeling.geometry.field_operations.Add",
        current,
        prev
    ]
    return {key: value}


def add_sum_risk(i, current, prev):
    key = "sum.risk.{}".format(i)
    value = [
        "dask_geomodeling.geometry.field_operations.Add",
        current,
        prev
    ]
    return {key: value}

def update_result(code, label, result):
    result.append("{}".format(label))
    result.append("{}.seriesblock".format(code))
    return result


#%%
def main():
    
    labeltype_uuid = "2586be26-803a-4f61-9d0f-0b4afe10c5d6"
    
    labelparam_uuid = "1c776839-4be5-4257-9403-e9684b058ce4"

    rasters = {
        "foot_rot_disease": "771e7195-c748-40b3-81fa-ee3212fcbdbc",
        "yellow_disease": "2f8b18b9-9345-4f00-8651-b15b7c851b7e",
        "viral_disease": "01224a5f-dad6-4c24-b035-a321f199c5bd",
        "pepper_bug": "2b8820e6-ebfc-48a9-befc-1252a4e809c3",
        "tingid_bug": "997933d9-09c0-47a5-95f0-7abfdf9be6c1",
        "velvet_blight": "6d499a81-2f58-4859-bf6c-3c063b7736fd",
        "stem_borer": "23b83a27-def5-4952-bea1-a6d82af93ef5",
    }


    configure_logger(logging.DEBUG)
    logger = logging.getLogger("labellogger")
    
    logger.info("Start creation of P&D risk labeltype")
    
    with open("PDconfig\Labels_basis.json") as json_file:
        data = json.load(json_file)
    
    source = data["source"]
    graph = source["graph"]
    result = graph["result"]
    
    logger.info("Data read succefully")
    
    logger.info("Building labeltype")
    
    
    
    logger.info("Adding labelparams")
    for i, rast in enumerate(rasters.keys()):
        if i == 0:
            prev = "parcels"
        else:
            prev = f"labelparams.{i}"
            
        labelparam = create_labelparam(rast, labelparam_uuid, prev, i+1)
        graph.update(labelparam)
    
    sourceblock = next(iter(labelparam.keys()))
    logger.info(f"Building rest of block with sourceblock {sourceblock}")
    
    logger.info("adding aggregate")
        
    for i, key in enumerate(rasters.keys()):
        
        code = key.replace("_",".")
        sb = create_seriesblock_lp(code, sourceblock)
        graph.update(sb)
        
        less = create_less(code)
        graph.update(less)
        invert = create_invert(code)
        graph.update(invert)
        mask = create_mask(code)
        graph.update(mask)
        
        if i!=0:
            current = next(iter(mask.keys()))
            sumblock = add_sum(i, current, prev)
            graph.update(sumblock)
            prev = next(iter(sumblock.keys()))    
        else: 
            prev = next(iter(mask.keys()))
    
    classify = {"overall.pd.report": [
        "dask_geomodeling.geometry.field_operations.Classify",
        prev,
        [0.5],
        [0,1]
        ]}
    
    graph.update(classify)
    
    result = [r if r != "parcels" else sourceblock for r in result]

    result.append("overall_pd_report")
    result.append("overall.pd.report")
    
    
    for key, value in rasters.items():
        code = key.replace("_",".")
        raster = create_lizardrastersource(code, value)
        graph.update(raster)
        
        agg = create_aggregate(code)
        graph.update(agg)
        
        sb = create_seriesblock(code)
        graph.update(sb)
        
        result = update_result(code, f"{key}_risk", result)
    
    for i, key in enumerate(rasters.keys()):
        code = key.replace("_",".")
        
        if i==0:
            prev = "{}.seriesblock".format(code)
        else:
            current = "{}.seriesblock".format(code)
            sumblock = add_sum_risk(i, current, prev)
            graph.update(sumblock)
            prev = next(iter(sumblock.keys()))        

    classify = {"overall.pd.risk": [
        "dask_geomodeling.geometry.field_operations.Classify",
        prev,
        [0.5],
        [0,1]
        ]}
    
    graph.update(classify)
    
    result.append("overall_pd_risk")
    result.append("overall.pd.risk")
    
    graph["result"] = result
    source["graph"] = graph
    data["source"] = source
    
    with open("PD_Label_result.json", "w+") as outfile:
        json.dump(data, outfile)
    logger.info("Patching Lizard P&D labeltype")
    r = patch_labeltype(source, username, password, labeltype_uuid)
    logger.debug(r.json())
    r.raise_for_status()
    logger.info("Complete!")