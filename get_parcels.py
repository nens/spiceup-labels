# -*- coding: utf-8 -*-
"""
Created on Tue Feb 16 14:55:08 2021

@author: martijn.krol
"""

import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import localsecret
from shapely.geometry import Point

LOGIN = {
    "username": localsecret.username,
    "password": localsecret.password,
    "Content-Type": "application/json",
}

baseurl = "https://demo.lizard.net/api/v3/parcels/?in_bbox=103.8,-6.03,119.2,5.4"

r = requests.get(baseurl, headers=LOGIN)

count = r.json()["count"]

pages = int(np.ceil(count/100))

df = pd.DataFrame(columns=["id","geometry","properties","type"])

for p in range(1,pages+1):
    print("{}/{}".format(p,pages))
    url = "{}&page={}&page_size=100".format(baseurl,p)
    r = requests.get(url, headers=LOGIN)
    subdf = pd.DataFrame(r.json()["results"]["features"])
    df = df.append(subdf)

#%%
df.drop("type", axis=1,inplace=True)

df["id"] = df["id"].astype(int)
df.set_index("id", drop=False, inplace=True)

df["organisation"] = df["properties"].apply(lambda d: d["organisation"]["name"])
df["code"] = df["properties"].apply(lambda d: d["code"])
df["name"] = df["properties"].apply(lambda d: d["name"])
df["external_id"] = df["properties"].apply(lambda d: d["external_id"])
df = df[df["organisation"] == "G4AW SpiceUp"]
df.drop("properties", axis=1,inplace=True)

df["geometry"]=df["geometry"].apply(lambda x: Point(x["coordinates"][0][0]))
gdf = gpd.GeoDataFrame(df)
gdf.crs = {'init' : "EPSG:4326"}

gdf.to_file('./Shapes/parcels.shp')


