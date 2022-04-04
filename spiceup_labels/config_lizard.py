import requests
import simplejson
import logging
from dask_geomodeling.raster import *
from dask_geomodeling.geometry import *
from dask_geomodeling.geometry.aggregate import AggregateRaster
from dask_geomodeling.geometry.base import GetSeriesBlock, SetSeriesBlock

# ----------------------------------------------------------
def mimic_rasters(lizard_rasters):
    """Mimic lizard rasters locally with rasterized wkt polygons"""
    dg_rasters = {}
    wkt = "POLYGON((1 2, 2 3, 2 3, 1 2))"
    for counter, (raster, uuid) in enumerate(lizard_rasters.items(), 1):
        # Use RasterizeWKT locally, replace it later with LizardRasterSource
        # To get unique dummy sources use wkt.replace("1", "{RASTERNUMBER}")
        dg_rasters[raster] = misc.RasterizeWKT(
            wkt.replace("1", str(counter)), "EPSG:4326"
        )

    graph_rasters = {
        lizard_rasters[k]: list(v.serialize()["graph"].items())[0][1]
        for k, v in dg_rasters.items()
    }
    return dg_rasters, graph_rasters


def raster_seriesblocks(dg_rasters, parcels):
    """Aggregate rasters per object as seriesblocks (sb) objects and / or serialized graphs"""
    sb_graphs = {}
    sb_objects = {}
    for raster, data in dg_rasters.items():
        lbl = f"{raster}_label"
        raster_sb = f"{raster}_sb"
        agg = AggregateRaster(parcels, data, "max", "EPSG:4326", 0.00001, None, lbl)
        sb = GetSeriesBlock(agg, lbl)
        # seriesblock objects
        sb_objects[raster_sb] = sb
        # seriesblock graphs
        sb_graph = sb.serialize()["graph"]
        sb_tmp_raster_key = list(sb_graph.keys())[-1]
        sb_graph[raster_sb] = sb_graph.pop(sb_tmp_raster_key)
        sb_graphs = {**sb_graphs, **sb_graph}
    return sb_objects


def get_labeltype_source(result_seriesblock, graph_rasters, labeled_parcels):
    """Serialize result and replace mimic data with Lizard data. 
    Return dg_source, the lizard labeltype config"""
    dg_source = result_seriesblock.serialize()
    parcels_with_labelparameters = False
    parcels_block = "parcels"
    for block in dg_source["graph"]:
        block_value = dg_source["graph"][block]
        if isinstance(block_value[1], str):
            # replace mimic rasters with Lizard rasters
            if "dask_geomodeling.raster.misc.RasterizeWKT" in block_value[0]:
                for uuid, graph_raster in graph_rasters.items():
                    if block_value == graph_raster:
                        dg_source["graph"][block] = [
                            "lizard_nxt.blocks.LizardRasterSource",
                            uuid,
                        ]
            # replace mimic parcels with Lizard parcels
            if "parcels.geojson" in block_value[1]:
                dg_source["graph"][block] = labeled_parcels["parcels"]
                parcels_block = block
            if "labelparameters.geojson" in block_value[1]:
                dg_source["graph"][block] = labeled_parcels["parcels_labeled"]
                parcels_with_labelparameters = True
            if "sources.GeometryWKTSource" in block_value[0]:
                dg_source["graph"][block] = labeled_parcels["parcels_labeled"]
            if "MergeGeometryBlocks" in block:
                dg_source["graph"][block] = labeled_parcels["parcels_labeled"]

    # replace mimic parcels with Lizard parcels
    add_labeled_parcels = dict(
        (k, labeled_parcels[k]) for k in tuple(labeled_parcels.keys())[1:-1]
    )

    if parcels_with_labelparameters:
        add_labeled_parcels = dict(
            (k, labeled_parcels[k]) for k in tuple(labeled_parcels.keys())[1:-1]
        )
        add_labeled_parcels[list(add_labeled_parcels.keys())[0]][1] = parcels_block
        dg_source["graph"] = {**dg_source["graph"], **add_labeled_parcels}
    return dg_source


def patch_labeltype(dg_source, username, password, labeltype_uuid):
    """Serialize model (to json form) and replace raster file sources with lizard raster sources
    Set final json and PATCH the labeltype"""
    source = {"source": dg_source}
    # specify credentials for Lizard
    headers = {
        "username": username,
        "password": password,
        "Content-Type": "application/json",
    }
    # PATCH the labeltype
    labeltype_url = f"https://spiceup.lizard.net/api/v3/labeltypes/{labeltype_uuid}/"

    response = requests.patch(
        url=labeltype_url,
        headers=headers,
        data=simplejson.dumps(source, ignore_nan=True),
    )
    return response

def configure_logger(loglevel):
    logger = logging.getLogger("labellogger")
    logger.setLevel(loglevel)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setLevel(loglevel)
    sh.setFormatter(formatter)
    logger.addHandler(sh)