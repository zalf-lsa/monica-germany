#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import time
import os
import math
import json
import csv
import copy
from StringIO import StringIO
from datetime import date, datetime, timedelta
from collections import defaultdict
#import types
import sys
print sys.path
import zmq
print "pyzmq version: ", zmq.pyzmq_version(), " zmq version: ", zmq.zmq_version()

import sqlite3
import numpy as np
from scipy.interpolate import NearestNDInterpolator
from pyproj import Proj, transform

import monica_io
import soil_io
import ascii_io

LOCAL_RUN = False

PATHS = {
    "berg-lc": {
        "include-file-base-path": "C:/Users/berg.ZALF-AD/GitHub",
        "path-to-soil-dir": "N:/soil/buek1000/brd/",
        "path-to-climate-csvs-dir": "N:/climate/dwd/csvs/germany/",
        #"path-to-climate-csvs-dir": "N:/climate/isimip/csvs/germany/",
        "path-to-data-dir": "N:/",
        "path-to-projects-dir": "P:/",
        "archive-path-to-climate-csvs-dir": "/archiv-daten/md/data/climate/dwd/csvs/germany/"
        #"archive-path-to-climate-csvs-dir": "/archiv-daten/md/data/climate/isimip/csvs/germany/"
    },
    "berg-xps15": {
        "include-file-base-path": "C:/Users/berg.ZALF-AD/GitHub",
        "path-to-soil-dir": "D:/soil/buek1000/brd/",
        "path-to-climate-csvs-dir": "D:/climate/dwd/csvs/germany/",
        #"path-to-climate-csvs-dir": "N:/climate/isimip/csvs/germany/",
        "path-to-data-dir": "N:/",
        "path-to-projects-dir": "P:/",
        "archive-path-to-climate-csvs-dir": "/archiv-daten/md/data/climate/dwd/csvs/germany/"
        #"archive-path-to-climate-csvs-dir": "/archiv-daten/md/data/climate/isimip/csvs/germany/"
    }
}

def main():
    "main"

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    config_and_no_data_socket = context.socket(zmq.PUSH)

    config = {
        "user": "berg-lc",
        "port": "6666",
        "no-data-port": "5555",
        "server": "cluster3",
        "start-row": "0",
        "end-row": "-1"
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v

    paths = PATHS[config["user"]]

    soil_db_con = sqlite3.connect(paths["path-to-data-dir"] + "germany/buek1000.sqlite")

    config_and_no_data_socket.bind("tcp://*:" + str(config["no-data-port"]))

    if LOCAL_RUN:
        socket.connect("tcp://localhost:" + str(config["port"]))
    else:
        socket.connect("tcp://" + config["server"] + ":" + str(config["port"]))


    with open("sim.json") as _:
        sim_json = json.load(_)

    with open("site.json") as _:
        site_json = json.load(_)

    with open("crop.json") as _:
        crop_json = json.load(_)


    def read_sim_setups(path_to_setups_csv):
        with open(path_to_setups_csv) as setup_file:
            setups = {}
            reader = csv.reader(setup_file)
            header_cols = reader.next()
            for row in reader:
                data = {}
                for i, header_col in enumerate(header_cols):
                    value = row[i]
                    if value in ["true", "false"]:
                        value = True if value == "true" else False
                    data[header_col] = row[i]    
                setups[data["run-id"]] = data
            return setups

    setups = read_sim_setups(paths["path-to-projects-dir"] + "monica-germany/sim_setups_mb.csv")
    run_setups = [3]

    def read_grid_meta_data(path_to_asc_file):
        with open(path_to_asc_file) as _:
            meta = {}
            for index, line in enumerate(_):
                if index > 5: 
                    break
                key, value = line.strip().split()
                meta[key.strip().lower()] = float(value.strip())
            return meta
    
    wgs84 = Proj(init="epsg:4326")
    #gk3 = Proj(init="epsg:3396")
    gk5 = Proj(init="epsg:31469")

    ilr_seed_harvest_data = defaultdict(lambda: defaultdict(dict))
    def create_seed_harvest_gk5_interpolator(path_to_csv_file, wgs85, gk5):
        "read seed/harvest dates"

        rename = {
            "Winterweizen": [("WW", True)],
            "Mais": [("SM", False), ("GM", False)],
            "Winterraps": [("WRa", True)],
            "Zucker-Ruebe": [("SBee", False)],
            "Sommergerste": [("SB", False)]
        }

        with open(path_to_csv_file) as _:
            reader = csv.reader(_)

            # skip header line
            reader.next()

            points = []
            values = []

            prev_cs = None
            prev_lat_lon = [None, None]
            data_at_cs = defaultdict()
            for row in reader:
                
                cs = int(row[1])

                # if new climate station, store the data of the old climate station
                if prev_cs is not None and cs != prev_cs:

                    llat, llon = prev_lat_lon
                    r_gk5, h_gk5 = transform(wgs84, gk5, llon, llat)
                        
                    points.append([r_gk5, h_gk5])
                    values.append(prev_cs)

                crop_name = row[5]
                crop_ids = rename[crop_name]
                for crop_id, is_wintercrop in crop_ids:
                    try:
                        sd, sm = map(int, row[6].split(".")[:2])
                        ilr_seed_harvest_data[cs][crop_id]["sowing-date"] = "0000-{:02d}-{:02d}".format(sm, sd)

                        esd, esm = map(int, row[11].split(".")[:2])
                        ilr_seed_harvest_data[cs][crop_id]["earliest-sowing-date"] = "0000-{:02d}-{:02d}".format(esm, esd)

                        lsd, lsm = map(int, row[12].split(".")[:2])
                        ilr_seed_harvest_data[cs][crop_id]["latest-sowing-date"] = "0000-{:02d}-{:02d}".format(lsm, lsd)

                        digit = 1 if is_wintercrop else 0

                        hd, hm = map(int, row[9].split(".")[:2])
                        ilr_seed_harvest_data[cs][crop_id]["harvest-date"] = "000{}-{:02d}-{:02d}".format(digit, hm, hd)

                        ehd, ehm = map(int, row[13].split(".")[:2])
                        ilr_seed_harvest_data[cs][crop_id]["earliest-harvest-date"] = "000{}-{:02d}-{:02d}".format(digit, ehm, ehd)

                        lhd, lhm = map(int, row[14].split(".")[:2])
                        ilr_seed_harvest_data[cs][crop_id]["latest-harvest-date"] = "000{}-{:02d}-{:02d}".format(digit, lhm, lhd)
                    except:
                        continue

                lat = float(row[2])
                lon = float(row[3])
                prev_lat_lon = (lat, lon)      
                prev_cs = cs

            return NearestNDInterpolator(np.array(points), np.array(values))

    seed_harvest_gk5_interpolate = create_seed_harvest_gk5_interpolator(paths["path-to-projects-dir"] + "monica-germany/ILR_SEED_HARVEST_crops.csv", wgs84, gk5)


    def create_dem_slope_gk5_interpolator(dem_grid, meta):
        "read an ascii grid into a map, without the no-data values"

        rows, cols = dem_grid.shape
        xll = int(meta["xllcorner"])
        yll = int(meta["yllcorner"])
        cellsize = int(meta["cellsize"])
        
        points = []
        values = []

        i = 0
        for row in range(rows):
            for col in range(cols):
                heightNN = dem_grid[row, col]

                if heightNN < -1000:
                    continue
                
                r_gk5 = xll + cellsize // 2 + col * cellsize
                h_gk5 = yll + cellsize // 2 + (rows - row - 1) * cellsize
                points.append([r_gk5, h_gk5])
                values.append((row, col))

        return NearestNDInterpolator(np.array(points), np.array(values))

    dem_slope_metadata = read_grid_meta_data(paths["path-to-data-dir"] + "/germany/dem_1000_gk5.asc")
    dem_grid = np.loadtxt(paths["path-to-data-dir"] + "/germany/dem_1000_gk5.asc", dtype=int, skiprows=6)
    slope_grid = np.loadtxt(paths["path-to-data-dir"] + "/germany/slope_1000_gk5.asc", dtype=float, skiprows=6)
    dem_slope_gk5_interpolate = create_dem_slope_gk5_interpolator(dem_grid, dem_slope_metadata)


    cdict = {}
    def create_climate_gk5_interpolator_from_json_file(path_to_latlon_to_rowcol_file, wgs84, gk5):
        "create interpolator from json list of lat/lon to row/col mappings"
        with open(path_to_latlon_to_rowcol_file) as _:
            points = []
            values = []

            for latlon, rowcol in json.load(_):
                row, col = rowcol
                clat, clon = latlon
                try:
                    cr_gk5, ch_gk5 = transform(wgs84, gk5, clon, clat)
                    cdict[(row, col)] = (round(clat, 4), round(clon, 4))
                    points.append([cr_gk5, ch_gk5])
                    values.append((row, col))
                    #print "row:", row, "col:", col, "clat:", clat, "clon:", clon, "h:", h, "r:", r, "val:", values[i]
                except:
                    continue

            return NearestNDInterpolator(np.array(points), np.array(values))

    climate_gk5_interpolate = create_climate_gk5_interpolator_from_json_file(paths["path-to-climate-csvs-dir"] + "../latlon-to-rowcol.json", wgs84, gk5)


    crops_ids = [
        "WW",
        #"WB",
        #"SB",
        ##"RY",
        #"GM",
        #"SM",
        #"PO",
        #"SBee",
        #"WRa"
    ]

    sent_env_count = 1
    start_time = time.clock()

    for setup_id in run_setups:

        if setup_id not in setups:
            continue
        setup = setups[setup_id]

        path_to_soil_map = paths["path-to-data-dir"] + "germany/buek1000_1000_gk5.asc"
        soil_meta = read_grid_meta_data(path_to_soil_map)
        with open(path_to_soil_map) as soil_f:
            for _ in range(0, 6):
                soil_f.readline()
            
            scols = int(soil_meta["ncols"])
            srows = int(soil_meta["nrows"])
            scellsize = int(soil_meta["cellsize"])
            xllcorner = int(soil_meta["xllcorner"])
            yllcorner = int(soil_meta["yllcorner"])
                        
            resolution = 1 #20
            vrows = srows // resolution
            vcols = scols // resolution
            lines = np.empty(resolution, dtype=object)

            # send config information to consumer
            config_and_no_data_socket.send_json({
                "type": "target-grid-metadata",
                "nrows": vrows, 
                "ncols": vcols,
                "cellsize": scellsize * resolution,
                "xllcorner": xllcorner,
                "yllcorner": yllcorner,
                "no-data": -9999
            })

            #unknown_soil_ids = set()

            env_template = monica_io.create_env_json_from_json_config({
                "crop": crop_json,
                "site": site_json,
                "sim": sim_json,
                "climate": ""
            })
            crop_rotation_templates = env_template.pop("cropRotation")
            env_template["cropRotation"] = []

            def get_value(list_or_value):
                return list_or_value[0] if isinstance(list_or_value, list) else list_or_value

            crows_cols = set()

            crop_id = setup["crop"]

            for srow in xrange(0, vrows*resolution, resolution):

                #print "srow:", srow, "crows/cols/lat/lon:", crows_cols
                print srow,

                #virtual row
                vrow = srow // resolution

                #print "srow:", srow, "vrow:", vrow

                for k in xrange(0, resolution):
                    lines[k] = np.fromstring(soil_f.readline(), dtype=int, sep=" ")

                if vrow < int(config["start-row"]):
                    continue
                elif int(config["end-row"]) > 0 and vrow > int(config["end-row"]):
                    break

                for scol in xrange(0, vcols*resolution, resolution):

                    unique_jobs = defaultdict(lambda: 0)

                    #virtual col
                    vcol = scol // resolution
                    #print "scol:", scol, "vcol:", vcol

                    full_no_data_block = True
                    for line_idx, line in enumerate(lines):
                        row = srow + line_idx

                        for col in xrange(vcol*resolution, (vcol+1)*resolution):

                            soil_id = line[col]
                            if soil_id == -9999:
                                continue
                            if soil_id < 1 or soil_id > 71:
                                #print "row/col:", row, "/", col, "has unknown soil_id:", soil_id
                                #unknown_soil_ids.add(soil_id)
                                continue
                            
                            #get coordinate of clostest climate element of real soil-cell
                            sh_gk5 = yllcorner + (scellsize / 2) + (srows - row - 1) * scellsize
                            sr_gk5 = xllcorner + (scellsize / 2) + col * scellsize
                            #inter = crow/ccol encoded into integer
                            crow, ccol = climate_gk5_interpolate(sr_gk5, sh_gk5)

                            ds_row, ds_col = dem_slope_gk5_interpolate(sr_gk5, sh_gk5)
                            heightNN = dem_grid[ds_row, ds_col]
                            slope = slope_grid[ds_row, ds_col]
                            
                            seed_harvest_cs = seed_harvest_gk5_interpolate(sr_gk5, sh_gk5)

                            unique_jobs[(crow, ccol, soil_id, int(round(heightNN/10.0)*10), int(round(slope)), seed_harvest_cs)] += 1

                            full_no_data_block = False

                            #clon, clat = transform(gk5, wgs84, sr_gk5, sh_gk5)
                            #crows_cols.add(((crow, ccol), (round(clat,2), round(clon,2))))
                            #crows_cols.add((crow, ccol))
                            #print "scol:", scol, "vcol:", vcol, "crow/col:", (crow, ccol), "clat/lon:", cdict[(crow, ccol)], "slat/lon:", (round(clat,4),round(clon,4))
                    #continue

                    if full_no_data_block:
                        config_and_no_data_socket.send_json({
                            "type": "no-data",
                            "customId": str(resolution) + "|" + str(vrow) + "|" + str(vcol) + "|-1|-1|-1"
                        })
                        continue
                    else:
                        config_and_no_data_socket.send_json({
                            "type": "jobs-per-cell",
                            "count": len(unique_jobs),
                            "customId": str(resolution) + "|" + str(vrow) + "|" + str(vcol) + "|-1|-1|-1"
                        })

                    uj_id = 0
                    for (crow, ccol, soil_id, heightNN, slope, seed_harvest_cs), job in unique_jobs.iteritems():

                        uj_id += 1

                        clat, clon = cdict[(crow, ccol)]
                        #slon, slat = transform(gk5, wgs84, r, h)
                        #print "srow:", srow, "scol:", scol, "h:", h, "r:", r, " inter:", inter, "crow:", crow, "ccol:", ccol, "slat:", slat, "slon:", slon, "clat:", clat, "clon:", clon
                        
                        env_template["cropRotation"] = crop_rotation_templates[crop_id]
                        
                        # set external seed/harvest dates
                        seed_harvest_data = ilr_seed_harvest_data[seed_harvest_cs].get(crop_id, None)
                        if seed_harvest_data:
                            if setup["sowing-date"] == "fixed":
                                env_template["cropRotation"][0]["worksteps"][0]["date"] = seed_harvest_data["sowing-date"]
                            #if setup["harvest-date"]:
                            #env_template["cropRotation"][0]["worksteps"][1]["date"] = seed_harvest_data["harvest-date"]
                            #env_template["cropRotation"][0]["worksteps"][1]["latest-date"] = seed_harvest_data["latest-harvest-date"]

                        # set soil-profile
                        sp_json = soil_io.soil_parameters(soil_db_con, soil_id)
                        soil_profile = monica_io.find_and_replace_references(sp_json, sp_json)["result"]
                        env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile

                        # setting groundwater level
                        if setup["groundwater-level"]:
                            groundwaterlevel = 20
                            layer_depth = 0
                            for layer in soil_profile:
                                if layer.get("is_in_groundwater", False):
                                    groundwaterlevel = layer_depth
                                    print "setting groundwaterlevel of soil_id:", str(soil_id), "to", groundwaterlevel, "m"
                                    break
                                layer_depth += get_value(layer["Thickness"])
                            env_template["params"]["userEnvironmentParameters"]["MinGroundwaterDepthMonth"] = 3
                            env_template["params"]["userEnvironmentParameters"]["MinGroundwaterDepth"] = [max(0, groundwaterlevel - 0.2) , "m"]
                            env_template["params"]["userEnvironmentParameters"]["MaxGroundwaterDepth"] = [groundwaterlevel + 0.2, "m"]
                            
                        # setting impenetrable layer
                        if setup["impenetrable-layer"]:
                            impenetrable_layer_depth = get_value(env_template["params"]["userEnvironmentParameters"]["LeachingDepth"])
                            layer_depth = 0
                            for layer in soil_profile:
                                if layer.get("is_impenetrable", False):
                                    impenetrable_layer_depth = layer_depth
                                    print "setting leaching depth of soil_id:", str(soil_id), "to", impenetrable_layer_depth, "m"
                                    break
                                layer_depth += get_value(layer["Thickness"])
                            env_template["params"]["userEnvironmentParameters"]["LeachingDepth"] = [impenetrable_layer_depth, "m"]
                            env_template["params"]["siteParameters"]["ImpenetrableLayerDepth"] = [impenetrable_layer_depth, "m"]

                        if setup["elevation"]:
                            env_template["params"]["siteParameters"]["heightNN"] = heightNN

                        if setup["slope"]:
                            env_template["params"]["siteParameters"]["slope"] = slope / 100.0

                        if setup["latitude"]:
                            env_template["params"]["siteParameters"]["Latitude"] = clat

                        if setup["fertilization"]:
                            env_template["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = True

                        env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = setup["NitrogenResponseOn"]
                        env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = setup["WaterDeficitResponseOn"]
                        env_template["params"]["simulationParameters"]["EmergenceMoistureControlOn"] = setup["EmergenceMoistureControlOn"]
                        env_template["params"]["simulationParameters"]["EmergenceFloodingControlOn"] = setup["EmergenceFloodingControlOn"]

                        env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]

                        if LOCAL_RUN:
                            env_template["pathToClimateCSV"] = paths["path-to-climate-csvs-dir"] + "row-" + str(crow) + "/col-" + str(ccol) + ".csv"
                        else:
                            env_template["pathToClimateCSV"] = paths["archive-path-to-climate-csvs-dir"] + "row-" + str(crow) + "/col-" + str(ccol) + ".csv"

                        #print env_template["pathToClimateCSV"]

                        env_template["customId"] = str(resolution) \
                        + "|" + str(vrow) + "|" + str(vcol) \
                        + "|" + str(crow) + "|" + str(ccol) \
                        + "|" + str(soil_id) \
                        + "|" + crop_id \
                        + "|" + str(uj_id)

                        #with open("envs/env-"+str(i)+".json", "w") as _: 
                        #    _.write(json.dumps(env))

                        socket.send_json(env_template)
                        print "sent env ", sent_env_count, " customId: ", env_template["customId"]
                        #exit()
                        sent_env_count += 1

            #print "unknown_soil_ids:", unknown_soil_ids

            print "crows/cols:", crows_cols

    stop_time = time.clock()

    print "sending ", (sent_env_count-1), " envs took ", (stop_time - start_time), " seconds"
    #print "ran from ", start, "/", row_cols[start], " to ", end, "/", row_cols[end]
    return

main()