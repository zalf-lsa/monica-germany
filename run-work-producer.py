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
#import copy
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
    "lc": {
        "include-file-base-path": "C:/Users/berg.ZALF-AD.000/Documents/GitHub",
    },
    "xps15": {
        "include-file-base-path": "C:/Users/berg.ZALF-AD/GitHub",
        "path-to-soil-dir": "D:/soil/buek1000/brd/",
        "path-to-climate-csvs-dir": "D:/climate/dwd/csvs/",
        "archive-path-to-climate-csvs-dir": "/archiv-daten/md/data/climate/dwd/csvs/"
    }
}

def main():
    "main"

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)

    config = {
        "user": "xps15",
        "port": "6666",
        "server": "cluster2",
        "start-row": "1",
        "end-row": "8157"
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v

    paths = PATHS[config["user"]]

    soil_db_con = sqlite3.connect(paths["path-to-soil-dir"] + "soil.sqlite")

    if LOCAL_RUN:
        socket.connect("tcp://localhost:" + str(config["port"]))
    else:
        socket.connect("tcp://" + config["server"] + ":" + str(config["port"]))

    with open("sim.json") as _:
        sim = json.load(_)

    with open("site.json") as _:
        site = json.load(_)

    with open("crop.json") as _:
        crop = json.load(_)

    #sim["include-file-base-path"] = PATHS[USER]["INCLUDE_FILE_BASE_PATH"]

    cdict = {}
    def create_interpolator(path_to_file, wgs84, gk5):
        "read an ascii grid into a map, without the no-data values"
        with open(path_to_file) as file_:
            # skip headerlines
            file_.next()
            file_.next()

            crows = 938
            ccols = 720

            points = np.zeros((ccols*crows, 2), np.int32)
            values = np.zeros((ccols*crows), np.int32)

            i = -1
            row = -1
            for line in file_:
                row += 1
                col = -1

                for col_str in line.strip().split(" "):
                    col += 1
                    i += 1
                    clat, clon = col_str.split("|")
                    cdict[(row, col)] = (clat, clon)
                    cr, ch = transform(wgs84, gk5, clon, clat)
                    points[i, 0] = ch
                    points[i, 1] = cr
                    values[i] = 1000 * row + col
                    #print "row:", row, "col:", col, "clat:", clat, "clon:", clon, "h:", h, "r:", r, "val:", values[i]

            return NearestNDInterpolator(points, values)


    wgs84 = Proj(init="epsg:4326")
    #gk3 = Proj(init="epsg:31467")
    gk5 = Proj(init="epsg:31469")

    interpol = create_interpolator(paths["path-to-climate-csvs-dir"] + "germany-lat-lon-coordinates.grid", wgs84, gk5)

    def read_ascii_grid_into_numpy_array(path_to_file, no_of_headerlines=6, \
    extract_fn=lambda s: int(s), np_dtype=np.int32, nodata_value=-9999):
        "read an ascii grid into a map, without the no-data values"
        with open(path_to_file) as file_:
            nrows = 0
            ncols = 0
            row = -1
            arr = None
            skip_count = 0
            for line in file_:
                if skip_count < no_of_headerlines:
                    skip_count += 1
                    sline = line.split(sep=" ")
                    if len(sline) > 1:
                        key = sline[0].strip().upper()
                        if key == "NCOLS":
                            ncols = int(sline[1].strip())
                        elif key == "NROWS":
                            nrows = int(sline[1].strip())

                if skip_count == no_of_headerlines:
                    arr = np.full((nrows, ncols), nodata_value, dtype=np_dtype)

                row += 1
                col = -1
                for col_str in line.strip().split(" "):
                    col += 1
                    if int(col_str) == -9999:
                        continue
                    arr[row, col] = extract_fn(col_str)

            return arr

    #soil_ids = read_ascii_grid_into_numpy_array(paths["path-to-soil-dir"] + "buek1000_50_gk5.asc")

    germany_dwd_lats = read_ascii_grid_into_numpy_array(paths["path-to-climate-csvs-dir"] + "germany-lat-lon-coordinates.grid", 2, \
    lambda s: float(s.split("|")[0]), np_dtype=np.float)

    germany_dwd_nodata = read_ascii_grid_into_numpy_array(paths["path-to-climate-csvs-dir"] + "germany-data-no-data.grid", 2, \
    lambda s: 0 if s == "-" else 1)
    

    def update_soil(soil_res, row, col, crop_id):
        "update function"

        crop["cropRotation"][2] = crop_id

        site["SiteParameters"]["SoilProfileParameters"] = soil_io.soil_parameters(soil_db_con, soil_ids[row, col])

        #print site["SiteParameters"]["SoilProfileParameters"]


    i = 1
    start_time = time.clock()

    with open(paths["path-to-soil-dir"] + "buek1000_50_gk5.asc") as soil_f:

        no_all_scols = -1
        no_all_srows = -1
        scellsize = -1
        for i in range(0, 6):
            line = soil_f.readline()
            sline = line.split(sep=" ")
            if len(sline) > 1:
                key = sline[0].strip().upper()
                if key == "NCOLS":
                    no_all_scols = int(sline[1].strip())
                elif key == "NROWS":
                    no_all_srows = int(sline[1].strip())
                elif key == "CELLSIZE":
                    scellsize = int(sline[1].strip())
                    
        sresolution = 10
        no_srows = no_all_srows / sresolution
        no_scols = no_all_scols / sresolution
        lines = np.empty(sresolution, dtype=object)

        for srow in xrange(0, no_srows, sresolution):

            for k in xrange(0, sresolution):
                lines[k] = np.fromstring(gin.readline(), dtype=int, sep=" ")

            for scol in xrange(0, no_scols, sresolution):

                unique_jobs = defaultdict(lambda: 0)

                for line in lines:
                    for col in xrange(scol*sresolution, (scol+1)*resolution):

                        soil_id = line[col]
                        if soil_id == -9999:
                            continue

                        #get coordinate of clostest climate element of real soil-cell
                        sh = yll + (scellsize / 2) + (srows - srow) * scellsize
                        sr = xll + (scellsize / 2) + scol * scellsize
                        #inter = crow/ccol encoded into integer
                        inter = interpol(sh, sr)
                        
                        unique_jobs[(inter, soil_id)] += 1

                for (inter, soil_id), job in unique_jobs.iteritems():
                    
                    crow = int(inter / 1000)
                    ccol = inter - (crow * 1000)
                    clat, clon = cdict[(crow, ccol)]
                    slon, slat = transform(gk5, wgs84, r, h)
                    print "srow:", srow, "scol:", scol, "h:", h, "r:", r, " inter:", inter, "crow:", crow, "ccol:", ccol, "slat:", slat, "slon:", slon, "clat:", clat, "clon:", clon
                    
                    site["SiteParameters"]["SoilProfileParameters"] = soil_io.soil_parameters(soil_db_con, soil_id)
                    env = monica_io.create_env_json_from_json_config({
                        "crop": crop,
                        "site": site,
                        "sim": sim,
                        "climate": ""
                    })

                    env["csvViaHeaderOptions"] = sim["climate.csv-options"]

                    if LOCAL_RUN:
                        env["pathToClimateCSV"] = paths["path-to-climate-csvs"] + "germany/row-" + str(crow) + "/col-" + str(ccol) + ".csv"
                    else:
                        env["pathToClimateCSV"] = paths["archive-path-to-climate-csvs"] + "germany/row-" + str(crow) + "/col-" + str(ccol) + ".csv"


                    env["customId"] = crop_id \
                    + "|" + str(climate_resolution) \
                    + "|(" + str(climate_row) + "/" + str(climate_col) + ")" \
                    + "|" + str(soil_resolution) \
                    + "|(" + str(soil_row) + "/" + str(soil_col) + ")" \
                    + "|" + period \
                    + "|" + grcp \
                    + "|" + gcm \
                    + "|" + production_id

                    #with open("envs/env-"+str(i)+".json", "w") as _:
                    #    _.write(json.dumps(env))

                    socket.send_json(env)
                    print "sent env ", i, " customId: ", env["customId"]
                    #exit()
                    i += 1






    for c2s in climate_to_soil:

        step = c2s["step"]
        climate_resolution = c2s["climate"]
        soil_resolution = c2s["soil"]

        climate_to_soils = defaultdict(set)
        for mmm in lookup:
            climate_to_soils[mmm[climate_resolution]].add(mmm[soil_resolution])

        print "step: ", step, " ", sum([len(s[1]) for s in climate_to_soils.iteritems()]), " runs in climate_res: ", climate_resolution, " and soil_res: ", soil_resolution

        for (climate_row, climate_col), soil_coords in climate_to_soils.iteritems():

            #if (climate_row, climate_col) != (6,6):
            #    continue

            for soil_row, soil_col in soil_coords:

                #if (soil_row, soil_col) != (11,11):
                #    continue

                for crop_id in ["W", "M"]:
                    rootdepth_soillimited = update_soil(soil_resolution, soil_row, soil_col, crop_id)
                    env = monica_io.create_env_json_from_json_config({
                        "crop": crop,
                        "site": site,
                        "sim": sim,
                        "climate": ""
                    })

                    env["csvViaHeaderOptions"] = sim["climate.csv-options"]
                    env["events"] = sims["output"][crop_id]
                    for workstep in env["cropRotation"][0]["worksteps"]:
                        if workstep["type"] == "Seed":
                            current_rootdepth = float(workstep["crop"]["cropParams"]["cultivar"]["CropSpecificMaxRootingDepth"])
                            workstep["crop"]["cropParams"]["cultivar"]["CropSpecificMaxRootingDepth"] = min(current_rootdepth, rootdepth_soillimited)
                            break
                    
                    for production_id, switches in production_situations.iteritems():

                        env["params"]["simulationParameters"]["WaterDeficitResponseOn"] = switches["water-response"]
                        env["params"]["simulationParameters"]["NitrogenResponseOn"] = switches["nitrogen-response"]

                        for period_gcm in period_gcms:

                            grcp = period_gcm["grcp"]
                            period = period_gcm["period"]
                            gcm = period_gcm["gcm-rcp"]

                            #if period != "0":
                            #    continue
                            #if climate_resolution != 50:
                            #    continue
                            #if soil_resolution != 25:
                            #    continue
                            #if crop_id != "W":
                            #    continue

                            if period != "0":
                                climate_filename = "daily_mean_P{}_GRCP_{}_RES{}_C0{}R{}.csv".format(period, grcp, climate_resolution, climate_col, climate_row)
                            else:
                                climate_filename = "daily_mean_P{}_RES{}_C0{}R{}.csv".format(period, climate_resolution, climate_col, climate_row)
                                
                            if LOCAL_RUN:
                                env["pathToClimateCSV"] = PATHS[USER]["LOCAL_ARCHIVE_PATH_TO_PROJECT"] + "Climate_data/NRW_weather_climate_change_v3/res_" + str(climate_resolution) + "/period_" + period + "/GRCP_" + grcp + "/" + climate_filename
                            else:
                                env["pathToClimateCSV"] = PATHS[USER]["ARCHIVE_PATH_TO_PROJECT"] + "Climate_data/NRW_weather_climate_change_v3/res_" + str(climate_resolution) + "/period_" + period + "/GRCP_" + grcp + "/" + climate_filename

                            #initialize nitrate/ammonium in soil layers at start of simulation 
                            #for i in range(3):
                            #    env["cropRotation"][0]["worksteps"][i]["date"] = start_year[period] + "-01-01"

                            env["customId"] = crop_id \
                                                + "|" + str(climate_resolution) \
                                                + "|(" + str(climate_row) + "/" + str(climate_col) + ")" \
                                                + "|" + str(soil_resolution) \
                                                + "|(" + str(soil_row) + "/" + str(soil_col) + ")" \
                                                + "|" + period \
                                                + "|" + grcp \
                                                + "|" + gcm \
                                                + "|" + production_id

                            #with open("envs/env-"+str(i)+".json", "w") as _:
                            #    _.write(json.dumps(env))

                            socket.send_json(env)
                            print "sent env ", i, " customId: ", env["customId"]
                            #exit()
                            i += 1


    stop_store = time.clock()

    print "sending ", (i-1), " envs took ", (stop_store - start_store), " seconds"
    #print "ran from ", start, "/", row_cols[start], " to ", end, "/", row_cols[end]
    return


main()