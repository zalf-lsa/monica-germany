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
        "archive-path-to-climate-csvs-dir": "/archiv-daten/md/data/climate/dwd/csvs/germany/"
        #"archive-path-to-climate-csvs-dir": "/archiv-daten/md/data/climate/isimip/csvs/germany/"
    },
    "berg-xps15": {
        "include-file-base-path": "C:/Users/berg.ZALF-AD/GitHub",
        "path-to-soil-dir": "D:/soil/buek1000/brd/",
        "path-to-climate-csvs-dir": "D:/climate/dwd/csvs/germany/",
        #"path-to-climate-csvs-dir": "N:/climate/isimip/csvs/germany/",
        "path-to-data-dir": "N:/",
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

    soil_db_con = sqlite3.connect(paths["path-to-soil-dir"] + "soil.sqlite")

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


    def read_grid_meta_data(path_to_asc_file):
        with open(path_to_asc_file) as _:
            meta = {}
            for index, line in enumerate(_):
                if index > 5: 
                    break
                key, value = line.strip().split()
                meta[key.strip().lower()] = float(value.strip())
            return meta

    
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

    wgs84 = Proj(init="epsg:4326")
    #gk3 = Proj(init="epsg:3396")
    gk5 = Proj(init="epsg:31469")

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

    soil_meta = read_grid_meta_data(paths["path-to-soil-dir"] + "buek1000_50_gk5.asc")
    with open(paths["path-to-soil-dir"] + "buek1000_50_gk5.asc") as soil_f:
        for _ in range(0, 6):
            soil_f.readline()
        
        scols = int(soil_meta["ncols"])
        srows = int(soil_meta["nrows"])
        scellsize = int(soil_meta["cellsize"])
        xllcorner = int(soil_meta["xllcorner"])
        yllcorner = int(soil_meta["yllcorner"])
                    
        resolution = 20
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

        crows_cols = set()

        for crop_id in crops_ids:

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
                            
                            unique_jobs[(crow, ccol, soil_id, int(round(heightNN/10.0)*10), int(round(slope)))] += 1

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
                    for (crow, ccol, soil_id, heightNN, slope), job in unique_jobs.iteritems():

                        uj_id += 1

                        #clat, clon = cdict[(crow, ccol)]
                        #slon, slat = transform(gk5, wgs84, r, h)
                        #print "srow:", srow, "scol:", scol, "h:", h, "r:", r, " inter:", inter, "crow:", crow, "ccol:", ccol, "slat:", slat, "slon:", slon, "clat:", clat, "clon:", clon
                        
                        env_template["cropRotation"] = crop_rotation_templates[crop_id]

                        sp_json = soil_io.soil_parameters(soil_db_con, soil_id)
                        soil_profile = monica_io.find_and_replace_references(sp_json, sp_json)["result"]
                        env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile
                        env_template["params"]["siteParameters"]["heightNN"] = heightNN
                        env_template["params"]["siteParameters"]["slope"] = slope / 100.0

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