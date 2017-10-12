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
    config_and_no_data_socket = context.socket(zmq.PUSH)

    config = {
        "user": "xps15",
        "port": "6666",
        "no-data-port": "5555",
        "server": "cluster2"#,
        #"start-row": "1",
        #"end-row": "8157"
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

    #germany_dwd_lats = read_ascii_grid_into_numpy_array(paths["path-to-climate-csvs-dir"] + "germany-lat-lon-coordinates.grid", 2, \
    #lambda s: float(s.split("|")[0]), np_dtype=np.float)

    #germany_dwd_nodata = read_ascii_grid_into_numpy_array(paths["path-to-climate-csvs-dir"] + "germany-data-no-data.grid", 2, \
    #lambda s: 0 if s == "-" else 1)
    

    def update_soil(soil_res, row, col, crop_id):
        "update function"

        crop["cropRotation"][2] = crop_id

        site["SiteParameters"]["SoilProfileParameters"] = soil_io.soil_parameters(soil_db_con, soil_ids[row, col])

        #print site["SiteParameters"]["SoilProfileParameters"]


    sent_env_count = 1
    start_time = time.clock()

    with open(paths["path-to-soil-dir"] + "buek1000_50_gk5.asc") as soil_f:

        scols = -1
        srows = -1
        scellsize = -1
        xllcorner = -1
        yllcorner = -1
        for sent_env_count in range(0, 6):
            line = soil_f.readline()
            sline = [x for x in line.split(" ") if len(x) > 0]
            if len(sline) > 1:
                key = sline[0].strip().upper()
                if key == "NCOLS":
                    scols = int(sline[1].strip())
                elif key == "NROWS":
                    srows = int(sline[1].strip())
                elif key == "CELLSIZE":
                    scellsize = int(sline[1].strip())
                elif key == "YLLCORNER":
                    yllcorner = int(sline[1].strip())
                elif key == "XLLCORNER":
                    xllcorner = int(sline[1].strip())
                    
        resolution = 10
        vrows = srows // resolution
        vcols = scols // resolution
        lines = np.empty(resolution, dtype=object)

        # send config information to consumer
        config_and_no_data_socket.send_json({
            "nrows": vrows, 
            "ncols": vcols,
            "cellsize": scellsize * resolution,
            "xllcorner": xllcorner,
            "yllcorner": yllcorner,
            "no-data": -9999
        })

        for srow in xrange(0, srows, resolution):

            #virtual row
            vrow = srow // resolution

            print "srow:", srow, "vrow:", vrow

            for k in xrange(0, resolution):
                lines[k] = np.fromstring(soil_f.readline(), dtype=int, sep=" ")

            for scol in xrange(0, scols, resolution):

                unique_jobs = defaultdict(lambda: 0)

                #virtual col
                vcol = scol // resolution
                print "scol:", scol, "vcol:", vcol

                full_no_data_block = True
                for line_idx, line in enumerate(lines):
                    row = srow + line_idx

                    for col in xrange(vcol*resolution, (vcol+1)*resolution):

                        soil_id = line[col]
                        if soil_id == -9999:
                            continue
                        
                        #get coordinate of clostest climate element of real soil-cell
                        sh = yllcorner + (scellsize / 2) + (srows - row) * scellsize
                        sr = xllcorner + (scellsize / 2) + col * scellsize
                        #inter = crow/ccol encoded into integer
                        inter = interpol(sh, sr)
                        
                        unique_jobs[(inter, soil_id)] += 1

                        full_no_data_block = False

                if full_no_data_block:
                    config_and_no_data_socket.send_json({
                        "type": "no-data",
                        "customId": str(resolution) + "|" + str(vrow) + "|" + str(vcol)
                    })
                    continue

                for (inter, soil_id), job in unique_jobs.iteritems():
                    
                    crow = int(inter / 1000)
                    ccol = inter - (crow * 1000)
                    #clat, clon = cdict[(crow, ccol)]
                    #slon, slat = transform(gk5, wgs84, r, h)
                    #print "srow:", srow, "scol:", scol, "h:", h, "r:", r, " inter:", inter, "crow:", crow, "ccol:", ccol, "slat:", slat, "slon:", slon, "clat:", clat, "clon:", clon
                    
                    site["SiteParameters"]["SoilProfileParameters"] = soil_io.soil_parameters(soil_db_con, soil_id)
                    env = monica_io.create_env_json_from_json_config({
                        "crop": crop,
                        "site": site,
                        "sim": sim,
                        "climate": ""
                    })

                    env["csvViaHeaderOptions"] = sim["climate.csv-options"]

                    if LOCAL_RUN:
                        env["pathToClimateCSV"] = paths["path-to-climate-csvs-dir"] + "germany/row-" + str(crow) + "/col-" + str(ccol) + ".csv"
                    else:
                        env["pathToClimateCSV"] = paths["archive-path-to-climate-csvs-dir"] + "germany/row-" + str(crow) + "/col-" + str(ccol) + ".csv"


                    env["customId"] = str(resolution) \
                    + "|" + str(vrow) + "|" + str(vcol) #\
                    #+ "|(" + str(crow) + "/" + str(ccol) + ")"

                    #with open("envs/env-"+str(i)+".json", "w") as _:
                    #    _.write(json.dumps(env))

                    #socket.send_json(env)
                    print "sent env ", sent_env_count, " customId: ", env["customId"]
                    #exit()
                    sent_env_count += 1

    stop_time = time.clock()

    print "sending ", (sent_env_count-1), " envs took ", (stop_time - start_time), " seconds"
    #print "ran from ", start, "/", row_cols[start], " to ", end, "/", row_cols[end]
    return


main()