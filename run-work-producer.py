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
        "server": "cluster2",
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
        sim = json.load(_)

    with open("site.json") as _:
        site = json.load(_)

    with open("crop.json") as _:
        crop = json.load(_)

    #sim["include-file-base-path"] = PATHS[USER]["INCLUDE_FILE_BASE_PATH"]

    cdict = {}
    def create_interpolator(path_to_file_lat_lon_coordinates, path_to_data_no_data, wgs84, gk5):
        "read an ascii grid into a map, without the no-data values"
        lat_lon_f = open(path_to_file_lat_lon_coordinates)
        # skip 2 headerlines
        lat_lon_f.next()
        lat_lon_f.next()

        no_data_f = open(path_to_data_no_data)
        # skip 3 headerlines
        no_data_f.next()
        no_data_f.next()
        no_data_f.next()

        crows = 938
        ccols = 720

        points = []
        values = []

        i = 0
        for row, ll_line in enumerate(lat_lon_f):
            col_ll_strs = ll_line.strip().split(" ")

            nd_line = no_data_f.next()
            col_nd_strs = nd_line.strip().split(" ")

            for col, col_ll_str in enumerate(col_ll_strs):
                if col_nd_strs[col] == "-":
                    continue
                
                clat, clon = col_ll_str.split("|")
                cdict[(row, col)] = (clat, clon)
                cr, ch = transform(wgs84, gk5, clon, clat)
                points.append([ch, cr])
                values.append(1000 * row + col)
                #print "row:", row, "col:", col, "clat:", clat, "clon:", clon, "h:", h, "r:", r, "val:", values[i]

                i += 1

        lat_lon_f.close()
        no_data_f.close()

        return NearestNDInterpolator(np.array(points), np.array(values))


    wgs84 = Proj(init="epsg:4326")
    #gk3 = Proj(init="epsg:31467")
    gk5 = Proj(init="epsg:31469")

    s = time.clock()
    interpol = create_interpolator(paths["path-to-climate-csvs-dir"] + "germany-lat-lon-coordinates.grid", \
    paths["path-to-climate-csvs-dir"] + "germany-data-no-data.grid", wgs84, gk5)
    e = time.clock()
    print (e-s), "s"

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

        unknown_soil_ids = set()

        for srow in xrange(0, vrows*resolution, resolution):

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
                            print "row/col:", row, "/", col, "has unknown soil_id:", soil_id
                            unknown_soil_ids.add(soil_id)
                            continue
                        
                        #get coordinate of clostest climate element of real soil-cell
                        sh = yllcorner + (scellsize / 2) + (srows - row - 1) * scellsize
                        sr = xllcorner + (scellsize / 2) + col * scellsize
                        #inter = crow/ccol encoded into integer
                        inter = interpol(sh, sr)
                        
                        unique_jobs[(inter, soil_id)] += 1

                        full_no_data_block = False

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
                        env["pathToClimateCSV"] = paths["path-to-climate-csvs-dir"] + "germany/row-" + str(crow+1) + "/col-" + str(ccol) + ".csv"
                    else:
                        env["pathToClimateCSV"] = paths["archive-path-to-climate-csvs-dir"] + "germany/row-" + str(crow+1) + "/col-" + str(ccol) + ".csv"


                    env["customId"] = str(resolution) \
                    + "|" + str(vrow) + "|" + str(vcol) \
                    + "|" + str(crow) + "|" + str(ccol) \
                    + "|" + str(soil_id)

                    #with open("envs/env-"+str(i)+".json", "w") as _:
                    #    _.write(json.dumps(env))

                    socket.send_json(env)
                    print "sent env ", sent_env_count, " customId: ", env["customId"]
                    #exit()
                    sent_env_count += 1

        print "unknown_soil_ids:", unknown_soil_ids

    stop_time = time.clock()

    print "sending ", (sent_env_count-1), " envs took ", (stop_time - start_time), " seconds"
    #print "ran from ", start, "/", row_cols[start], " to ", end, "/", row_cols[end]
    return


main()