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
import fnmatch
print sys.path

import numpy as np
from scipy.interpolate import NearestNDInterpolator
from pyproj import Proj, transform

def main():

    config = {
        "path-to-grids-dir": "P:/monica-germany/dwd-weather-germany-1995-2012/2017-11-08/",
        
        #"path-to-agg-grid": "D:/germany/landkreise/landkreise_1000_gk3.asc",
        #"path-to-out-dir": "landkreise-avgs/"
        "path-to-agg-grid": "D:/germany/bkr_soil-climate-regions/bkr_1000_gk3.asc",
        "path-to-out-dir": "bkr-avgs/",

        "agg-grid-epsg": "3396", #gk3
        "grids-epsg": "31469", #gk5   #wgs84 = 4326

        "pattern": "*_yield_*.asc"
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v


    def read_header(path_to_ascii_grid_file):
        "read metadata from esri ascii grid file"
        metadata = {}
        header_str = ""
        with open(path_to_ascii_grid_file) as _:
            for i in range(0, 6):
                line = _.readline()
                header_str += line
                sline = [x for x in line.split() if len(x) > 0]
                if len(sline) > 1:
                    metadata[sline[0].strip().lower()] = float(sline[1].strip())
        return metadata, header_str

    def create_agg_grid_interpolator(path_to_file):
        "read an ascii grid into a map, without the no-data values"

        arr = np.loadtxt(path_to_file, skiprows=6)
        rows, cols = arr.shape

        meta, header_str = read_header(path_to_file)

        if "xllcorner" not in meta \
        or "yllcorner" not in meta \
        or "cellsize" not in meta \
        or "nrows" not in meta:
            print path_to_file, "didn't contain correct header information, can't create gk3_interpolator"
            return None, None

        xll_center = int(meta["xllcorner"]) + int(meta["cellsize"]) // 2
        yll_center = int(meta["yllcorner"]) + int(meta["cellsize"]) // 2
        yul_center = yll_center + (int(meta["nrows"]) - 1)*int(meta["cellsize"])

        points = []
        values = []

        for row in range(rows):
            for col in range(cols):
                r = xll_center + col * int(meta["cellsize"])
                h = yul_center - row * int(meta["cellsize"])
                points.append([r, h])
                values.append(arr[row, col])

        return NearestNDInterpolator(np.array(points), np.array(values)), arr, header_str
    
    agg_grid_interpolate, arr_template, header_str = create_agg_grid_interpolator(config["path-to-agg-grid"])

    #wgs84 = Proj(init="epsg:4326")
    agg_grid_proj = Proj(init="epsg:" + config["agg-grid-epsg"])
    grids_proj = Proj(init="epsg:" + config["grids-epsg"])

    path_to_grids_dir = config["path-to-grids-dir"]
    path_to_out_dir = config["path-to-out-dir"]

    for filename in os.listdir(path_to_grids_dir):
        if fnmatch.fnmatch(filename, config["pattern"]):
            print "averaging", path_to_grids_dir, filename

            meta, _ = read_header(path_to_grids_dir + filename)
            
            if "xllcorner" not in meta \
            or "yllcorner" not in meta \
            or "cellsize" not in meta \
            or "nrows" not in meta:
                print path_to_grids_dir, filename, "didn't contain correct header information, skipping this file"
                continue

            xll_center = meta["xllcorner"] + int(meta["cellsize"]) // 2
            yll_center = meta["yllcorner"] + int(meta["cellsize"]) // 2
            yul_center = yll_center + (int(meta["nrows"]) - 1)*int(meta["cellsize"])

            sums = defaultdict(lambda: 0)
            counts = defaultdict(lambda: 0)

            arr = np.loadtxt(path_to_grids_dir + filename, skiprows=6)

            rows, cols = arr.shape

            print rows,
            for row in range(rows):
                for col in range(cols):

                    value = arr[row, col]
                    if int(value) == -9999:
                        continue 

                    grid_r = xll_center + col * int(meta["cellsize"])
                    grid_h = yul_center - row * int(meta["cellsize"])

                    agg_grid_r, agg_grid_h = transform(grids_proj, agg_grid_proj, grid_r, grid_h)
                    id = int(agg_grid_interpolate(agg_grid_r, agg_grid_h))

                    if int(id) == -9999:
                        #print "row:", row, "col:", col, "value:", value, "gave no id, skipping it"
                        continue

                    sums[id] += arr[row, col]
                    counts[id] += 1
                
                if row % 10 == 0:
                    print row,
            print ""

            results = {}
            for id, sum_ in sums.iteritems():
                results[id] = sum_ / counts[id]

            with open(path_to_out_dir + filename[:-4] + "_avgs.csv", "wb") as _:
                csv_writer = csv.writer(_)
                for id in sorted(results.keys()):
                    csv_writer.writerow([id, round(results[id], 1)])

            arr = np.full(arr_template.shape, -9999, dtype=float)
            rows, cols = arr.shape
            for row in range(rows):
                for col in range(cols):
                    if int(arr_template[row, col]) != -9999:
                        arr[row, col] = results[int(arr_template[row, col])]
            np.savetxt(path_to_out_dir + filename[:-4] + "_avgs.asc", arr, header=header_str.strip(), delimiter=" ", comments="", fmt="%.1f")

def write_grid():
    with open("D:/soil/buek1000/brd/buek1000_50_gk5.asc") as _:

        ids = set()
        for count, line in enumerate(_):
            if count < 6:
                continue
            for id in map(int, line.split()):
                if id != -9999:
                    ids.add(id)
            if count % 100 == 0:
                print count,

        print "\n", ids

        #for line_no in range(6 + 11194 + 1):
        #    if line_no == 6 + 11194:
        #        line = _.readline()
        #        cols = line.split()
        #        soil_id = cols[3697]
        #        print soil_id
        #        exit()
        #    else: 
        #        _.readline()

    print ""


#write_grid()
main()