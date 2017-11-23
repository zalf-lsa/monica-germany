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

import numpy as np
from scipy.interpolate import NearestNDInterpolator
from pyproj import Proj, transform

def main():


    def create_gk3_bkr_interpolator(path_to_bkrfile):
        "read an ascii grid into a map, without the no-data values"

        arr = np.loadtxt(path_to_bkrfile, skiprows=6)
        rows, cols = arr.shape

        xll_center = 3280800
        yll_center = 5238000
        yul_center = yll_center + (rows - 1)*1000

        points = []
        values = []

        for row in range(rows):
            for col in range(cols):
                r = xll_center + col * 1000
                h = yul_center - row * 1000
                points.append([r, h])
                values.append(arr[row, col])

        return NearestNDInterpolator(np.array(points), np.array(values))

    def read_header(path_to_ascii_grid_file):
        "read metadata from esri ascii grid file"
        metadata = {}
        with open(path_to_ascii_grid_file) as _:
            for i in range(0, 6):
                line = _.readline()
                sline = [x for x in line.split() if len(x) > 0]
                if len(sline) > 1:
                    metadata[sline[0].strip().lower()] = float(sline[1].strip())
        return metadata
    
    gk5_to_gk3_interpolate = create_gk3_bkr_interpolator("D:/germany/bkr_soil-climate-regions/bkr_1000_gk3.asc")

    #wgs84 = Proj(init="epsg:4326")
    gk3 = Proj(init="epsg:3396")
    gk5 = Proj(init="epsg:31469")

    path_to_dir = "P:/monica-germany/dwd-weather-germany-1995-2012/2017-02-11/"
    path_to_out_dir = "./bkr-avgs/"
    for filename in os.listdir(path_to_dir):
        if filename[17:22] == "yield" and filename[-3:] == "asc":
            print "averaging", path_to_dir, filename

            meta = read_header(path_to_dir + filename)
            
            if "xllcorner" not in meta \
            or "yllcorner" not in meta \
            or "cellsize" not in meta \
            or "nrows" not in meta:
                print path_to_dir, filename, "didn't contain correct header information, skipping this file"
                continue

            xll_center = meta["xllcorner"] + int(meta["cellsize"]) // 2
            yll_center = meta["yllcorner"] + int(meta["cellsize"]) // 2
            yul_center = yll_center + (int(meta["nrows"]) - 1)*int(meta["cellsize"])

            sums = defaultdict(lambda: 0)
            counts = defaultdict(lambda: 0)

            arr = np.loadtxt(path_to_dir + filename, skiprows=6)

            rows, cols = arr.shape

            print rows,
            for row in range(rows):
                for col in range(cols):

                    value = arr[row, col]
                    if int(value) == -9999:
                        continue 

                    gk5_r = xll_center + col * 1000
                    gk5_h = yul_center - row * 1000

                    gk3_r, gk3_h = transform(gk5, gk3, gk5_r, gk5_h)
                    bkr_id = int(gk5_to_gk3_interpolate(gk3_r, gk3_h))

                    if int(bkr_id) == -9999:
                        #print "row:", row, "col:", col, "value:", value, "gave no bkr_id, skipping it"
                        continue

                    sums[bkr_id] += arr[row, col]
                    counts[bkr_id] += 1
                
                print row,
            print ""


            results = {}
            for bkr_id, sum_ in sums.iteritems():
                results[bkr_id] = sum_ / counts[bkr_id]

            with open(path_to_out_dir + filename[:-4] + "_bkr_avgs.json", "w") as _:
                json.dump(results, _, indent=2)



main()