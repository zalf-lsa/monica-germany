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
import string
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

def aggregate_by_grid(path_to_grids_dir = None, path_to_out_dir = None, pattern = None):

    config = {
        #"path-to-grids-dir": "P:/monica-germany/dwd-weather-germany-1995-2012/WW-1000m-patched-2017-11-30/",
        "path-to-grids-dir": "out/",
        
        "path-to-agg-grid": "N:/germany/landkreise_1000_gk3.asc",
        "path-to-agg2-grid": "N:/germany/bkr_1000_gk3.asc",
        "path-to-out-dir": "landkreise-avgs/",
        #"path-to-agg-grid": "D:/germany/bkr_1000_gk3.asc",
        #"path-to-out-dir": "bkr-avgs/",

        "path-to-corine-grid": "N:/germany/corine2006_1000_gk5.asc",

        "agg-grid-epsg": "3396", #gk3
        "agg2-grid-epsg": "3396", #gk3
        "grids-epsg": "31469", #gk5   #wgs84 = 4326
        "corine-epsg": "31469",

        "pattern": "*_yield_*.asc",
        #"pattern": "*nfert_avg.asc"
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v

    if path_to_grids_dir:
        config["path-to-grids-dir"] = path_to_grids_dir
    if path_to_out_dir:
        config["path-to-out-dir"] = path_to_out_dir
    if pattern:
        config["pattern"] = pattern

    def create_integer_grid_interpolator(arr, meta):
        "read an ascii grid into a map, without the no-data values"

        rows, cols = arr.shape

        xll_center = int(meta["xllcorner"]) + int(meta["cellsize"]) // 2
        yll_center = int(meta["yllcorner"]) + int(meta["cellsize"]) // 2
        yul_center = yll_center + (int(meta["nrows"]) - 1)*int(meta["cellsize"])
        no_data = meta["nodata_value"]

        points = []
        values = []

        for row in range(rows):
            for col in range(cols):
                value = arr[row, col]
                if value == no_data:
                    continue
                r = xll_center + col * int(meta["cellsize"])
                h = yul_center - row * int(meta["cellsize"])
                points.append([r, h])
                values.append(value)

        return NearestNDInterpolator(np.array(points), np.array(values))
    
    path_to_agg_grid = config["path-to-agg-grid"]
    agg_meta, header_str = read_header(path_to_agg_grid)
    arr_template = np.loadtxt(path_to_agg_grid, skiprows=6)
    agg_grid_interpolate = create_integer_grid_interpolator(arr_template, agg_meta)

    path_to_agg2_grid = config["path-to-agg2-grid"]
    agg2_meta, _ = read_header(path_to_agg_grid)
    arr2_template = np.loadtxt(path_to_agg2_grid, skiprows=6)
    agg2_grid_interpolate = create_integer_grid_interpolator(arr2_template, agg2_meta)

    path_to_corine_grid = config["path-to-corine-grid"]
    corine_meta, _ = read_header(path_to_corine_grid)
    corine_grid = np.loadtxt(path_to_corine_grid, skiprows=6)
    corine_grid_interpolate = create_integer_grid_interpolator(corine_grid, corine_meta)

    #wgs84 = Proj(init="epsg:4326")
    agg_grid_proj = Proj(init="epsg:" + config["agg-grid-epsg"])
    agg2_grid_proj = Proj(init="epsg:" + config["agg2-grid-epsg"])
    grids_proj = Proj(init="epsg:" + config["grids-epsg"])
    corine_proj = Proj(init="epsg:" + config["corine-epsg"])

    path_to_grids_dir = config["path-to-grids-dir"]
    path_to_out_dir = config["path-to-out-dir"]

    year_to_agg_id_to_value = {}

    for filename in os.listdir(path_to_grids_dir):
        if fnmatch.fnmatch(filename, config["pattern"]):
            print "averaging", path_to_grids_dir, filename

            try:
                year = int(filename.split("_")[2])
            except ValueError:
                year = -1

            meta, _ = read_header(path_to_grids_dir + filename)
            
            xll_center = meta["xllcorner"] + int(meta["cellsize"]) // 2
            yll_center = meta["yllcorner"] + int(meta["cellsize"]) // 2
            yul_center = yll_center + (int(meta["nrows"]) - 1)*int(meta["cellsize"])

            sums = defaultdict(lambda: 0)
            counts = defaultdict(lambda: 0)

            arr = np.loadtxt(path_to_grids_dir + filename, skiprows=6)

            rows, cols = arr.shape

            agg_to_agg2 = defaultdict(set)

            print rows,
            for row in range(rows):
                for col in range(cols):

                    value = arr[row, col]
                    if int(value) == -9999:
                        continue 

                    grid_r = xll_center + col * int(meta["cellsize"])
                    grid_h = yul_center - row * int(meta["cellsize"])

                    corine_grid_r, corine_grid_h = transform(grids_proj, corine_proj, grid_r, grid_h)
                    corine_id = int(corine_grid_interpolate(corine_grid_r, corine_grid_h))

                    #aggregate just agricultural landuse
                    if corine_id in [200, 210, 211, 212, 240, 241, 242, 243, 244]:
                        continue

                    agg_grid_r, agg_grid_h = transform(grids_proj, agg_grid_proj, grid_r, grid_h)
                    agg_id = int(agg_grid_interpolate(agg_grid_r, agg_grid_h))

                    agg2_grid_r, agg2_grid_h = transform(grids_proj, agg2_grid_proj, grid_r, grid_h)
                    agg2_id = int(agg2_grid_interpolate(agg2_grid_r, agg2_grid_h))

                    agg_to_agg2[agg_id].add(agg2_id)

                    sums[agg_id] += arr[row, col]
                    counts[agg_id] += 1
                
                if row % 10 == 0:
                    print row,
            print ""

            results = {}
            for id, sum_ in sums.iteritems():
                results[id] = sum_ / counts[id]

            with open(path_to_out_dir + filename[:-4] + "_avgs.csv", "wb") as _:
                csv_writer = csv.writer(_)
                csv_writer.writerow(["id", "value", "id2..."])
                for id in sorted(results.keys()):
                    row = [id, round(results[id], 1)]
                    for id2 in sorted(agg_to_agg2[id]):
                        row.append(id2)
                    csv_writer.writerow(row)

            arr = np.full(arr_template.shape, -9999, dtype=float)
            rows, cols = arr.shape
            for row in range(rows):
                for col in range(cols):
                    if int(arr_template[row, col]) != -9999:
                        arr[row, col] = results.get(int(arr_template[row, col]), -9999)
            np.savetxt(path_to_out_dir + filename[:-4] + "_avgs.asc", arr, header=header_str.strip(), delimiter=" ", comments="", fmt="%.1f")

            year_to_agg_id_to_value[year] = results

    return year_to_agg_id_to_value

if __name__ == "__main__":
    aggregate_by_grid()

def create_lat_dem_per_lk_csv():

    def create_integer_grid_interpolator(arr, meta):
        "read an ascii grid into a map, without the no-data values"

        rows, cols = arr.shape

        xll_center = int(meta["xllcorner"]) + int(meta["cellsize"]) // 2
        yll_center = int(meta["yllcorner"]) + int(meta["cellsize"]) // 2
        yul_center = yll_center + (int(meta["nrows"]) - 1)*int(meta["cellsize"])
        no_data = meta["nodata_value"]

        points = []
        values = []

        for row in range(rows):
            for col in range(cols):
                value = arr[row, col]
                if value == no_data:
                    continue
                r = xll_center + col * int(meta["cellsize"])
                h = yul_center - row * int(meta["cellsize"])
                points.append([r, h])
                values.append(value)

        return NearestNDInterpolator(np.array(points), np.array(values))
    
    path_to_lk_grid = "N:/germany/landkreise_1000_gk3.asc"
    lk_meta, _ = read_header(path_to_lk_grid)
    lk_grid = np.loadtxt(path_to_lk_grid, skiprows=6, dtype=int)

    path_to_bkr_grid = "N:/germany/bkr_1000_gk3.asc"
    bkr_meta, _ = read_header(path_to_bkr_grid)
    bkr_grid = np.loadtxt(path_to_bkr_grid, skiprows=6, dtype=int)
    bkr_gk3_grid_interpolate = create_integer_grid_interpolator(bkr_grid, bkr_meta)

    path_to_dem_grid = "N:/germany/dem_1000_gk5.asc"
    dem_meta, _ = read_header(path_to_dem_grid)
    dem_grid = np.loadtxt(path_to_dem_grid, skiprows=6)
    dem_gk5_grid_interpolate = create_integer_grid_interpolator(dem_grid, dem_meta)

    wgs84 = Proj(init="epsg:4326")
    gk3 = Proj(init="epsg:3396")
    gk5 = Proj(init="epsg:31469")

    lat_sums = defaultdict(lambda: 0)
    dem_sums = defaultdict(lambda: 0)
    counts = defaultdict(lambda: 0)
    lk_to_bkrs = defaultdict(set)

    rows, cols = lk_grid.shape
    for row in xrange(rows):
        for col in xrange(cols):

            xll_center = int(lk_meta["xllcorner"]) + int(lk_meta["cellsize"]) // 2
            yll_center = int(lk_meta["yllcorner"]) + int(lk_meta["cellsize"]) // 2
            yul_center = yll_center + (int(lk_meta["nrows"]) - 1)*int(lk_meta["cellsize"])
            no_data = lk_meta["nodata_value"]

            lk_id = lk_grid[row, col]
            if lk_id == no_data:
                continue
    
            gk3_r = xll_center + col * int(lk_meta["cellsize"])
            gk3_h = yul_center - row * int(lk_meta["cellsize"])
            _, lat = transform(gk3, wgs84, gk3_r, gk3_h)
            lat_sums[lk_id] += lat
            
            gk5_r, gk5_h = transform(gk3, gk5, gk3_r, gk3_h)
            elevation = dem_gk5_grid_interpolate(gk5_r, gk5_h)
            dem_sums[lk_id] += elevation
            
            bkr_id = int(bkr_gk3_grid_interpolate(gk3_r, gk3_h))
            lk_to_bkrs[lk_id].add(bkr_id)

            counts[lk_id] += 1

        if row % 10 == 0:
            print row,


    lat_results = {}
    dem_results = {}
    for lk_id in lat_sums.keys():
        lat_results[lk_id] = lat_sums.get(lk_id, 0) / counts[lk_id]
        dem_results[lk_id] = dem_sums.get(lk_id, 0) / counts[lk_id]

    with open("avg_elevation_latitude_per_landkreis.csv", "wb") as _:
        csv_writer = csv.writer(_)
        csv_writer.writerow(["lk_id", "latitude", "elevation", "bkr_ids..."])
        for lk_id in sorted(lat_results.keys()):
            row = [lk_id, round(lat_results[lk_id], 1), round(dem_results[lk_id], 1)]
            for bkr in sorted(lk_to_bkrs[lk_id]):
                row.append(bkr)
            csv_writer.writerow(row)
if __name__ == "#__main__":
    create_lat_dem_per_lk_csv()

def write_csv_data_into_landkreis_grid():

    out_dir = "indices-out/"
    files = {
        "1": "report_indices_id1.csv"
    }

    path_to_lk_grid = "N:/germany/landkreise_1000_gk3.asc"
    lk_meta, lk_header = read_header(path_to_lk_grid)
    lk_grid = np.loadtxt(path_to_lk_grid, skiprows=6, dtype=int)
    nodata_value = int(lk_meta["nodata_value"])

    for id, path_to_file in files.iteritems():

        data = defaultdict(dict)
        with open(path_to_file) as csv_f:
            csv_reader = csv.reader(csv_f)
            csv_header = csv_reader.next()

            for row in csv_reader:
                for i, header_col in enumerate(csv_header):
                    if i == 0:
                        continue
                    data[header_col][int(row[0])] = float(row[i])

        for output, out_data in data.iteritems():

            print "creating", id, output
            grid = np.empty(lk_grid.shape, dtype=float)
            np.copyto(grid, lk_grid)
            rows, cols = grid.shape
            print rows, "rows"
            
            for row in xrange(rows):
                if row % 10 == 0:
                    print row,

                for col in xrange(cols):
                    value = grid[row, col]
                    if int(value) == nodata_value:
                        continue
                    grid[row, col] = out_data.get(value, nodata_value)
            
            full_out_dir = out_dir + "indices-" + id + "-" + date.today().isoformat() + "/"
            try:
                os.makedirs(full_out_dir)
            except:
                pass
            outfile = full_out_dir + output + ".asc"
            np.savetxt(outfile, grid, header=lk_header.strip(), delimiter=" ", comments="", fmt="%.5f") 
            print "wrote", outfile
if __name__ == "#__main__":
    write_csv_data_into_landkreis_grid()

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
if __name__ == "#__main__":
    write_grid()

import locale
def create_kreis_grids_from_statistical_data():
    with open("P:/monica-germany/statistical-data/yieldstatger.csv") as stat_f:
        reader = csv.reader(stat_f, delimiter=";")

        for i in range(7):
            reader.next()

        crop_to_year_to_data = defaultdict(lambda: defaultdict(dict))

        crops = ["winterwheat", "rye", "winterbarley", "summerbarley", "oat", "triticale", "potatoes", "sugarbeet", "winterrapeseed", "silagemaize"]

        #loc = locale.getlocale()
        #locale.setlocale(locale.LC_ALL, 'deu')

        for row in reader:
            for i, crop in enumerate(crops):
                try:
                    year = int(row[0])
                    id = int(row[1])
                    yield_ = float(string.replace(row[3+i],",","."))
                except: 
                    continue
                
                crop_to_year_to_data[crop][year][id] = yield_

        path_to_template = "N:/germany/landkreise_1000_gk3.asc"
        arr_template = np.loadtxt(path_to_template, skiprows=6)
        arr_meta, header_str = read_header(path_to_template)
        nodata_value = arr_meta["nodata_value"]

        for crop, y2d in crop_to_year_to_data.iteritems():
            for year, data in y2d.iteritems():

                arr = np.full(arr_template.shape, nodata_value, dtype=float)
                rows, cols = arr.shape

                for row in range(rows):
                    for col in range(cols):
                        value = arr_template[row, col]
                        if value == nodata_value:
                            continue

                        v = data.get(value, -9999)
                        if v != -9999:
                            v *= 100
                        arr[row, col] = v

                np.savetxt("statistical-data-out/" + crop + "_" + str(year) + ".asc", arr, header=header_str.strip(), delimiter=" ", comments="", fmt="%.1f")
if __name__ == "#__main__":
    create_kreis_grids_from_statistical_data()            

def create_avg_grid(path_to_dir):

    acc_arrs = defaultdict(lambda: {"arr": None, "count": 0, "filename": "", "header": ""})
    arr_counts = {}

    for filename in sorted(os.listdir(path_to_dir)):
        if filename[-3:] == "asc":
            id = "_".join(filename.split("_")[:2])#2])
            arr = np.loadtxt(path_to_dir + filename, skiprows=6)

            if id not in acc_arrs:
                acc_arrs[id]["arr"] = np.full(arr.shape, 0.0, arr.dtype)
                acc_arrs[id]["filename"] = id + "_avg.asc"
                with open(path_to_dir + filename) as _:
                    for i in range(6):
                        acc_arrs[id]["header"] += _.readline()

            acc_arrs[id]["arr"] += arr
            acc_arrs[id]["count"] += 1
            print "added:", filename


    for id, acc_arr in acc_arrs.iteritems():
        acc_arr["arr"] /= acc_arr["count"]
        np.savetxt(path_to_dir + acc_arr["filename"], acc_arr["arr"], header=acc_arr["header"], delimiter=" ", comments="", fmt="%.1f")
        print "wrote:", acc_arr["filename"]
if __name__ == "#__main__": 
    create_avg_grid("out/")
