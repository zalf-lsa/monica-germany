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
import itertools
#import copy
from StringIO import StringIO
from datetime import date, datetime, timedelta
from collections import defaultdict, OrderedDict
#import types
import sys
print sys.path
import fnmatch

from netCDF4 import Dataset
import numpy as np

def main():

    def create_avg_grid(path_to_ncs, pattern, path_to_out_dir):

        arr_count = 0
        acc_arr = None
        acc_year_arr = None 
        year_arr_count = 0
        prev_year = 0
        nrows = 0
        ncols = 0
        header = ""

        for filename in sorted(os.listdir(path_to_ncs)):
            if fnmatch.fnmatch(filename, pattern):
                year = int(filename[5:9])

                ds = Dataset(path_to_ncs + filename)
                arrs = ds.variables["temperature"]
                days, _, _ = arrs.shape

                if acc_arr is None:
                    _, nrows, ncols = arrs.shape
                    acc_arr = np.full((nrows, ncols), 0.0, dtype=float)
                    header = """nrows {}
ncols {}
xllcorner 12345
yllcorner 12345
cellsize 1000
nodata_value 9999.0""".format(nrows, ncols)

                if prev_year != year:
                    if acc_year_arr is not None:
                        acc_year_arr /= year_arr_count
                        np.savetxt(path_to_out_dir + "Tavg-" + str(prev_year) + "-avg.asc", np.flipud(acc_year_arr), header=header, delimiter=" ", comments="", fmt="%.1f")
                        year_arr_count = 0
                    acc_year_arr = np.full((nrows, ncols), 0.0, dtype=float)

                for day in range(days):
                    acc_arr += arrs[day]
                    acc_year_arr += arrs[day]
                    arr_count += 1
                    year_arr_count += 1
                    print "added:", filename, "day:", day, "count:", arr_count 
    
                prev_year = year
                ds.close()


        if acc_arr is not None:
            acc_year_arr /= year_arr_count
            np.savetxt(path_to_out_dir + "Tavg-" + str(prev_year) + "-avg.asc", np.flipud(acc_year_arr), header=header, delimiter=" ", comments="", fmt="%.1f")
            acc_arr /= arr_count
            np.savetxt(path_to_out_dir + "Tavg-1995-2012-avg.asc", np.flipud(acc_arr), header=header, delimiter=" ", comments="", fmt="%.1f")
            

    create_avg_grid("N:/climate/dwd/grids/germany/daily/", "tavg_*_daymean.nc", "./")

   

main()