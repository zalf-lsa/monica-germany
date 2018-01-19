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
sys.path.append("./calibrator")
#print sys.path
import zmq
#print "pyzmq version: ", zmq.pyzmq_version(), " zmq version: ", zmq.zmq_version()

import sqlite3
import numpy as np
from scipy.interpolate import NearestNDInterpolator
from pyproj import Proj, transform

from multiprocessing import Process

from sampler_MONICA import start_calibration
from run_producer_consumer_aggregation import prod_cons_calib

def main():

    config = {
        "server": "cluster2"
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v

    #load default params
    with open("calibrator/default-params/wheat.json") as _:
        species_params = json.load(_)
    with open("calibrator/default-params/winter-wheat.json") as _:
        cultivar_params = json.load(_)
    with open("calibrator/default-params/wheat-residue.json") as _:
        residue_params = json.load(_)

    #create crop object
    custom_crop = {
        "is-winter-crop": True,
        "cropParams": {
            "species": species_params,
            "cultivar": {
                "=": cultivar_params,
                "StageTemperatureSum": [
                    [
                        150, 
                        577, 
                        531, 
                        155, 
                        135, 
                        25
                    ], 
                    "°C d"
                ]
            }
        },
        "residueParams": residue_params
    }

    def read_design_csv(path_to_design_csv):
        with open(path_to_design_csv) as design_file:
            setups = {}
            reader = csv.reader(design_file)
            header_cols = reader.next()
            for row in reader:
                data = {}
                for i, header_col in enumerate(header_cols):
                    if i in [0, 1, 3]:
                        continue
                    value = row[i]
                    if value in ["true", "false"]:
                        value = True if value == "true" else False
                    if i == 2:
                        value = int(value)
                    data[header_col] = value    
                setups[int(data["run.no"])] = data
            return setups

    setups = read_design_csv("P:/monica-germany/design_nocalib.csv")

    best_calibs = {}
    for run_id, setup in setups.iteritems():
        #if run_id != 177:
        #    continue
        if setup.get("Calibration", False):
            continue
            best_calibs[run_id] = start_calibration(setup=setup, custom_crop=custom_crop, server=config["server"])
        else:
            prod_cons_calib(design_setup=setup, custom_crop=custom_crop, server=config["server"])

    with open("best_calibrations.csv", "w") as _:
        json.dump(best_calibs, _, indent=4, sort_keys=True)


if __name__ == "__main__":
    main()
