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
        "server": "cluster2",
        "prod-port": "6666",
        "cons-port": "7777",
        "nd-port": "5555",
        "run-ids": "all", #"[9, 10, 11, 12, 13, 14, 15, 16, 25, 26, 27, 28, 29, 30, 31, 32]"
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
    calibrated_custom_crop = {
        "is-winter-crop": True,
        "cropParams": {
            "species": species_params,
            "cultivar": {
                "=": cultivar_params,
                "StageTemperatureSum": [
                    [
                        150, 
                        584, 
                        526, 
                        168, 
                        389, 
                        25
                    ], 
                    "°C d"
                ]
            }
        },
        "residueParams": residue_params
    }

    default_custom_crop = {
        "is-winter-crop": True,
        "cropParams": {
            "species": species_params,
            "cultivar": cultivar_params
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
                    value = row[i]
                    if value in ["true", "false"]:
                        value = True if value == "true" else False
                    if i == 0:
                        value = int(value)
                    data[header_col] = value    
                setups[int(data["run.no"])] = data
            return setups

    setups = read_design_csv("P:/monica-germany/design_complete_mb.csv")
    
    server = {
        "producer": {
            "server": config["server"],
            "port": config["prod-port"],
            "nd-port": config["nd-port"]
        },
        "consumer": {
            "server": config["server"],
            "port": config["cons-port"],
            "nd-port": config["nd-port"]
        }
    }

    with open("best_calibrations.csv", "w") as _:
        writer = csv.writer(_)
        header = ["run_id", "best_cal_id", "params"]
        writer.writerow(header)

    run_ids_str = config["run-ids"]
    for run_id in setups.keys() if run_ids_str == "all" else json.loads(run_ids_str):
        setup = setups[run_id]
        #if run_id not in [2]:#[9, 10, 11, 12, 13, 14, 15, 16, 25, 26, 27, 28, 29, 30, 31, 32]:
        #    continue
        custom_crop = default_custom_crop if setup.get("Phenology", "default") == "default" else calibrated_custom_crop

        if setup.get("FCM_Calibration", False):
            #continue
            id_best, vals_params = start_calibration(setup=setup, custom_crop=custom_crop, server=server)
            with open("best_calibrations.csv", "ab") as _:
                writer = csv.writer(_)
                row = []
                row.append(str(run_id))
                row.append(str(id_best))
                row.append(str(vals_params))
                writer.writerow(row)
                
        else:
            #continue
            prod_cons_calib(design_setup=setup, custom_crop=custom_crop, server=server)


if __name__ == "__main__":
    main()
