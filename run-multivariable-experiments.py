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

from multiprocessing import Process

rwp = __import__("run-work-producer")
rwc = __import__("run-grid-work-consumer")
gs = __import__("grids-scripts")
from sampler_MONICA import start_calibration

def main():

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

    setups = read_design_csv("design.csv")

    best_calibs = {}
    for run_id, setup in setups.iteritems():
        if setup["Calibration"]:
            best_calibs[run_id] = start_calibration(setup, custom_crop)
        else:
            prod_cons_calib(setup, custom_crop)

    with open("best_calibrations.csv", "w") as _:
        json.dump(best_calibs, _, indent=4, sort_keys=True)


def prod_cons_calib(design_setup, custom_crop, calib_id="no_calibration"):

    setup = {
        "run-id": design_setup["run.no"],
        "groundwater-level": design_setup["GroundWaterLevel"],
        "impenetrable-layer": design_setup["ImpenetrableLayer"],
        "elevation": True,
        "latitude": True,
        "slope": design_setup["Slope"],
        "sowing-date": design_setup["SowingDate"],
        "harvest-date": design_setup["HarvestDate"],
        "landcover": design_setup["LandCover"],
        "fertilization": design_setup["Nresponse_and_Fertil"],
        "NitrogenResponseOn": design_setup["Nresponse_and_Fertil"],
        "irrigation": False,
        "WaterDeficitResponseOn": design_setup["WaterDeficitResponse"],
        "LeafExtensionModifier": design_setup["LeafExtensionModifier"],
        "EmergenceMoistureControlOn": False,
        "EmergenceFloodingControlOn": False
    }

    path_to_grids_output = str(setup["run-id"]) + "/" + str(calib_id) + "/"
    producer = Process(target=rwp.run_producer, args=(setup, custom_crop))
    consumer = Process(target=rwc.run_consumer, args=(path_to_grids_output, True))
    producer.start()
    consumer.start()
    producer.join()
    consumer.join()

    path_to_aggregated_output = path_to_grids_output + "aggregated/"
    year_to_lk_to_value = gs.aggregate_by_grid(path_to_grids_dir=path_to_grids_output,
                                               path_to_out_dir=path_to_aggregated_output,
                                               pattern="*_yield_*.asc")

    return year_to_lk_to_value


if __name__ == "__main__":
    main()

