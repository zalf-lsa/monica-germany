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

import os
import json
import csv
import copy
from collections import defaultdict
import sys
print sys.path

from multiprocessing import Process

rwp = __import__("run-work-producer")
rwc = __import__("run-grid-work-consumer")
gs = __import__("grids-scripts")

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
