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

import sys
#sys.path.insert(0, "C:\\Users\\berg.ZALF-AD\\GitHub\\monica\\project-files\\Win32\\Release")
#sys.path.insert(0, "C:\\Users\\berg.ZALF-AD\\GitHub\\monica\\src\\python")
#sys.path.insert(0, "C:\\Program Files (x86)\\MONICA")
print sys.path

import gc
import csv
import types
import os
import json
from datetime import datetime
from collections import defaultdict, OrderedDict
import numpy as np

import zmq
print "pyzmq version: ", zmq.pyzmq_version(), " zmq version: ", zmq.zmq_version()

import monica_io
#print "path to monica_io: ", monica_io.__file__

LOCAL_RUN = False

PATHS = {
    "lc": {
        "local-path-to-output-dir": "out/"
    },
    "xps15": {
        "local-path-to-output-dir": "out/"
    }
}


def create_output(result):
    "create output structure for single run"

    cm_count_to_vals = defaultdict(dict)
    if len(result.get("data", [])) > 0 and len(result["data"][0].get("results", [])) > 0:

        for data in result.get("data", []):
            results = data.get("results", [])
            oids = data.get("outputIds", [])

            #skip empty results, e.g. when event condition haven't been met
            if len(results) == 0:
                continue

            assert len(oids) == len(results)
            for kkk in range(0, len(results[0])):
                vals = {}

                for iii in range(0, len(oids)):
                    oid = oids[iii]
                    val = results[iii][kkk]

                    name = oid["name"] if len(oid["displayName"]) == 0 else oid["displayName"]

                    if isinstance(val, types.ListType):
                        for val_ in val:
                            vals[name] = val_
                    else:
                        vals[name] = val

                if "CM-count" not in vals or "Crop" not in vals:
                    print "Missing CM-count or Crop in result section. Skipping results section."
                    continue

                cm_count_to_vals[vals["CM-count"]].update(vals)

    return cm_count_to_vals


def write_row_to_grids(row_col_data, row, ncols, header, path_to_output_dir):
    "write grids row by row"

    make_dict_nparr = lambda: defaultdict(lambda: np.full((ncols,), -9999, dtype=np.float))

    output_grids = {
        "Yield": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 2}
    }

    cmc_to_crop = {}

    # skip this part if we write just a nodata line
    insert_nodata_row = False
    if row in row_col_data:
        no_data_cols = 0
        for col in xrange(0, ncols):
            if col in row_col_data[row]:
                rcd_val = row_col_data[row][col]
                if rcd_val == -9999:
                    no_data_cols += 1
                    continue
                else:
                    cmc_and_year_to_vals = defaultdict(lambda: defaultdict(list))
                    for cell_data in rcd_val:
                        for cm_count, data in cell_data.iteritems():
                            for key, val in output_grids.iteritems():
                                if cm_count not in cmc_to_crop:
                                    cmc_to_crop[cm_count] = data["Crop"]

                                cmc_and_year_to_vals[(cm_count, data["Year"])][key].append(data[key])

                    for (cm_count, year), key_to_vals in cmc_and_year_to_vals.iteritems():
                        for key, vals in key_to_vals.iteritems():
                            output_vals = output_grids[key]["data"]
                            if len(vals) > 0:
                                output_vals[(cm_count, year)][col] = sum(vals) / len(vals)
                            else:
                                output_vals[(cm_count, year)][col] = -9999
                                #no_data_cols += 1

        insert_nodata_row = no_data_cols == ncols

    for key, y2d_ in output_grids.iteritems():

        y2d = y2d_["data"]
        cast_to = y2d_["cast-to"]
        digits = y2d_["digits"]
        if cast_to == "int":
            mold = lambda x: str(int(x))
        else:
            mold = lambda x: str(round(x, digits))

        for (cm_count, year), row_arr in y2d.iteritems():

            crop = cmc_to_crop[cm_count]    
            crop = crop.replace("/", "").replace(" ", "")
            path_to_file = path_to_output_dir + crop + "_" + key + "_" + str(year) + "_" + str(cm_count) + ".asc"

            if not os.path.isfile(path_to_file):
                with open(path_to_file, "w") as _:
                    _.write(header)

            with open(path_to_file, "a") as _:

                if insert_nodata_row:
                    rowstr = " ".join(map(lambda x: "-9999", row_template))
                    _.write(rowstr +  "\n")

                rowstr = " ".join(map(lambda x: "-9999" if int(x) == -9999 else mold(x), row_arr))
                _.write(rowstr +  "\n")
    
    if row in row_col_data:
        del row_col_data[row]


def main():
    "collect data from workers"

    config = {
        "user": "xps15",
        "port": "7777",
        "no-data-port": "5555",
        "server": "cluster2", 
        "start-row": "0"
        #"end-row": "8157"
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v

    paths = PATHS[config["user"]]

    data = defaultdict(list)

    received_env_count = 1
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://localhost:" + config["no-data-port"])

    if LOCAL_RUN:
        socket.connect("tcp://localhost:" + config["port"])
    else:
        socket.connect("tcp://" + config["server"] + ":" + config["port"])
    socket.RCVTIMEO = 1000
    leave = False
    write_normal_output_files = False

    if not write_normal_output_files:  
        print("loading template for output...")
        #template_grid = create_template_grid(PATHS[USER]["LOCAL_PATH_TO_ARCHIV"] + "Soil/Carbiocial_Soil_Raster_final.asc", n_rows, n_cols)
        #datacells_per_row = np.sum(template_grid, axis=1) #.tolist()
        print("load complete")

        config_received = False
        while not config_received:
            try:
                msg = socket.recv_json()
                config_received = True
            except:
                continue
            
        nrows = int(msg["nrows"])
        ncols = int(msg["ncols"])
        cellsize = int(msg["cellsize"])
        xllcorner = int(msg["xllcorner"])
        yllcorner = int(msg["yllcorner"])
        no_data = int(msg["no-data"])

        header = "ncols\t\t" + str(ncols) + "\n" \
                 "nrows\t\t" + str(nrows) + "\n" \
                 "xllcorner\t" + str(xllcorner) + "\n" \
                 "yllcorner\t" + str(yllcorner) + "\n" \
                 "cellsize\t" + str(cellsize) + "\n" \
                 "NODATA_value\t" + str(no_data) + "\n"

        data = {
            "row-col-data": defaultdict(lambda: defaultdict(list)),
            "datacell-count": defaultdict(lambda: ncols),
            "datacell-col-count": defaultdict(lambda: defaultdict(lambda: -1)),
            "next-row": int(config["start-row"])
        }

        #debug_file = open("debug.out", "w")


    while not leave:

        try:
            result = socket.recv_json(encoding="latin-1")
        except:
            continue

        if result["type"] == "finish":
            print "received finish message"
            leave = True

        elif not write_normal_output_files:
            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            resolution = int(ci_parts[0])
            row = int(ci_parts[1])
            col = int(ci_parts[2])

            if "data" in result:
                debug_msg = "received work result " + str(received_env_count) + " customId: " + result.get("customId", "") \
                + " next row: " + str(data["next-row"]) + " cols@row to go: " + str(data["datacell-count"][row]) + "@" + str(row) #\
                #+ " rows unwritten: " + str(data["row-col-data"].keys()) 
                print debug_msg
                #debug_file.write(debug_msg + "\n")

                #data["row-col-data"][row][col] = create_output(result)
                data["row-col-data"][row][col].append(create_output(result))
            else if result.get("type", "") == "data-count":
                debug_msg = "received data-count result customId: " + result.get("customId", "") \
                + " next row: " + str(data["next-row"]) + " cols@row to go: " + str(data["datacell-count"][row]) + "@" + str(row)
                print debug_msg
                data["datacell-col-count"][row][col] += result.get("jobs-per-cell", 0) + 1
            else if result.get("type", "") == "no-data":
                debug_msg = "received no-data result customId: " + result.get("customId", "") \
                + " next row: " + str(data["next-row"]) + " cols@row to go: " + str(data["datacell-count"][row]) + "@" + str(row)
                print debug_msg
                data["row-col-data"][row][col] = -9999

            data["datacell-col-count"][row][col] -= 1
            if data["datacell-col-count"][row][col] == 0:
                data["datacell-count"][row] -= 1

            while data["next-row"] in data["row-col-data"] and data["datacell-count"][data["next-row"]] == 0:
                write_row_to_grids(data["row-col-data"], data["next-row"], ncols, header, paths["local-path-to-output-dir"])
                debug_msg = "wrote row: "  + str(data["next-row"]) + " next-row: " + str(data["next-row"]+1) + " rows unwritten: " + str(data["row-col-data"].keys())
                print debug_msg
                #debug_file.write(debug_msg + "\n")
                data["insert-nodata-rows-count"] = 0 # should have written the nodata rows for this period and 
                
                data["next-row"] += 1 # move to next row (to be written)

            received_env_count = received_env_count + 1

        elif write_normal_output_files:
            print "received work result ", received_env_count, " customId: ", result.get("customId", "")

            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            resolution = int(ci_parts[0])
            row = int(ci_parts[1])
            col = int(ci_parts[2])
            
            #with open("out/out-" + str(i) + ".csv", 'wb') as _:
            with open("out/out-" + file_name + ".csv", 'wb') as _:
                writer = csv.writer(_, delimiter=",")

                for data_ in result.get("data", []):
                    results = data_.get("results", [])
                    orig_spec = data_.get("origSpec", "")
                    output_ids = data_.get("outputIds", [])

                    if len(results) > 0:
                        writer.writerow([orig_spec.replace("\"", "")])
                        for row in monica_io.write_output_header_rows(output_ids,
                                                                      include_header_row=True,
                                                                      include_units_row=True,
                                                                      include_time_agg=False):
                            writer.writerow(row)

                        for row in monica_io.write_output(output_ids, results):
                            writer.writerow(row)

                    writer.writerow([])

            received_env_count = received_env_count + 1

    #debug_file.close()

main()

