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
#print sys.path

import gc
import csv
import types
import os
import json
from datetime import datetime
from collections import defaultdict, OrderedDict
import numpy as np

import zmq
#print "pyzmq version: ", zmq.pyzmq_version(), " zmq version: ", zmq.zmq_version()

import monica_io
#print "path to monica_io: ", monica_io.__file__

LOCAL_RUN = False

PATHS = {
    "berg-lc": {
        "local-path-to-output-dir": "out/",
        "local-path-to-csv-output-dir": "csv-out/"
    },
    "berg-xps15": {
        "local-path-to-output-dir": "out/",
        "local-path-to-csv-output-dir": "csv-out/"
    },
    "stella": {
        "local-path-to-output-dir": "out/",
        "local-path-to-csv-output-dir": "csv-out/"
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

                if "CM-count" not in vals:
                    print "Missing CM-count in result section. Skipping results section."
                    continue

                cm_count_to_vals[vals["CM-count"]].update(vals)
    
    for cmc in sorted(cm_count_to_vals.keys()):
        if cm_count_to_vals[cmc]["last-doy"] >= 365:
            del cm_count_to_vals[cmc]

    return cm_count_to_vals


def write_row_to_grids(row_col_data, row, ncols, header, path_to_output_dir, path_to_csv_output_dir):
    "write grids row by row"

    is_data_row = len(filter(lambda x: x != -9999, row_col_data[row].values())) > 0
    if row in row_col_data and is_data_row:
        path_to_row_file = path_to_csv_output_dir + "row-" + str(row) + ".csv" 

        if not os.path.isfile(path_to_row_file):
            with open(path_to_row_file, "w") as _:
                _.write("CM-count,row,col,Crop,Year,Globrad,Tmax,Tmin,Tavg,Precip,LAImax,AbBiom,G-iso,G-mono,cycle-length,\
Globrad2,Tmax2,Tmin2,Tavg2,Precip2,LAI2,AbBiom2,G-iso2,G-mono2,length-S2,\
Globrad3,Tmax3,Tmin3,Tavg3,Precip3,LAI3,AbBiom3,G-iso3,G-mono3,length-S3,\
Globrad4,Tmax4,Tmin4,Tavg4,Precip4,LAI4,AbBiom4,G-iso4,G-mono4,length-S4,\
Globrad5,Tmax5,Tmin5,Tavg5,Precip5,LAI5,AbBiom5,G-iso5,G-mono5,length-S5,\
Globrad6,Tmax6,Tmin6,Tavg6,Precip6,LAI6,AbBiom6,G-iso6,G-mono6,length-S6\n")

        with open(path_to_row_file, 'ab') as _:
            writer = csv.writer(_, delimiter=",")

            for col in xrange(0, ncols):
                if col in row_col_data[row]:
                    rcd_val = row_col_data[row][col]
                    if rcd_val != -9999 and len(rcd_val) > 0:
                        cell_data = rcd_val[0]

                        for cm_count, data in cell_data.iteritems():
                            row_ = [
                                cm_count,
                                row,
                                col,
                                data["Crop"],
                                data["Year"],
                                data["Globrad"],
                                data["Tmax"],
                                data["Tmin"],
                                data["Tavg"],
                                data["Precip"],
                                data["LAImax"],
                                data["AbBiom"],
                                data["G-iso"],
                                data["G-mono"],
                                data["cycle-length"],
                                
                                data.get("Globrad2", "NA"),
                                data.get("Tmax2", "NA"),
                                data.get("Tmin2", "NA"),
                                data.get("Tavg2", "NA"),
                                data.get("Precip2", "NA"),
                                data.get("LAI2", "NA"),
                                data.get("AbBiom2", "NA"),
                                data.get("G-iso2", "NA"),
                                data.get("G-mono2", "NA"),
                                data.get("length-S2", "NA"),

                                data.get("Globrad3", "NA"),
                                data.get("Tmax3", "NA"),
                                data.get("Tmin3", "NA"),
                                data.get("Tavg3", "NA"),
                                data.get("Precip3", "NA"),
                                data.get("LAI3", "NA"),
                                data.get("AbBiom3", "NA"),
                                data.get("G-iso3", "NA"),
                                data.get("G-mono3", "NA"),
                                data.get("length-S3", "NA"),

                                data.get("Globrad4", "NA"),
                                data.get("Tmax4", "NA"),
                                data.get("Tmin4", "NA"),
                                data.get("Tavg4", "NA"),
                                data.get("Precip4", "NA"),
                                data.get("LAI4", "NA"),
                                data.get("AbBiom4", "NA"),
                                data.get("G-iso4", "NA"),
                                data.get("G-mono4", "NA"),
                                data.get("length-S4", "NA"),

                                data.get("Globrad5", "NA"),
                                data.get("Tmax5", "NA"),
                                data.get("Tmin5", "NA"),
                                data.get("Tavg5", "NA"),
                                data.get("Precip5", "NA"),
                                data.get("LAI5", "NA"),
                                data.get("AbBiom5", "NA"),
                                data.get("G-iso5", "NA"),
                                data.get("G-mono5", "NA"),
                                data.get("length-S5", "NA"),

                                data.get("Globrad6", "NA"),
                                data.get("Tmax6", "NA"),
                                data.get("Tmin6", "NA"),
                                data.get("Tavg6", "NA"),
                                data.get("Precip6", "NA"),
                                data.get("LAI6", "NA"),
                                data.get("AbBiom6", "NA"),
                                data.get("G-iso6", "NA"),
                                data.get("G-mono6", "NA"),
                                data.get("length-S6", "NA")
                            ]
                            writer.writerow(row_)


    if not hasattr(write_row_to_grids, "nodata_row_count"):
        write_row_to_grids.nodata_row_count = 0
        write_row_to_grids.list_of_output_files = []

    make_dict_nparr = lambda: defaultdict(lambda: np.full((ncols,), -9999, dtype=np.float))

    output_grids = {
        "G-iso": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "G-mono": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "G-iso2": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "G-mono2": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "length-S2": {"data" : make_dict_nparr(), "cast-to": "int"},
        "G-iso3": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "G-mono3": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "length-S3": {"data" : make_dict_nparr(), "cast-to": "int"},
        "G-iso4": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "G-mono4": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "length-S4": {"data" : make_dict_nparr(), "cast-to": "int"},
        "G-iso5": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "G-mono5": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "length-S5": {"data" : make_dict_nparr(), "cast-to": "int"},
        "G-iso6": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "G-mono6": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "length-S6": {"data" : make_dict_nparr(), "cast-to": "int"},
        "LAImax": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "Tavg": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "Globrad": {"data" : make_dict_nparr(), "cast-to": "float", "digits": 1},
        "cycle-length": {"data" : make_dict_nparr(), "cast-to": "int"}
    }

    cmc_to_crop = {}

    #is_no_data_row = True
    # skip this part if we write just a nodata line
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

                                if key in data:
                                    cmc_and_year_to_vals[(cm_count, data["Year"])][key].append(data[key])
                                else:
                                    cmc_and_year_to_vals[(cm_count, data["Year"])][key] #just make sure at least an empty list is in there

                    for (cm_count, year), key_to_vals in cmc_and_year_to_vals.iteritems():
                        for key, vals in key_to_vals.iteritems():
                            output_vals = output_grids[key]["data"]
                            if len(vals) > 0:
                                output_vals[(cm_count, year)][col] = sum(vals) / len(vals)
                            else:
                                output_vals[(cm_count, year)][col] = -9999
                                #no_data_cols += 1

        is_no_data_row = no_data_cols == ncols
        if is_no_data_row:
            write_row_to_grids.nodata_row_count += 1

    def write_nodata_rows(file_):
        for _ in range(write_row_to_grids.nodata_row_count):
            rowstr = " ".join(["-9999" for __ in range(ncols)])
            file_.write(rowstr +  "\n")

    for key, y2d_ in output_grids.iteritems():

        y2d = y2d_["data"]
        cast_to = y2d_["cast-to"]
        digits = y2d_.get("digits", 0)
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
                    write_row_to_grids.list_of_output_files.append(path_to_file)

            with open(path_to_file, "a") as file_:
                write_nodata_rows(file_)
                rowstr = " ".join(["-9999" if int(x) == -9999 else mold(x) for x in row_arr])
                file_.write(rowstr +  "\n")

    # if we're at the end of the output and just empty lines are left, then they won't be written in the
    # above manner because there won't be any rows with data where they could be written before
    # so add no-data rows simply to all files we've written to before
    if is_no_data_row and write_row_to_grids.list_of_output_files:
        for path_to_file in write_row_to_grids.list_of_output_files:
            with open(path_to_file, "a") as file_:
                write_nodata_rows(file_)
        write_row_to_grids.nodata_row_count = 0
    
    # clear the no-data row count when no-data rows have been written before a data row
    if not is_no_data_row:
        write_row_to_grids.nodata_row_count = 0

    if row in row_col_data:
        del row_col_data[row]


def run_consumer(path_to_output_dir = None, leave_after_finished_run = True, server = {"server": None, "port": None, "nd-port": None}):
    "collect data from workers"

    config = {
        "user": "stella", # "berg-lc",
        "port": server["port"] if server["port"] else "77773",
        "no-data-port": server["nd-port"] if server["nd-port"] else "5555",
        "server": server["server"] if server["server"] else "cluster3", #"localhost", 
        "start-row": "0",
        "end-row": "-1"
    }
    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v

    paths = PATHS[config["user"]]
    if path_to_output_dir:
        paths["local-path-to-output-dir"] = path_to_output_dir

    try:
        os.makedirs(paths["local-path-to-output-dir"])
    except:
        pass

    data = defaultdict(list)

    print "consumer config:", config

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
            
        start_row = int(config["start-row"])
        end_row = int(config["end-row"])    
        nrows = end_row - start_row + 1 if start_row > 0 and end_row >= start_row else int(msg["nrows"])
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
            "jobs-per-cell-count": defaultdict(lambda: defaultdict(lambda: -1)),
            "next-row": start_row
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
            crow = int(ci_parts[3])
            ccol = int(ci_parts[4])
            soil_id = int(ci_parts[5])

            if result.get("type", "") == "jobs-per-cell":
                debug_msg = "received jobs-per-cell message count: " + str(result["count"]) + " customId: " + result.get("customId", "") \
                + " next row: " + str(data["next-row"]) + " jobs@col to go: " + str(data["jobs-per-cell-count"][row][col]) + "@" + str(col) \
                + " cols@row to go: " + str(data["datacell-count"][row]) + "@" + str(row)
                #print debug_msg
                #debug_file.write(debug_msg + "\n")
                data["jobs-per-cell-count"][row][col] += 1 + result["count"]
                #print "--> jobs@row/col: " + str(data["jobs-per-cell-count"][row][col]) + "@" + str(row) + "/" + str(col)
            elif result.get("type", "") == "no-data":
                debug_msg = "received no-data message customId: " + result.get("customId", "") \
                + " next row: " + str(data["next-row"]) + " jobs@col to go: " + str(data["jobs-per-cell-count"][row][col]) + "@" + str(col) \
                + " cols@row to go: " + str(data["datacell-count"][row]) + "@" + str(row)
                #print debug_msg
                #debug_file.write(debug_msg + "\n")
                data["row-col-data"][row][col] = -9999
                data["jobs-per-cell-count"][row][col] = 0
            elif "data" in result:
                debug_msg = "received work result " + str(received_env_count) + " customId: " + result.get("customId", "") \
                + " next row: " + str(data["next-row"]) + " jobs@col to go: " + str(data["jobs-per-cell-count"][row][col]) + "@" + str(col) \
                + " cols@row to go: " + str(data["datacell-count"][row]) + "@" + str(row) #\
                #+ " rows unwritten: " + str(data["row-col-data"].keys()) 
                #print debug_msg
                #debug_file.write(debug_msg + "\n")
                data["row-col-data"][row][col].append(create_output(result))
                data["jobs-per-cell-count"][row][col] -= 1

                received_env_count = received_env_count + 1

            if data["jobs-per-cell-count"][row][col] == 0:
                data["datacell-count"][row] -= 1

            while data["next-row"] in data["row-col-data"] and data["datacell-count"][data["next-row"]] == 0:
                write_row_to_grids(data["row-col-data"], data["next-row"], ncols, header, paths["local-path-to-output-dir"], paths["local-path-to-csv-output-dir"])
                debug_msg = "wrote row: "  + str(data["next-row"]) + " next-row: " + str(data["next-row"]+1) + " rows unwritten: " + str(data["row-col-data"].keys())
                print debug_msg
                #debug_file.write(debug_msg + "\n")
                data["insert-nodata-rows-count"] = 0 # should have written the nodata rows for this period and 
                
                data["next-row"] += 1 # move to next row (to be written)

                if leave_after_finished_run and ((end_row < 0 and data["next-row"] > nrows-1) or (end_row >= 0 and data["next-row"] > end_row)): 
                    print "all results received, exiting"
                    leave = True
                    break
                
        elif write_normal_output_files:

            if result.get("type", "") in ["jobs-per-cell", "no-data", "target-grid-metadata"]:
                print "ignoring", result.get("type", "")
                continue

            print "received work result ", received_env_count, " customId: ", result.get("customId", "")

            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            resolution = int(ci_parts[0])
            row = int(ci_parts[1])
            col = int(ci_parts[2])
            crow = int(ci_parts[3])
            ccol = int(ci_parts[4])
            soil_id = int(ci_parts[5])
            
            #with open("out/out-" + str(i) + ".csv", 'wb') as _:
            with open("out-normal/out-" + custom_id.replace("|", "_") + ".csv", 'wb') as _:
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

    print "exiting run_consumer()"
    #debug_file.close()

if __name__ == "__main__":
    run_consumer()
#main()


