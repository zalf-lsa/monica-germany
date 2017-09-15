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

import zmq
print "pyzmq version: ", zmq.pyzmq_version(), " zmq version: ", zmq.zmq_version()

import monica_io
#print "path to monica_io: ", monica_io.__file__

LOCAL_RUN = True #False

def create_output(cl_res, cl_row, cl_col, s_res, s_row, s_col, crop_id, period, gcm, result):
    "create crop output lines"

    out = []
    if len(result.get("data", [])) > 0 and len(result["data"][0].get("results", [])) > 0:
        year_to_vals = defaultdict(dict)

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

                if "Year" not in vals:
                    print "Missing Year in result section. Skipping results section."
                    continue

                #this filters out the last started (but not finished) cropping season for wheat
                if vals.get("DOY", 366) < 365 or crop_id == "M":
                    year_to_vals[vals.get("Year", 0)].update(vals)

        for year, vals in OrderedDict(sorted(year_to_vals.items())).iteritems():
            if len(vals) > 0: #and (crop_id == "W" or year > 1980):
                if cl_res <= s_res:
                    gridcell = "C" + str(cl_col) + ":R" + str(cl_row)
                else:
                    gridcell = "C" + str(s_col) + ":R" + str(s_row)

                out.append([
                    gridcell,
                    period,
                    gcm,
                    year,
                    vals.get("Yield", "nan") / 1000.0 if "Yield" in vals else "nan",
                    vals.get("Biom-ma", "nan") / 1000.0 if "Biom-ma" in vals else "nan",
                    vals.get("CumET", "nan"),
                    "nan", #PAR
                    vals.get("MaxLAI", "nan"),
                    vals.get("AntDOY", "nan"),
                    vals.get("MatDOY", "nan"),
                    vals.get("SOC-top", "nan"),
                    vals.get("SOC-profile", "nan"),
                    vals.get("NEE", "nan") / 10.0 if "NEE" in vals else "nan",
                    vals.get("NPP", "nan") / 10.0 if "NPP" in vals else "nan",
                    vals.get("N2O-crop", "nan"),
                    vals.get("N2O-year", "nan"),
                    vals.get("NLeach-year", "nan"),
                    vals.get("NLeach-crop", "nan"),
                    vals.get("WDrain-year", "nan"),
                    vals.get("WDrain-crop", "nan"),
                    vals.get("RootDep", "nan") / 10.0 if "RootDep" in vals else "nan",
                    #vals.get("OrgBiom", "nan") #for debugging purposes
                    #vals.get("Precip-crop", "nan"),
                    #vals.get("Precip-year", "nan"),
                ])
    else:
        with open("error.txt", "a") as _:
            _.write("climate: " + str(cl_res) + "/(" + str(cl_row) + "," + str(cl_col) + "), soil: " + str(s_res) + "/(" + str(s_row) + "," + str(s_col) + "), crop_id: " + crop_id + " period: " + period + " gcm: " + gcm + "\n")
            #_.write(json.dumps(result))

    return out


#+"Stage,HeatRed,RelDev,"\
HEADER = \
    "gridcell," \
    "Period," \
    "gcp_rcp," \
    "year," \
    "Yield (t DM/ha)," \
    "Total above ground biomass (t DM/ha)," \
    "Total ET over the growing season (mm/growing season)," \
    "Total intercepted PAR over the growing season (MJ/ha/growing season)," \
    "Maximum LAI during the growing season (m2/m2)," \
    "Anthesis date (DOY)," \
    "Maturity date (DOY)," \
    "SOC at sowing date 20 cm or 30 cm (gC/m2)," \
    "SOC at sowing date at 1.5 m (gC/m2)," \
    "Total net ecosystem exchange over the growing season (gC/m2/growing season)," \
    "Total net primary productivity over the growing season (gC/m2/growing season)," \
    "Total N20 over the growing season (kg N/ha/growing season)," \
    "Total annual N20 (kg N/ha/year)," \
    "Total annual N leaching over 1.5 m (kg N/ha/year)," \
    "Total N leaching over the growing season over 1.5 m (kg N/ha/growing season)," \
    "Total annual water loss below 1.5 m (mm/ha/year)," \
    "Total water loss below 1.5 m over the growing season (mm/ha/growing season)," \
    "Maximum rooted soil depth (m)" \
    "\n"


#overwrite_list = set()
def write_data(crop_id, production_situation, period, grcp, climate_resolution, soil_resolution, data):
    "write data"

    path_to_file = "out/" + crop_id + "_" + production_situation + "_P" + period + "_GRP" + grcp + "_c" + str(climate_resolution) + "xs" + str(soil_resolution) + ".csv"

    if not os.path.isfile(path_to_file):# or (row, col) not in overwrite_list:
        with open(path_to_file, "w") as _:
            _.write(HEADER)
        #overwrite_list.add((row, col))

    with open(path_to_file, 'ab') as _:
        writer = csv.writer(_, delimiter=",")
        for row_ in data[(crop_id, production_situation, period, grcp, climate_resolution, soil_resolution)]:
            writer.writerow(row_)
        data[(crop_id, production_situation, period, grcp, climate_resolution, soil_resolution)] = []




def main():
    "collect data from workers"

    data = defaultdict(list)

    i = 1
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    if LOCAL_RUN:
        socket.connect("tcp://localhost:7777")
    else:
        socket.connect("tcp://cluster2:7777")
    socket.RCVTIMEO = 1000
    leave = False
    write_normal_output_files = False
    start_writing_lines_threshold = 270
    while not leave:

        try:
            #result = socket.recv_json()
            result = socket.recv_json(encoding="latin-1")
            #result = socket.recv_string(encoding="latin-1")
            #result = socket.recv_string()
            #print result
            #with open("out/out-latin1.csv", "w") as _:
            #    _.write(result)
            #continue
        except:
            for crop_id, production_situation, period, grcp, climate_resolution, soil_resolution in data.keys():
                if len(data[(crop_id, production_situation, period, grcp, climate_resolution, soil_resolution)]) > 0:
                    write_data(crop_id, production_situation, period, grcp, climate_resolution, soil_resolution, data)
            continue

        if result["type"] == "finish":
            print "received finish message"
            leave = True

        elif not write_normal_output_files:
            print "received work result ", i, " customId: ", result.get("customId", "")

            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            crop_id = ci_parts[0]
            climate_resolution = int(ci_parts[1])
            cl_row_, cl_col_ = ci_parts[2][1:-1].split("/")
            cl_row, cl_col = (int(cl_row_), int(cl_col_))
            soil_resolution = int(ci_parts[3])
            s_row_, s_col_ = ci_parts[4][1:-1].split("/")
            s_row, s_col = (int(s_row_), int(s_col_))
            period = ci_parts[5]
            grcp = ci_parts[6]
            gcm = ci_parts[7]
            production_situation = ci_parts[8]

            res = create_output(climate_resolution, cl_row, cl_col, soil_resolution, s_row, s_col, crop_id, period, gcm, result)
            data[(crop_id, production_situation, period, grcp, climate_resolution, soil_resolution)].extend(res)

            if len(data[(crop_id, production_situation, period, grcp, climate_resolution, soil_resolution)]) >= start_writing_lines_threshold:
                write_data(crop_id, production_situation, period, grcp, climate_resolution, soil_resolution, data)

            i = i + 1

        elif write_normal_output_files:
            print "received work result ", i, " customId: ", result.get("customId", "")

            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            crop_id = ci_parts[0]
            climate_resolution = ci_parts[1]
            cl_row_, cl_col_ = ci_parts[2][1:-1].split("/")
            soil_resolution = ci_parts[3]
            s_row_, s_col_ = ci_parts[4][1:-1].split("/")
            period = ci_parts[5]
            grcp = ci_parts[6]
            production_situation = ci_parts[8]
            file_name = crop_id + "_" + production_situation + "_P" + period + "_GRP" + grcp + "_cr" + climate_resolution + "c"+ cl_col_ + "r"+ cl_row_ + "xsr" + soil_resolution + "c"+ s_col_ + "r"+ s_row_
            

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

            i = i + 1


main()


