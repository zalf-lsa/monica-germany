import os
import csv
from collections import defaultdict

filepath = "report/"
report_files = [x[2] for x in os.walk(filepath)][0]

best_cals = defaultdict(dict)

for fname in report_files:
    if "no_calibration" in fname:
        continue
    exp_id = int(fname.split(".")[0].split("_")[2].replace("exp", ""))
    calib_id = int(fname.split(".")[0].split("_")[3].replace("calib", ""))

    if exp_id not in best_cals.keys():
        best_cals[exp_id]["rrmse"] = 9999.9
        best_cals[exp_id]["cal_id"] = 9999

    with open(filepath + fname) as _:
        reader = csv.reader(_)
        reader.next()
        rrmse = float(reader.next()[1])
        if rrmse < best_cals[exp_id]["rrmse"]:
            best_cals[exp_id]["rrmse"] = rrmse
            best_cals[exp_id]["cal_id"] = calib_id

with open("best_cals.csv", "wb") as _:
    writer = csv.writer(_)
    header = ["exp_id", "best_cal"]
    writer.writerow(header)
    for exp, props in best_cals.iteritems():
        row =[exp, props["cal_id"]]
        writer.writerow(row)

print "done!"