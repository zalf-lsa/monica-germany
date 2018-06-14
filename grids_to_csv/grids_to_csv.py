import numpy as np
import os
from collections import defaultdict
import csv

basepath = os.path.dirname(os.path.abspath(__file__))
input_dir = basepath + "/input/"

O3_level = 75#25 #75
CO2_level = 680#380 #680

# keys: 
# (row, col)
# year
# variable name
all_data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
all_vars = set()
all_yrs = set()

#read in available data
for filename in os.listdir(input_dir):    
    print("reading " + filename)
    info_file = filename.split("_")
    my_var = info_file[1]
    my_year = int(info_file[2])

    all_vars.add(my_var)
    all_yrs.add(my_year)
    
    my_grid = np.loadtxt(input_dir + filename, dtype=float, skiprows=6)

    rows = my_grid.shape[0]
    cols = my_grid.shape[1]
    for x in range(0, rows):
        for y in range(0, cols):
            val = my_grid[x,y]
            if val != -9999:
                coord = (x, y)
                all_data[coord][my_year][my_var] = val

#write csv out
print("writing out file")
with open(basepath + "/converted.csv", "wb") as _:
    writer = csv.writer(_)
    
    header = []
    header.append("cell")
    header.append("year")
    header.append("CO2")
    header.append("O3")
    for var in all_vars:
        header.append(var)
    writer.writerow(header)

    for coord in all_data.keys():
        for year in all_yrs:
            line = []
            cell = str(coord[0]).zfill(3) + "_" + str(coord[1]).zfill(3)
            line.append(cell)
            line.append(year)
            line.append(CO2_level)
            line.append(O3_level)
            for var in all_vars:
                line.append(all_data[coord][year].get(var, "NA"))
            writer.writerow(line)

print("finished!")