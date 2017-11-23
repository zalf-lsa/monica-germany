import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import numpy as np
from colour import Color
import sys
import os
from collections import defaultdict



def create_png(config, path_to_file, write_legend_file=False):

    img = np.full((1746, 1286, 3), 1, dtype=np.float32)

    arr = np.loadtxt(path_to_file, skiprows=6)

    data = np.array(filter(lambda x: x != -9999, arr.flat))
    #sorted_without_nodata = sorted(without_nodata)
    #lower_bound = int(len(sorted_without_nodata) * 0.01)
    #print "lower-bound:", lower_bound
    #sorted_without_nodata_90 = sorted_without_nodata[lower_bound:]

    median = np.median(data)
    upper_quartile = np.percentile(data, 75)
    lower_quartile = np.percentile(data, 25)

    iqr = upper_quartile - lower_quartile
    upper_whisker = data[data<=upper_quartile+float(config["times-iqr"])*iqr].max()
    lower_whisker = data[data>=lower_quartile-float(config["times-iqr"])*iqr].min()

    print "median:", median, "upper-quartile:", upper_quartile, "lower-quartile:", lower_quartile, "iqr:", iqr,
    "upper-whisker:", upper_whisker, "lower-whisker:", lower_whisker

    min_val = float(config["abs-min"]) if config["abs-min"] else lower_whisker #min(sorted_without_nodata_90)# without_nodata)
    max_val = float(config["abs-max"]) if config["abs-max"] else upper_whisker #max(sorted_without_nodata_90)#without_nodata)
    print "min:", min_val, "max:", max_val

    val_range = max_val - min_val

    colors = list(Color(config["from-color"]).range_to(Color(config["over-color"]), int(val_range//2) + 1))
    colors.extend(Color(config["over-color"]).range_to(Color(config["to-color"]), int(val_range//2) + 1))

    print int(val_range//2)+1, int(val_range//2)+1, len(colors)

    for row in range(arr.shape[0]):
        for col in range(arr.shape[1]):
            val = arr[row, col]
            if int(val) != -9999:
                if val < min_val:
                    rgb = Color(config["lower-outlier-color"]).rgb
                elif val > max_val:
                    rgb = Color(config["upper-outlier-color"]).rgb
                else:
                    try:
                        rgb = colors[int(val - min_val)].rgb
                    except:
                        print val, int(val - min_val)
                img[row, col] = rgb

    mpimg.imsave(path_to_file[:-3] + "png", img)

    print "created", path_to_file[:-3] + "png"

    #imgplot = plt.imshow(img)
    #plt.show()

    if write_legend_file:
        legend_str = """
  -----   {}\t <- upper-whisker = max(upper-quartile + {}*iqr)
    |
|-------| {}\t <- upper-quartile
|       |
|-------| {}\t <- median | iqr -> {}
|       |
|-------| {}\t <- lower-quartile
    |
  -----   {}\t <- lower-whisker = min(lower-quartile - {}*iqr)
"""

        with open(path_to_file[:-4] + "_legend.txt", "w") as _:
            _.write(legend_str.format(upper_whisker, float(config["times-iqr"]), \
            upper_quartile, median, iqr, lower_quartile, lower_whisker, float(config["times-iqr"])))

def create_avg_grid(path_to_dir):

    acc_arrs = defaultdict(lambda: {"arr": None, "count": 0, "filename": "", "header": ""})
    arr_counts = {}

    for filename in sorted(os.listdir(path_to_dir)):
        if filename[-3:] == "asc":
            id = "_".join(filename.split("_")[:2])
            arr = np.loadtxt(path_to_dir + filename, skiprows=6)

            if id not in acc_arrs:
                acc_arrs[id]["arr"] = np.full(arr.shape, 0.0, arr.dtype)
                acc_arrs[id]["filename"] = id + "_avg.asc"
                with open(path_to_dir + filename) as _:
                    for i in range(6):
                        acc_arrs[id]["header"] += _.readline()

            acc_arrs[id]["arr"] += arr
            acc_arrs[id]["count"] += 1
            print "added:", filename


    for id, acc_arr in acc_arrs.iteritems():
        acc_arr["arr"] /= acc_arr["count"]
        np.savetxt(path_to_dir + acc_arr["filename"], acc_arr["arr"], header=acc_arr["header"], delimiter=" ", comments="", fmt="%.1f")
        print "wrote:", acc_arr["filename"]


def main():

    config = {
        "dir": "out/", #"P:/monica-germany/landkreise-avgs/", #"out/",
        "file": "",

        "from-color": "red",
        "over-color": "yellow",
        "to-color": "green",
        "lower-outlier-color": "purple",
        "upper-outlier-color": "cyan",
        
        "abs-min": "",
        "abs-max": "",

        "times-iqr": "4", #1.5,
        "write-legends": True
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v

    create_avg_grid(config["dir"])
    exit()

    for filename in os.listdir(config["dir"]):
        if (config["file"] and filename == config["file"]) \
        or (not config["file"] and filename[-3:] == "asc"):
            create_png(config, config["dir"]+filename, 
            not (config["abs-min"] and config["abs-max"]) 
            or bool(config["write-legends"]))

main()


    
#mpimg.imsave("out/rapewinterrape_Yield_1996_1.png", img)



legend_str = """
  -----   {}\t <- upper-whisker (upper)
    |
|-------| {}\t <- upper-quartile
|       |
|-------| {}\t <- median | iqr -> {}
|       |
|-------| {}\t <- lower quartile
    |
  -----   {}\t <- lower whisker
"""

#with open("out/img-legend.txt", "w") as _:
#    _.write(legend_str.format(upper_whisker, upper_quartile, median, iqr, lower_quartile, lower_whisker))
