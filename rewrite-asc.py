import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import numpy as np
from colour import Color
import sys
import os
from collections import defaultdict


def dem():

    in_asc = open("C:/Users/berg.ZALF-AD/Desktop/dem/dem_100_gk5_2.asc") 
    out_asc = open("C:/Users/berg.ZALF-AD/Desktop/dem/dem_100_gk5_3.asc", "w")

    for idx, line in enumerate(in_asc):
        if idx < 6:
            out_asc.write(line)
        else:
            cols = map(lambda col: "-9999" if col < -1000 else str(int(round(col))), np.fromstring(line, sep=" "))
            out_asc.write(" ".join(cols) + "\n")

        print idx,

    in_asc.close()
    out_asc.close()

def slope():

    in_asc = open("C:/Users/berg.ZALF-AD/Desktop/dem/slope_100_gk5.asc") 
    out_asc = open("C:/Users/berg.ZALF-AD/Desktop/dem/slope_100_gk5_2.asc", "w")

    for idx, line in enumerate(in_asc):
        if idx < 6:
            out_asc.write(line)
        else:
            cols = map(lambda col: "-9999" if col < -1000 else str(round(col, 2)), np.fromstring(line, sep=" "))
            out_asc.write(" ".join(cols) + "\n")

        print idx,

    in_asc.close()
    out_asc.close()

#dem()
slope()