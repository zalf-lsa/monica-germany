import os
import spotpy
from collections import defaultdict
import csv
import numpy as np

basepath = os.path.dirname(os.path.abspath(__file__))
out_dir = basepath + "/out/"

def walklevel(some_dir, level=1):
    some_dir = some_dir.rstrip(os.path.sep)
    assert os.path.isdir(some_dir)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(os.path.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]

def representsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False

def representsfloat(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

#associate landkreise (lk) to pedoclimatic regions (bkr)
lk_2_bkr = defaultdict(set)
bkr_2_lk = defaultdict(set)
sim_lk = set()

with open("landkreise_bkrs.csv") as _:
    reader = csv.reader(_)
    next(reader, None)
    for row in reader:
        lk = int(row[0])
        for i in range(1, len(row)):
            if representsInt(row[i]):
                bkr = int(row[i])
                lk_2_bkr[lk].add(bkr)
                bkr_2_lk[bkr].add(lk)
                sim_lk.add(lk)

#read lk elevation/latitude
topography = defaultdict(lambda: defaultdict(float))
with open("avg_elevation_latitude_per_landkreis.csv") as _:
    reader = csv.reader(_)
    next(reader, None)
    for row in reader:
        lk = int(row[0])
        topography[lk]["lat"] = float(row[1])
        topography[lk]["elev"] = float(row[2])

#data[bkr/lk][year][sim/obs]
yield_data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
maxLAI_data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
#all_years = set()
all_years = range(1999, 2013) #both obs and sims available for these years!

#read official yields
obs_lk = set()
obs_bkr = set()
with open("official_yields_DE.csv") as _:
    reader = csv.reader(_)
    for i in range(7):
        next(reader, None)
    
    #3= Winterweizen (WW)
    #4=	Roggen und Wintermenggetreide (RY)
    #5= Wintergerste (WB)
    #6= Sommergerste (SB)
    #7= Hafer (OA)
    #8=	Triticale (TR)
    #9=	Kartoffeln (PO)
    #10= Zuckerrüben (SBee)
    #11= Winterraps	(WRa)
    #12= Silomais (SM)

    for row in reader:
        if len(row)>2 and representsInt(row[1]):
            lk = int(row[1])
            if lk in sim_lk:
                year = int(row[0])
                #all_years.add(year)
                if representsfloat(row[3]):
                    if float(row[3]) == 0.0:
                        #consider 0 as nodata
                        continue
                    obs_lk.add(lk)
                    for bkr in lk_2_bkr[lk]:
                        obs_bkr.add(bkr)
                    obs_yield = float(row[3]) * 100 #kg ha-1
                    yield_data[lk][year]["obs"] = obs_yield

#print len(obs_lk)

#read output
for root, dirs, filenames in walklevel(out_dir, level=1):
    for filename in filenames:
        year = int(filename.split("_")[2])
        with open(out_dir + filename) as _:
            reader = csv.reader(_)
            next(reader, None)
            for row in reader:
                lk = int(row[0])
                sim_var = float(row[1])
                if "yield" in filename:
                    yield_data[lk][year]["sim"] = sim_var
                elif "max-LAI" in filename:
                    maxLAI_data[lk][year]["sim"] = sim_var

#aggregate to bkr, TODO weighted average
for bkr, lks in bkr_2_lk.iteritems():
    sims = []
    obs = []
    for year in all_years:
        for lk in lks:
            if "obs" in yield_data[lk][year].keys():
                sims.append(yield_data[lk][year]["sim"])
                obs.append(yield_data[lk][year]["obs"])
        avg_sim = float(np.average(sims))
        avg_obs = float(np.average(obs))
        yield_data[bkr][year]["sim"] = avg_sim
        yield_data[bkr][year]["obs"] = avg_obs


#calculate indices
def calculate_indices(obs, sim):
    out = defaultdict(float)
    out["RRMSE"] = spotpy.objectivefunctions.rrmse(obs, sim)
    out["pBIAS"] = spotpy.objectivefunctions.pbias(obs, sim)
    out["NSE"] = spotpy.objectivefunctions.nashsutcliff(obs, sim)
    out["AI"] = spotpy.objectivefunctions.agreementindex(obs, sim)
    out["r"] = spotpy.objectivefunctions.correlationcoefficient(obs, sim)
    out["r2"] = spotpy.objectivefunctions.rsquared(obs, sim)
    out["n_obs"] = len(obs)
    out["avg_obs"] = float(np.average(obs))
    out["avg_sim"] = float(np.average(sim))
    return out

def retrieve_data(ids, years, data=yield_data, obs_sim=""):
    'returns data from selected bkrs/lks (i.e., ids) and years. obs_sim must be either "obs" or "sim"'
    out = []
    for id in ids:
        if id not in data.keys():
            continue
        for yr in years:
            if yr not in data[id].keys():
                continue
            if obs_sim not in data[id][yr].keys():
                continue
            out.append(data[id][yr][obs_sim])
            #print(data[id][yr][obs_sim], id, yr, obs_sim)
    return out

def find_obs_years(id, data=yield_data, yr_range=all_years):
    'returns a list of years for which observed yields are available in the lk or bkr'
    out = []
    for yr in data[id].keys():
        if yr in yr_range and "obs" in data[id][yr].keys():
            out.append(yr)
    return out

indices_lks = defaultdict(lambda: defaultdict(float)) #[index][lk] = value for all years
other_var_lks =  defaultdict(lambda: defaultdict(float)) #[var][lk] = value for all years
for lk in obs_lk:
    yrs = find_obs_years(lk)
    obs = retrieve_data(ids=[lk], years=yrs, obs_sim="obs")
    sim = retrieve_data(ids=[lk], years=yrs, obs_sim="sim")
    indices = calculate_indices(obs, sim)
    for id in indices.keys():
        indices_lks[id][lk] = indices[id]
    sim_maxLAI = retrieve_data(ids=[lk], years=yrs, data=maxLAI_data, obs_sim="sim")
    other_var_lks["max-LAI"][lk] = float(np.average(sim_maxLAI))


#write report
with open("report_indices.csv", "wb") as _:
    writer = csv.writer(_)
    indices_header = ["RRMSE", "pBIAS", "NSE", "AI", "r", "r2", "n_obs", "avg_obs", "avg_sim"]
    
    header = ["lk x all years"] + indices_header + ["latitude", "elevation", "max-LAI"]
    writer.writerow(header)
    for lk in obs_lk:
        if indices_lks["n_obs"][lk] < 3:
            continue
        row=[lk]
        for id in indices_header:
            row.append(str(indices_lks[id][lk]))
        row.append(topography[lk]["lat"])
        row.append(topography[lk]["elev"])
        row.append(str(other_var_lks["max-LAI"][lk]))
        writer.writerow(row)

print("finished!")
exit()
#more stuff to be implemented below



















indices_bkrs = defaultdict(lambda: defaultdict(float)) #[index][bkr] = value for all years
for bkr in obs_bkr:
    yrs = find_obs_years(bkr)
    obs = retrieve_yield_data(ids=[bkr], years=yrs, obs_sim="obs")
    sim = retrieve_yield_data(ids=[bkr], years=yrs, obs_sim="sim")
    indices = calculate_indices(obs, sim)
    for id in indices.keys():
        indices_bkrs[id][bkr] = indices[id]

#the following 2 indices need to be checked again: not all the years might be there
indices_years_bkr = defaultdict(lambda: defaultdict(float)) #[index][year] = value for all bkrs
for yr in all_years:
    obs = retrieve_yield_data(ids=obs_bkr, years=[yr], obs_sim="obs")
    sim = retrieve_yield_data(ids=obs_bkr, years=[yr], obs_sim="sim")
    indices = calculate_indices(obs, sim)
    for id in indices.keys():
        indices_years_bkr[id][yr] = indices[id]


indices_years_lk = defaultdict(lambda: defaultdict(float)) #[index][year] = value for all lks
for yr in all_years:
    obs = retrieve_yield_data(ids=obs_lk, years=[yr], obs_sim="obs")
    sim = retrieve_yield_data(ids=obs_lk, years=[yr], obs_sim="sim")
    indices = calculate_indices(obs, sim)
    for id in indices.keys():
        indices_years_lk[id][yr] = indices[id]


indices_all = defaultdict(lambda: defaultdict(float)) #[index]["bkrs"/"lks"] = value for all bkrs/lks and years
#bkrs
obs = retrieve_yield_data(ids=obs_bkr, years=all_years, obs_sim="obs")
sim = retrieve_yield_data(ids=obs_bkr, years=all_years, obs_sim="sim")
indices = calculate_indices(obs, sim)
for id in indices.keys():
    indices_all[id]["bkrs"] = indices[id]
#lks
obs = retrieve_yield_data(ids=obs_lk, years=all_years, obs_sim="obs")
sim = retrieve_yield_data(ids=obs_lk, years=all_years, obs_sim="sim")
indices = calculate_indices(obs, sim)
for id in indices.keys():
    indices_all[id]["lks"] = indices[id]

#write report
with open("report_indices.csv", "wb") as _:
    writer = csv.writer(_)
    indices_header = ["RRMSE", "pBIAS", "NSE", "AI", "r", "r2"]
        
    header = ["allbkrs x all years"] + indices_header
    writer.writerow(header)
    row=[""]
    for id in indices_header:
        row.append(str(indices_all[id]["bkrs"]))
    writer.writerow(row)

    writer.writerow([])

    header = ["alllks x all years"] + indices_header
    writer.writerow(header)
    row=[""]
    for id in indices_header:
        row.append(str(indices_all[id]["lks"]))
    writer.writerow(row)

    writer.writerow([])

    header = ["bkr x all years"] + indices_header
    writer.writerow(header)
    for bkr in obs_bkr:
        row=[bkr]
        for id in indices_header:
            row.append(str(indices_bkrs[id][bkr]))
        writer.writerow(row)

    writer.writerow([])

    header = ["lk x all years"] + indices_header + ["latitude", "elevation"]
    writer.writerow(header)
    for lk in obs_lk:
        row=[lk]
        for id in indices_header:
            row.append(str(indices_lks[id][lk]))
        row.append(topography[lk]["lat"])
        row.append(topography[lk]["elev"])
        writer.writerow(row)

    writer.writerow([])

    header = ["year x all bkrs"] + indices_header
    writer.writerow(header)
    for yr in all_years:
        row=[yr]
        for id in indices_header:
            row.append(str(indices_years_bkr[id][yr]))
        writer.writerow(row)
    
    writer.writerow([])

    header = ["year x all lks"] + indices_header
    writer.writerow(header)
    for yr in all_years:
        row=[yr]
        for id in indices_header:
            row.append(str(indices_years_lk[id][yr]))
        writer.writerow(row)

print("finished!!!")