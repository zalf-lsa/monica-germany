import os
import spotpy
from collections import defaultdict
import csv
import numpy as np

config = {
    "elevation-filter": -9999,
    "gwaterlevel-filter": 9999,
    "write_files_for_R": True,
    "disregard": {
        #solve problems with last sim year and winter crops (duplicated files, one filled of 0 values)
        "year": 2012,
        "cm_count": 15
    }
}

basepath = os.path.dirname(os.path.abspath(__file__))
main_out_dir = "Z:/projects/monica-germany/calibration-sensitivity-runs/2018-01-30-full/"#basepath + "/out/"
out_for_R_script = {}

def read_best_cals():
    with open(main_out_dir + "best_cals.csv") as _:
        reader =  csv.reader(_)
        reader.next()
        best_cals = {}
        for row in reader:
            exp_id = row[0]
            best_cal = row[1]
            best_cals[exp_id] = best_cal
        return best_cals

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

def calculate_indices(obs, sim):
    out = defaultdict(float)
    out["RRMSE"] = spotpy.objectivefunctions.rrmse(obs, sim)
    out["pBIAS"] = spotpy.objectivefunctions.pbias(obs, sim)
    out["NSE"] = spotpy.objectivefunctions.nashsutcliffe(obs, sim)
    out["AI"] = spotpy.objectivefunctions.agreementindex(obs, sim)
    out["r"] = spotpy.objectivefunctions.correlationcoefficient(obs, sim)
    out["r2"] = spotpy.objectivefunctions.rsquared(obs, sim)
    out["n_obs"] = len(obs)
    out["avg_obs"] = float(np.average(obs))
    out["avg_sim"] = float(np.average(sim))
    return out

def retrieve_data(ids, years, data, obs_sim):
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

def find_obs_years(id, data, yr_range):
    'returns a list of years for which observed yields are available in the lk or bkr'
    out = []
    for yr in data[id].keys():
        if yr in yr_range and "obs" in data[id][yr].keys():
            out.append(yr)
    return out

def check_obs_sim_length(obs, sim, entity):
    if len(obs) != len(sim):
        print("!!obs and sim lists of " + entity + " have different length!!")

best_cals = read_best_cals()
skip_folders = ["avg-agg-maps", "avg-maps"]

exp_dirs = [x[1] for x in os.walk(main_out_dir)][0]
for exp_folder in exp_dirs:
    if exp_folder in skip_folders:
        continue
    calib_dirs = [x[1] for x in os.walk(main_out_dir + exp_folder + "/")][0]
    for calib_folder in calib_dirs:
        #analyze only best calibs
        if exp_folder in best_cals.keys():
            if calib_folder != best_cals[exp_folder]:
                continue
        out_dir = main_out_dir + exp_folder + "/" + calib_folder + "/"
        print("starting the analysis of " + out_dir)

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
        with open("avg_elevation_latitude_gw_per_landkreis.csv") as _:
            reader = csv.reader(_)
            next(reader, None)
            for row in reader:
                lk = int(row[0])
                topography[lk]["lat"] = float(row[1])
                topography[lk]["elev"] = float(row[2])
                topography[lk]["gwl"] = float(row[2])

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
            #10= Zuckerruben (SBee)
            #11= Winterraps	(WRa)
            #12= Silomais (SM)

            for row in reader:
                if len(row)>2 and representsInt(row[1]):
                    lk = int(row[1])
                    #filter lks for elevation/groundwater leve
                    if topography[lk]["elev"] < config["elevation-filter"]:
                        continue
                    if topography[lk]["gwl"] > config["gwaterlevel-filter"]:
                        continue
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
        #exp_files = [x[2] for x in os.walk(out_dir + "no_calibration/aggregated/")][0]
        exp_files = [x[2] for x in os.walk(out_dir + "aggregated/")][0]
        for filename in exp_files:
            if ".csv" not in filename:
                continue #skip grids and pictures
            year = int(filename.split("_")[2])
            cm_count = int(filename.split("_")[3])            
            if year == config["disregard"]["year"] and cm_count == config["disregard"]["cm_count"]:
                continue #skip duplicated out files
            with open(out_dir + "aggregated/" + filename) as _:
                reader = csv.reader(_)
                next(reader, None)
                for row in reader:
                    lk = int(row[0])
                    sim_var = float(row[1])
                    if "yield" in filename:
                        yield_data[lk][year]["sim"] = sim_var
                    elif "max-LAI" in filename:
                        maxLAI_data[lk][year]["sim"] = sim_var
                
        #calculate indices
        indices_all_lks_x_yrs = defaultdict(float)
        all_obs = []
        all_sim =[]
        indices_lks = defaultdict(lambda: defaultdict(float)) #[index][lk] = value for all years
        other_var_lks =  defaultdict(lambda: defaultdict(float)) #[var][lk] = value for all years

        for lk in obs_lk:
            yrs = find_obs_years(lk, yield_data, all_years)
            obs = retrieve_data(ids=[lk], years=yrs, data=yield_data, obs_sim="obs")
            sim = retrieve_data(ids=[lk], years=yrs, data=yield_data, obs_sim="sim")
            check_obs_sim_length(obs, sim, "lk"+str(lk))
            all_obs += obs
            all_sim += sim
            indices = calculate_indices(obs, sim)
            for id in indices.keys():
                indices_lks[id][lk] = indices[id]
            sim_maxLAI = retrieve_data(ids=[lk], years=yrs, data=maxLAI_data, obs_sim="sim")
            other_var_lks["max-LAI"][lk] = float(np.average(sim_maxLAI))

        indices = calculate_indices(all_obs, all_sim)
        for id in indices.keys():
            indices_all_lks_x_yrs[id] = indices[id]
        out_for_R_script[int(exp_folder)] = indices_all_lks_x_yrs


        #write report
        report_name = "report_indices_exp" + exp_folder + "_calib" + calib_folder + ".csv" 
        with open("report/" + report_name, "wb") as _:
            writer = csv.writer(_)
            indices_header = ["RRMSE", "pBIAS", "NSE", "AI", "r", "r2", "n_obs", "avg_obs", "avg_sim"]
            
            #all lks
            header = ["all lks x all years"] + indices_header
            row= []
            row.append("")
            for id in indices_header:
                row.append(str(indices_all_lks_x_yrs[id]))
            writer.writerow(header)
            writer.writerow(row)

            #each lk
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
        print(report_name + " written!")

#produce csv for r script
if config["write_files_for_R"]:
    for exp in sorted(out_for_R_script.keys()):
        with open("report/" + "RRMSE.csv", "ab") as _:
            writer = csv.writer(_)
            writer.writerow([out_for_R_script[exp]["RRMSE"]])
        with open("report/" + "pBIAS.csv", "ab") as _:
            writer = csv.writer(_)
            writer.writerow([out_for_R_script[exp]["pBIAS"]])
        with open("report/" + "AI.csv", "ab") as _:
            writer = csv.writer(_)
            writer.writerow([out_for_R_script[exp]["AI"]])
        with open("report/" + "r.csv", "ab") as _:
            writer = csv.writer(_)
            writer.writerow([out_for_R_script[exp]["r"]])
    print("files for R script written!")

print("finished!")
exit()
#more stuff to be implemented below











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