# Load the package required to read JSON files.
library(rjson)
library(jsonlite)
library(rzmq)
library(readr)
library(stringr)

context = init.context()
socket = init.socket(context,"ZMQ_REQ")
connect.socket(socket,"tcp://localhost:6666")

setwd("C:/Users/berg.ZALF-AD/GitHub/monica/installer/Hohenfinow2")

if(!exists("create_env_json_from_json_config", mode="function")) 
  source("C:/Users/berg.ZALF-AD/GitHub/monica/src/R/monica_io.r")

# Give the input file name to the function.
sim.json <- create_R_from_JSON_file("sim.json")
crop.json <- create_R_from_JSON_file("crop.json")
site.json <- create_R_from_JSON_file("site.json")

env <- create_env_json_from_json_config(
  list("crop" = crop.json,
       "site" = site.json,
       "sim" = sim.json,
       "climate" = ""
  )
)

env$pathToClimateCSV <- "C:/Users/berg.ZALF-AD/GitHub/monica/installer/Hohenfinow2/climate.csv"
#env$debugMode <- TRUE

env_str = create_JSON_string_from_R(env)
send_JSON_string(socket, env_str)
res_str <- receive_JSON_string(socket)
res_json <- create_R_from_JSON_string(res_str)
write_output(res_json, file = "out.csv", sep = ";")
