# Load the package required to read JSON files.
#library(rjson)
library(jsonlite)
library(rzmq)
library(readr)

setwd("C:/Users/berg.ZALF-AD/GitHub/monica/installer/Hohenfinow2")

if(!exists("create_env_json_from_json_config", mode="function")) 
  source("monica_io.r")

# Give the input file name to the function.
sim.json <- fromJSON(read_file("sim.json"), simplifyVector = FALSE)
crop.json <- fromJSON(read_file("crop.json"), simplifyVector = FALSE)
site.json <- fromJSON(read_file("site.json"), simplifyVector = FALSE)

env <- create_env_json_from_json_config(
  list("crop" = crop.json,
       "site" = site.json,
       "sim" = sim.json,
       "climate" = ""
  )
)

env$pathToClimateCSV <- "C:/Users/berg.ZALF-AD/GitHub/monica/installer/Hohenfinow2/climate.csv"

context = init.context()
socket = init.socket(context,"ZMQ_REQ")
connect.socket(socket,"tcp://localhost:6666")

in_str = toJSON(env, pretty = TRUE)


send.socket(socket, data=toJSON(env))
res_str <- receive.string(socket)
res_json <- fromJSON(res_str)

