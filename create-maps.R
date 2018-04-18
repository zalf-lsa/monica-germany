
#https://stackoverflow.com/questions/9436947/legend-properties-when-legend-only-t-raster-package
#http://neondataskills.org/R/Plot-Rasters-In-R/


# Clear Environment Variables
rm(list = ls())

library(raster)
library(rgdal)
library(stringr)

setwd("C:/Users/berg.ZALF-AD/GitHub/monica-germany/")
r <- raster("wheatwinterwheat_yield_1996_1.asc") 
print("wheatwinterwheat_yield_1996_1.png")
png("wheatwinterwheat_yield_1996_1.png", width=2000, height=2000, pointsize=30)
plot(r, main="wheatwinterwheat_yield_1996_1")
dev.off()

setwd("C:/Users/berg.ZALF-AD/GitHub/monica-germany/")
#for(i in 1:32)
#for(i in list(c(1,9)))
for(dir in list(paste0("P:/monica-germany/UBA-final-runs/grassland/2018-03-23/revised-outputs")))
#for(dir in list(paste0("P:/monica-germany/UBA-final-runs/silage-maize/2018-03-27/best"),
#                paste0("P:/monica-germany/UBA-final-runs/silage-maize/2018-03-27/best/", "/aggregated")))
#for(dir in list(paste0("P:/monica-germany/UBA-final-runs/potato/2018-03-28/best"),
#                paste0("P:/monica-germany/UBA-final-runs/potato/2018-03-28/best/", "/aggregated")))
{
  #dir <- paste0("N:/germany")
  #dir <- paste0("P:/monica-germany/UBA-final-runs/silage-maize/2018-03-09/best")
  #dir <- paste0("P:/monica-germany/UBA-final-runs/silage-maize/2018-03-09/best/", "/aggregated")
  #dir <- paste0("P:/monica-germany/UBA-final-runs/winter-barley/2018-03-07/", i[[2]], "_best/")
  #dir <- paste0("P:/monica-germany/UBA-final-runs/winter-barley/2018-03-07/", i[[2]], "_best/", "/aggregated")
  #dir <- paste0("C:/Users/berg.ZALF-AD/GitHub/monica-germany/", i, "/no_calibration")
  if(file.exists(dir))
  {
    setwd(dir)
    files <- list.files(path=".", pattern=glob2rx("*exported*.asc"), full.names=F, recursive=FALSE)
    #print(files)
    for(filename in files)
    {
      out_filename = str_replace(filename, "asc", "png")
      print(out_filename)
      png(out_filename, width=2000, height=2000, pointsize=30)
      r <- raster(filename)
      plot(r, col=rev(terrain.colors(2000)), breaks=seq(0, 14000, length.out=2000), legend=F)
      plot(r, col=rev(terrain.colors(14)), breaks=seq(0, 14000, length.out=15), legend.only=T)
      title(main=paste0("grassland exported cut biomass  ", strsplit(out_filename, "_|\\.")[[1]][[3]]))
      #plot(r, main=paste0("grassland N leach ", strsplit(out_filename, "_|\\.")[[1]][[3]]))
      #plot(r, main=paste0("grassland exported cut biomass ", strsplit(out_filename, "_|\\.")[[1]][[3]]))
      dev.off()
    }
  }
}

#dir <- paste0("P:/carbiocial/out_grids/historical/results")
dir <- paste0("P:/carbiocial/out_grids/future_starr/results")
#dir <- paste0("P:/carbiocial/out_grids/future_wrf/results")
if(file.exists(dir))
{
  setwd(dir)
  files <- list.files(path=".", pattern=glob2rx("mmaizegrainmaize*.asc"), full.names=F, recursive=FALSE)
  #print(files)
  for(filename in files)
  {
    out_filename = str_replace(filename, "asc", "png")
    print(out_filename)
    png(out_filename, width=2000, height=2000, pointsize=30)
    r <- raster(filename)
    name_parts <- strsplit(out_filename, "_|\\.")[[1]]
    in_crop <- name_parts[[1]]
    first_crop <- paste0("soybean ", name_parts[[4]])
    second_crop <- name_parts[[5]]
    from_year <- name_parts[[7]]
    to_year <- name_parts[[8]]
    full_title <- paste0(in_crop, " in ", first_crop, " | ", second_crop, " ", from_year, "-", to_year)
    plot(r, col=rev(terrain.colors(2000)), breaks=seq(0, 12000, length.out=2000), legend=F)
    plot(r, col=rev(terrain.colors(12)), breaks=seq(0, 12000, length.out=13), legend.only=T)
    title(main=full_title)
    #plot(r, main=full_title)
    dev.off()
  }
}


setwd("P:/monica-germany/bkr-avgs/")
setwd("P:/monica-germany/landkreise-avgs/")
setwd("C:/Users/berg.ZALF-AD/GitHub/monica-germany/landkreise-avgs/")
setwd("C:/Users/berg.ZALF-AD/GitHub/monica-germany/statistical-data-out/")
setwd("C:/Users/berg.ZALF-AD/GitHub/monica-germany/indices-out/indices-1-2017-12-14/")
setwd("P:/monica-germany/statistical-data/grids")
setwd("C:/Users/berg.ZALF-AD/GitHub/monica-germany/out/")
files <- list.files(path=".", pattern=glob2rx("*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  #r[r > 1000] <- r@file@nodatavalue #nfert
  #r[r < 0 | r > 2000] <- r@file@nodatavalue #nleach
  #plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 200, length.out=100), legend=F)
  #plot(r, col=rev(terrain.colors(5)), breaks=seq(0, 200, length.out=6), legend.only=T)
  plot(r, main=out_filename)
  #title(main=out_filename)
  dev.off()
}

filename = "C:/Users/berg.ZALF-AD/GitHub/monica-germany/wheatwinterwheat_yearly-sum-nleach_1997_2.asc"
out_filename = str_replace(filename, "asc", "png")
print(out_filename)
png(out_filename, width=2000, height=2000, pointsize=30)
r <- raster(filename)
r[r > 1000] <- r@file@nodatavalue
plot(r, main=out_filename)
dev.off()



setwd("C:/Users/berg.ZALF-AD/GitHub/monica-germany/out")
files <- list.files(path=".", pattern=glob2rx("*clay*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 1, length.out=100), legend=F)
  plot(r, col=rev(terrain.colors(5)), breaks=seq(0, 1, length.out=6), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}

files <- list.files(path=".", pattern=glob2rx("*sand*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 1, length.out=100), legend=F)
  plot(r, col=rev(terrain.colors(5)), breaks=seq(0, 1, length.out=6), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}

files <- list.files(path=".", pattern=glob2rx("wheatwinterwheat_avg-30cm-silt*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 1, length.out=100), legend=F)
  plot(r, col=rev(terrain.colors(5)), breaks=seq(0, 1, length.out=6), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}

files <- list.files(path=".", pattern=("*transpiration-deficit*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 1, length.out=100), legend=F)
  plot(r, col=rev(terrain.colors(5)), breaks=seq(0, 1, length.out=6), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}

files <- list.files(path=".", pattern=glob2rx("*harvest-doy*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(365)), breaks=seq(1, 366), legend=F)
  plot(r, col=rev(terrain.colors(5)), breaks=seq(1, 366, length.out=6), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}

files <- list.files(path=".", pattern=glob2rx("wheatwinterwheat_maturity-doy*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(365)), breaks=seq(1, 366), legend=F)
  plot(r, col=rev(terrain.colors(5)), breaks=seq(1, 366, length.out=6), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}

files <- list.files(path=".", pattern=glob2rx("*max-LAI*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(139)), breaks=seq(0, 14, length.out=140), legend=F)
  plot(r, col=rev(terrain.colors(7)), breaks=seq(0, 14, length.out=8), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}

files <- list.files(path=".", pattern=glob2rx("*may-to-harvest-precip*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 100, length.out=100), legend=F)
  plot(r, col=rev(terrain.colors(5)), breaks=seq(0, 100, length.out=6), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}

files <- list.files(path=".", pattern=glob2rx("wheatwinterwheat_relative-total-development*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 1, length.out=100), legend=F)
  plot(r, col=rev(terrain.colors(5)), breaks=seq(0, 1, length.out=6), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}

files <- list.files(path=".", pattern=glob2rx("*anthesis-doy*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 1, length.out=100), legend=F)
  plot(r, col=rev(terrain.colors(5)), breaks=seq(0, 1, length.out=6), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}

files <- list.files(path=".", pattern=glob2rx("*total-precip*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 1, length.out=100), legend=F)
  plot(r, col=rev(terrain.colors(5)), breaks=seq(0, 1, length.out=6), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}

files <- list.files(path=".", pattern=glob2rx("*yield*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(99)), breaks=seq(2000, 8000, length.out=100), legend=F)
  plot(r, col=rev(terrain.colors(5)), breaks=seq(2000, 8000, length.out=6), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}









files <- list.files(path=".", pattern=glob2rx("wheatwinterwheat_may-to-harvest-precip*.asc"), full.names=F, recursive=FALSE)
for(filename in files)
{
  out_filename = str_replace(filename, "asc", "png")
  print(out_filename)
  png(out_filename, width=2000, height=2000, pointsize=30)
  r <- raster(filename)
  plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 100, length.out=100), legend=F)
  plot(r, col=rev(terrain.colors(5)), breaks=seq(0, 100, length.out=6), legend.only=T)
  #plot(r, main=out_filename)
  title(main="main=out_filename")
  dev.off()
}





r <- raster("C:/Users/berg.ZALF-AD/GitHub/monica-germany/out/wheatwinterwheat_avg-transpiration-deficit_avg.asc") 
plot(r, main="avg tradef 1995-2012")
plot(r, col=rev(terrain.colors(99)), breaks=seq(0, maxValue(r), length.out=100), legend=F)
r.range <- c(0, 1000)
plot(r, col=rev(terrain.colors(5)), legend.only=T,
     legend.shrink=0.75,
     axis.args=list(at=seq(0, 1000, 100),
                    labels=seq(0, 1000, 100), 
                    cex.axis=0.6))
plot(r, col=rev(rainbow(99, start=0, end=1)), breaks=seq(0, 1,length.out=100))
plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 1, length.out=100))
plot(r, col=rev(terrain.colors(99)), breaks=seq(0, 1, length.out=100), legend=F)
plot(r, col=rev(terrain.colors(9)), breaks=seq(0, 1, length.out=10), legend.only=T)

##################
#Poster SUSTAg
#r <- raster("allcrops_avg_pot_residues_.asc") 
#plot(r,
#     axes=FALSE)
#title(main="potential residue yield (kg crop-1)", cex.main=2)


