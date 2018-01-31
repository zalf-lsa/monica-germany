library(FrF2)
setwd("C:/Users/stella/GitHub/monica-germany")
#setwd("C:/Users/berg.ZALF-AD/GitHub/monica-germany")

design <- FrF2(resolution = 4, randomize = FALSE, factor.names = list(
  GroundWaterLevel = c("false", "true"), 
  ImpenetrableLayer = c("false", "true"), 
  Phenology = c("default", "calibrated"), #decide wether to 
  Slope = c("false", "true"), 
  SowingDate = c("fixed", "auto"),
  HarvestDate = c("fixed", "auto"),
  LandCover = c("false", "true"), 
  Nresponse_and_Fertil = c("false", "true"), 
  WaterDeficitResponse = c("false", "true"),
  FCM_Calibration = c("false", "true"),
  LeafExtensionModifier = c("false", "true")
  ))
 
#design

summary(design)


export.design(design, filename = "design_complete", type = "csv", OutDec = ".") 

res_r <- read.csv("C:/Users/stella/Documents/GitHub/monica-germany/calculate-indices/report_250118/shallow_groundwater/r.csv",header=F)
res_rrmse <- read.csv("C:/Users/stella/Documents/GitHub/monica-germany/calculate-indices/report_250118/shallow_groundwater/RRMSE.csv",header=F)
res_pbias <- read.csv("C:/Users/stella/Documents/GitHub/monica-germany/calculate-indices/report_250118/shallow_groundwater/pBIAS.csv",header=F)
res_ai <- read.csv("C:/Users/stella/Documents/GitHub/monica-germany/calculate-indices/report_250118/shallow_groundwater/AI.csv",header=F)

resp_r <- add.response(design, res_r)
resp_rrmse <- add.response(design, res_rrmse)
resp_pbias <- add.response(design, res_pbias)
resp_ai <- add.response(design, res_ai)

plot(resp_rrmse, cex = 1, cex.lab = 0.8, cex.axis = 0.8,
     main = "Main effects plot rrmse", cex.main = 2)

IAPlot(resp_rrmse, abbrev = 5, show.alias = TRUE, lwd = 2, cex = 2,
       cex.xax = 1.2, cex.lab = 1.5)

DanielPlot(resp_rrmse, code = TRUE, half = TRUE, alpha = 0.1,
           cex.main = 1.8, cex.pch = 1.2, cex.lab = 1.4, cex.fac = 1.4,
           cex.axis = 1.2)


#Definition of high elevation and shallow groundwater landkreise
topography <- read.csv("C:/Users/stella/Documents/GitHub/monica-germany/avg_elevation_latitude_gw_per_landkreis.csv",header=T)
elevations <- as.vector(topography['elevation'])
gwater <- as.vector(topography['groundwaterlevel'])

my_var = unlist(gwater)

hist(my_var, breaks=5, col="red")

x <- my_var 
h<-hist(x, breaks=10, col="red",  
        main="Histogram with Normal Curve") 
xfit<-seq(min(x),max(x),length=40) 
yfit<-dnorm(xfit,mean=mean(x),sd=sd(x)) 
yfit <- yfit*diff(h$mids[1:2])*length(x) 
lines(xfit, yfit, col="blue", lwd=2)

#example of RFD below
plan.annotated <- FrF2(16, 6, factor.names = list(
  DieOrif = c(2.093, 2.1448), PistDiam = c(9.462, 9.5),
  Temp = c(188.1, 191.1), DieClean = c("Dirty", "Clean"),
  SMass = c(4, 8), BarClean = c("Dirty", "Clean")),
  seed = 6285)

summary(plan.annotated)

MI <- c(35.77, 35.03, 38.5, 39.33, 35.7, 35.1, 39.27, 37, 41.07, 32.03,
           42, 37.63, 40.2, 37, 40.1, 35.03)

plan.resp <- add.response(plan.annotated, MI)

plot(plan.resp, cex = 1.2, cex.lab = 1.2, cex.axis = 1.2,
     main = "Main effects plot for MI", cex.main = 2)

#MEPlot(plan.resp, abbrev = 5, cex.xax = 1.6, cex.main = 2)

IAPlot(plan.resp, abbrev = 5, show.alias = TRUE, lwd = 2, cex = 2,
       cex.xax = 1.2, cex.lab = 1.5)

IAPlot(plan.resp, abbrev = 5, select = 3:6, lwd = 2, cex = 2,
       cex.xax = 1.2, cex.lab = 1.5)

summary(lm(plan.resp))

DanielPlot(plan.resp, code = TRUE, half = TRUE, alpha = 0.1,
           cex.main = 1.8, cex.pch = 1.2, cex.lab = 1.4, cex.fac = 1.4,
           cex.axis = 1.2)

summary(plan.resp)
