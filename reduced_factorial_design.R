library(FrF2)
setwd("C:/Users/stella/Desktop/ZALF/Laura_Klement/evaluation")
setwd("C:/Users/berg.ZALF-AD/GitHub/monica-germany")

design <- FrF2(resolution = 4, randomize = FALSE, factor.names = list(
  GroundWaterLevel = c("false", "true"), 
  ImpenetrableLayer = c("false", "true"), 
  #Skeleton = c("false", "true"), 
  Slope = c("false", "true"), 
  SowingDate = c("fixed", "auto"),
  HarvestDate = c("fixed", "auto"),
  LandCover = c("false", "true"), 
  Nresponse_and_Fertil = c("false", "true"), 
  WaterDeficitResponse = c("false", "true"), 
  LeafExtensionModifier = c("false", "true"), 
  #Calibration = c("false", "true")
  ))
 
design

export.design(design, filename = "design", type = "csv", OutDec = ".") 
