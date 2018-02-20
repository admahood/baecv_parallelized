library(raster)
library(rgdal)
library(parallel)
library(doParallel)
library(sp)

setwd("~/fire_data_mongering")
tif_path <- "data/" # path to a folder containing tif files
tif_path <- "~/DATA/FIRE/baecv/"
result_path <- "results/" # where you want the resulting rasters to go

tifs <- Sys.glob(paste0(tif_path, "BAECV*.tif"))
pol <- st_as_sfc(st_bbox(raster(tifs[1])))
grd <- st_make_grid(pol,n=c(8,1))

#lyb for whole US
lyb_par <- function(tifs){
  year = as.integer(substr(tifs, 15,18)) # needs to be changed away from these magic numbers
  
  # perhaps a list of years as the input is better?
  v = c(0,0,0,
        0.9,1.1,year)
  m = matrix(v, ncol=3, byrow=TRUE)
  r = raster::raster(tifs)
  rcl <- raster::reclassify(r,m)
  raster::writeRaster(rcl, filename = paste0(result_path,"lyb_",year,".tif"), overwrite = TRUE)
  rm(r)
  rm(rcl)
  # saving just in case parallel only gets partially through
}

corz <- detectCores()
cl <- makeCluster(getOption("cl.cores", corz)) 
parLapply(cl, tifs, lyb_par)
stopCluster(cl)

res_tifs <- Sys.glob(paste0(result_path,"lyb_*.tif"))
lyb_stk <- raster::stack(res_tifs)
lyb = calc(lyb_stk, max)
raster::writeRaster(lyb, filename = "lyb_whole_US_1984_2015_BAECV.tif")