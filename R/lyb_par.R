library(raster)
library(rgdal)
library(parallel)
library(doParallel)
library(sp)
library(sf)

# Functions ----------------------------------------------------------------------------
lyb_par <- function(tif,splits){
  year = as.integer(substr(tif, 15, 18)) # needs to be changed away from these magic numbers
  
  # perhaps a list of years as the input is better?
  v = c(0,0,0,
        0.9,1.1,year)
  m = matrix(v, ncol=3, byrow=TRUE)
  rcl <- raster::reclassify(splits,m)
  return(rcl)
  # saving just in case parallel only gets partially through
}

# Workflow _______________________________________________________________________________

setwd("~/fire_data_mongering")
tif_path <- "data/" # path to a folder containing tif files
tif_path <- "~/DATA/FIRE/baecv/"
result_path <- "results" # where you want the resulting rasters to go
corz <- detectCores()


tifs <- Sys.glob(paste0(tif_path, "BAECV*.tif"))
pol <- st_as_sfc(st_bbox(raster(tifs[1])))
grd <- st_make_grid(pol,n=c(corz,1))
sp_grd <- sf::as_Spatial(grd)

for(i in 1:length(tifs)){
  r <- raster(tifs[i])
  year = as.integer(substr(tifs[i], 15, 18))
  splits <- list()
  for(j in 1:length(sp_grd)){
    splits[[j]] <-raster::crop(r, sp_grd[j])
  }
  cl <- makeCluster(getOption("cl.cores", corz))
  spl_rcl <- parLapply(cl, c(tifs[i], splits), lyb_par)
  stopCluster(cl)
  rcl_all <- do.call(raster::merge, spl_rcl)
  writeRaster(rcl_all, file.path(result_path, paste0("lyb_",year, ".tif")))
}

res_tifs <- Sys.glob(paste0(result_path,"lyb_*.tif"))
lyb_stk <- raster::stack(res_tifs)
lyb = calc(lyb_stk, max)
raster::writeRaster(lyb, filename = paste0(result_path,"lyb_whole_US_1984_2015_BAECV.tif"))