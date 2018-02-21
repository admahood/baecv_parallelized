library(raster)
library(rgdal)
library(parallel)
library(doParallel)
library(sp)
library(sf)

# Functions ----------------------------------------------------------------------------
lyb_par <- function(splits){
  
  rcl <- raster::reclassify(splits,m)
  return(rcl)
  # saving just in case parallel only gets partially through
}

# Workflow _______________________________________________________________________________

#setwd("~/fire_data_mongering")
tif_path <- "data/" # path to a folder containing tif files
#tif_path <- "~/DATA/FIRE/baecv/"
result_path <- "results" # where you want the resulting rasters to go
corz <- detectCores()


tifs <- Sys.glob(paste0(tif_path, "BAECV*.tif"))
pol <- st_as_sfc(st_bbox(raster(tifs[1])))
grd <- st_make_grid(pol,n=c(corz,1))
sp_grd <- sf::as_Spatial(grd)

for(i in 1:length(tifs)){
  r <- raster(tifs[i])
  splits <- list()
  
  #cl <- makeCluster(corz)
  registerDoParallel(cores=corz) # for some reason this works better with doparallel and foreach
  splits <- foreach(j=1:length(sp_grd)) %dopar% raster::crop(r, sp_grd[j])
  #stopCluster(cl)
  
  rm(r)
  year = as.integer(substr(splits[[1]]@data@names, 10,13)) # needs to be changed away from these magic numbers
  v = c(0,0,0,
        0.9,1.1,year)
  m = matrix(v, ncol=3, byrow=TRUE)
  
  registerDoParallel(cores=corz)
  spl_rcl <- foreach(k=1:length(splits)) %dopar% raster::reclassify(splits[[j]], m)
  
  # cl <- makeCluster(getOption("cl.cores", corz))
  # spl_rcl <- parLapply(cl, c(splits, m), raster::reclassify)
  # stopCluster(cl)
  
  rcl_all <- do.call(raster::merge, spl_rcl)
  writeRaster(rcl_all, file.path(result_path, paste0("lyb_",year, ".tif")))
  rm(splits)
}

res_tifs <- Sys.glob(paste0(result_path,"lyb_*.tif"))
lyb_stk <- raster::stack(res_tifs)
lyb = calc(lyb_stk, max)
raster::writeRaster(lyb, filename = paste0(result_path,"lyb_whole_US_1984_2015_BAECV.tif"))