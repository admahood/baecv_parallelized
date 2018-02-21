library(raster)
library(rgdal)
library(parallel)
library(doParallel)
library(sp)
library(sf)

# Workflow _______________________________________________________________________________

tif_path <- "data/" # path to a folder containing tif files
result_path <- "results" # where you want the resulting rasters to go
corz <- detectCores()


tifs <- Sys.glob(paste0(tif_path, "BAECV*.tif"))
pol <- st_as_sfc(st_bbox(raster(tifs[1])))
grd <- st_make_grid(pol,n=c(corz,1))
sp_grd <- sf::as_Spatial(grd)

for(i in 1:length(tifs)){
  r <- raster(tifs[i])
  splits <- list()

  registerDoParallel(cores=corz) # for some reason this works better with doparallel and foreach
  splits <- foreach(j=1:length(sp_grd)) %dopar% raster::crop(r, sp_grd[j])

  rm(r)
  
  year = as.integer(substr(splits[[1]]@data@names, 10,13)) # needs to be changed away from these magic numbers
  v = c(0,0,0,
        0.9,1.1,year)
  m = matrix(v, ncol=3, byrow=TRUE)
  spl_rcl <- list()
  
  registerDoParallel(cores=corz)
  spl_rcl <- foreach(k=1:length(splits)) %dopar% raster::reclassify(splits[[k]], m)
  
  rm(splits)
  
  rcl_all <- do.call(raster::merge, spl_rcl)
  file <- file.path(result_path, paste0("lyb_",year, ".tif"))
  writeRaster(rcl_all, file)
  system(paste0("aws s3 cp ",
                file, " ",
                "s3://earthlab-ls-fire/lyb/lyb_",year,".tif"))
  rm(rcl_all)
  rm(spl_rcl)
  
}

res_tifs <- Sys.glob(paste0(result_path,"lyb_*.tif"))
lyb_stk <- raster::stack(res_tifs)
lyb = calc(lyb_stk, max)
raster::writeRaster(lyb, filename = paste0(result_path,"lyb_whole_US_1984_2015_BAECV.tif"))