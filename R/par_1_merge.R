library(raster)
library(rgdal)
library(parallel)
library(doParallel)
library(sp)
library(sf)

# functions---------------------------------------------------------------------------
# copied from raster vignette

thanks_internet <- function(x, a, filename) {
  out <- raster(x)
  out <- writeStart(out, filename, overwrite=TRUE)
  for (r in 1:nrow(out)) {
    v <- getValues(x, r)
    v <- v * a
    out <- writeValues(out, v, r)
    }
  out <- writeStop(out)
  return(out)
  }

# Workflow _______________________________________________________________________________
setwd("~/baecv_parallelized")
dir.create("results")
dir.create("scrap")
raster::removeTmpFiles()

tif_path <- "data/" # path to a folder containing tif files
result_path <- "results" # where you want the resulting rasters to go
corz <- detectCores()


tifs <- Sys.glob(paste0(tif_path, "BAECV*.tif"))
pol <- st_as_sfc(st_bbox(raster(tifs[1])))
grd <- st_make_grid(pol,n=c(corz,1))
sp_grd <- sf::as_Spatial(grd)

for(i in 1:length(tifs)){
  t00 <- Sys.time()
  r <- raster(tifs[i])
  print(paste("beginning", tifs[i]))
  splits <- list()
  
  t1 <- Sys.time()
  registerDoParallel(cores=corz) # for some reason this works better with doparallel and foreach
  splits <- foreach(j=1:length(sp_grd)) %dopar% {raster::crop(r, sp_grd[j])}
  
  print("time for splitting")
  print(Sys.time() - t1)
  rm(r)
  
  year = as.integer(substr(splits[[1]]@data@names, 10,13)) # needs to be changed away from these magic numbers
  
  spl_rcl <- list()
  t1 <- Sys.time()
  registerDoParallel(cores=corz)
  print(paste("reclassifying"))
  spl_rcl <- foreach(k=1:length(splits)) %dopar% {
    xmin <- (substr(as.character(sp_grd[k]@bbox[[1]]),1,4))
    filename <- paste0("scrap/rcl", year,"_", xmin, ".tif")
    spl_rcl[[k]] <- thanks_internet(splits[[k]], year, filename)
    splits[[k]] <- NULL
  }
  print("time for reclassifying")
  print(Sys.time()-t1)
  rm(splits)
  
  t1 <- Sys.time()
  system(paste0("aws s3 cp ",
                "scrap/ ",
                "s3://earthlab-ls-fire/lyb/nomerge/ ",
                "--recursive"))
  print(Sys.time()-t1)
  print("for sending to s3")
  
  system(paste("rm scrap/*"))
  rm(spl_rcl)
  raster::removeTmpFiles()
  gc()
  print(Sys.time()-t00)
  print("for the whole thing")
}
# 
# res_tifs <- Sys.glob(paste0(result_path,"lyb_*.tif"))
# lyb_stk <- raster::stack(res_tifs)
# lyb <- calc(lyb_stk, max) 
# #now reclassify or figure out how to add 1983 while preserving 0s
# final_file <-  paste0(result_path,"lyb_whole_US_1984_2015_BAECV.tif")
# raster::writeRaster(lyb, filename = final_file)
# system(paste0("aws s3 cp ",
#               final_file, " ",
#               "s3://earthlab-ls-fire/lyb/lyb_whole_US_1984_2015.tif"))