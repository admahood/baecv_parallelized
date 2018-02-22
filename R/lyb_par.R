library(raster)
library(rgdal)
library(parallel)
library(doParallel)
library(sp)
library(sf)

# Workflow _______________________________________________________________________________
setwd("~/baecv_parallelized")
dir.create("results")

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
  
  t1 <- Sys.time()
  registerDoParallel(cores=corz) # for some reason this works better with doparallel and foreach
  splits <- foreach(j=1:length(sp_grd)) %dopar% raster::crop(r, sp_grd[j])
  print(paste(Sys.time() - t1, "minutes for splitting", tifs[i]))
  rm(r)
  
  year = as.integer(substr(splits[[1]]@data@names, 10,13)) # needs to be changed away from these magic numbers
  year_thing = year - 1983
  
  v = c(0,0,0,
        0.9,1.1,year_thing)
  m = matrix(v, ncol=3, byrow=TRUE)
  spl_rcl <- list()
  
  t1 <- Sys.time()
  registerDoParallel(cores=corz)
  spl_rcl <- foreach(k=1:length(splits)) %dopar% 
    raster::reclassify(splits[[k]], m, dtype = "INT1U")
  
  print(paste(Sys.time()-t1, "minutes for reclassifying", tifs[i]))
  rm(splits)
  
  print(dataType(spl_rcl[[1]]))
  t1 <- Sys.time()
  rcl_all <- do.call(raster::merge, spl_rcl)
  print(paste(Sys.time()-t1, "minutes for merging", tifs[i]))
  
  file <- file.path(result_path, paste0("lyb_",year, ".tif"))
  print(dataType(rcl_all))
  
  t1 <- Sys.time()
  writeRaster(rcl_all, file, dtype = "INT1U")
  print(paste(Sys.time()-t1, "minutes for writing", tifs[i]))
  
  t1 <- Sys.time()
  system(paste0("aws s3 cp ",
                file, " ",
                "s3://earthlab-ls-fire/lyb/lyb_",year,".tif"))
  print(paste(Sys.time()-t1, "sending to s3", tifs[i]))
  
  system(paste("rm", file))
  rm(rcl_all)
  rm(spl_rcl)
  raster::removeTmpFiles()
}

res_tifs <- Sys.glob(paste0(result_path,"lyb_*.tif"))
lyb_stk <- raster::stack(res_tifs)
lyb <- calc(lyb_stk, max) 
#now reclassify or figure out how to add 1983 while preserving 0s
final_file <-  paste0(result_path,"lyb_whole_US_1984_2015_BAECV.tif")
raster::writeRaster(lyb, filename = final_file)
system(paste0("aws s3 cp ",
              final_file, " ",
              "s3://earthlab-ls-fire/lyb/lyb_whole_US_1984_2015.tif"))