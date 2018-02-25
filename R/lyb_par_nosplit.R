library(raster)
library(rgdal)
library(parallel)
library(doParallel)
library(sp)
library(sf)

# functions---------------------------------------------------------------------------
# copied from raster vignette

thanks_internet <- function(x, a, filename, corz=corz) {
  out <- raster(x)
  out <- writeStart(out, filename, overwrite=TRUE)
  registerDoParallel(cores=corz)
  foreach(i= 1:nrow(out)) %dopar% {
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


for(i in 1:length(tifs)){
  r <- raster(tifs[i])
  print(paste("data type is:", dataType(r)))
  
  year = as.integer(substr(splits[[1]]@data@names, 10,13)) # needs to be changed away from these magic numbers
  
  spl_rcl <- list()
  t1 <- Sys.time()
  print(paste("reclassifying"))
 
  filename <- paste0("scrap/reclassified", year, ".tif")
  spl_rcl <- thanks_internet(r, i, filename, corz)
    
  print(paste(Sys.time()-t1, "minutes for reclassifying", tifs[i]))

  t1 <- Sys.time()
  system(paste0("aws s3 cp ",
                filename, " ",
                "s3://earthlab-ls-fire/lyb/lyb_",year,".tif"))
  print(paste(Sys.time()-t1, "sending to s3", tifs[i]))
  system(paste("rm scrap/*"))
  
  raster::removeTmpFiles()
  gc()
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