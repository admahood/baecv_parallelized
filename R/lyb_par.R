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

for(i in 4:length(tifs)){
  r <- raster(tifs[i])
  print(paste("data type is:", dataType(r)))
  splits <- list()
  
  t1 <- Sys.time()
  registerDoParallel(cores=corz) # for some reason this works better with doparallel and foreach
  splits <- foreach(j=1:length(sp_grd)) %dopar% {raster::crop(r, sp_grd[j])}
  
  print(paste(Sys.time() - t1, "minutes for splitting"))
  rm(r)
  
  year = as.integer(substr(splits[[1]]@data@names, 10,13)) # needs to be changed away from these magic numbers
  
  spl_rcl <- list()
  t1 <- Sys.time()
  registerDoParallel(cores=corz)
  print(paste("reclassifying"))
  filename <- paste0("scrap/reclassified", year,k, ".tif")
  spl_rcl <- foreach(k=1:length(splits)) %dopar% {
    spl_rcl[[k]] <- thanks_internet(splits[[k]], i, filename)
    splits[[k]] <- NULL
  }
  print(paste(Sys.time()-t1, "minutes for reclassifying", tifs[i]))
  rm(splits)
  
  t1 <- Sys.time()
  spl_rcl <-list()
  files <- list.files("scrap/")
  for(i in 1:length(files)){
    spl_rcl[[i]] <- raster(files[i])
  }
  rcl_all <- do.call(raster::merge, spl_rcl)
  print(paste(Sys.time()-t1, "minutes for merging", tifs[i]))
  
  file <- file.path(result_path, paste0("lyb_",year, ".tif"))

  t1 <- Sys.time()
  writeRaster(rcl_all, file, dtype = "INT1U")
  print(paste(Sys.time()-t1, "minutes for writing", tifs[i]))
  
  t1 <- Sys.time()
  system(paste0("aws s3 cp ",
                file, " ",
                "s3://earthlab-ls-fire/lyb/lyb_",year,".tif"))
  print(paste(Sys.time()-t1, "sending to s3", tifs[i]))
  
  system(paste("rm", file))
  system(paste("rm scrap/*"))
  rm(rcl_all)
  rm(spl_rcl)
  raster::removeTmpFiles()
  gc()
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