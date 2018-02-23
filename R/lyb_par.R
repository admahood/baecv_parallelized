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
    v <- v + a
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
  r <- raster(tifs[i])
  print(paste("data type is:", dataType(r)))
  splits <- list()
  
  t1 <- Sys.time()
  registerDoParallel(cores=corz) # for some reason this works better with doparallel and foreach
  splits <- foreach(j=1:length(sp_grd)) %dopar% {raster::crop(r, sp_grd[j])}
  
  print(paste(Sys.time() - t1, "minutes for splitting"))
  print(paste("it is", inMemory(splits[[1]]), "that the cropped rasters are in memory."))
  rm(r)

  print(paste("They're this big:", 
              format(object.size(splits),units = "Gb")
              ))
  
  # year = as.integer(substr(splits[[1]]@data@names, 10,13)) # needs to be changed away from these magic numbers
  # year_thing = year - 1983
  # v = c(0,0,0,
  #       0.9,1.1,year_thing)
  # m = matrix(v, ncol=3, byrow=TRUE)
  
  spl_rcl <- list()
  t1 <- Sys.time()
  registerDoParallel(cores=corz)
  print(paste("reclassifying"))
  filename <- paste0("scrap/reclassified", year, ".tif")
  spl_rcl <- foreach(k=1:length(splits)) %dopar% {
    # raster::reclassify(splits[[k]], m)
    spl_rcl[[k]] <- thanks_internet(splits[[k]], i, filename)
    splits[[k]] <- NULL
  }
  print(paste(Sys.time()-t1, "minutes for reclassifying", tifs[i]))
  print(paste("reclassified thing is in memory?", inMemory(spl_rcl[[1]])))
  rm(splits)
  
  # gc()
  # for(l in 1:length(spl_rcl)){
  # print(paste("reclassified thing",i,"is this type:",dataType(spl_rcl[[1]])))
  # print(paste("reclassified thing",i, "is this big:", format(object.size(spl_rcl[[l]]),units = "Gb")))
  # }
  # if (storage.mode(spl_rcl[[1]][]) != "integer"){
  #   print(paste("We're gonna try and switch it to integer"))
  #   for(l in 1:length(spl_rcl)){
  #     gc()
  #     t <- Sys.time()
  #     storage.mode(spl_rcl[[l]][]) <- "integer"
  #     print(paste("converted raster", l, "to integer in", Sys.time()-t))
  #     writeRaster(spl_rcl[[l]], filename = paste0("scrap/reclass",year,l,".tif"))
  #     spl_rcl[[l]] <- NULL
  #   }
  #   print(paste("now it's this big:", format(object.size(spl_rcl), units = "Gb")))
  # }
  
  
  t1 <- Sys.time()
  # spl_rcl <-list()
  # files <- list.files("scrap/")
  # for(i in 1:length(files)){
  #   spl_rcl[[i]] <- raster(files[i])
  # }
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