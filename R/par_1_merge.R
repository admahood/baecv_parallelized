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
dir.create("data")
raster::removeTmpFiles()

tif_path <- "data/" # path to a folder containing tif files
result_path <- "results" # where you want the resulting rasters to go
corz <- detectCores()
s3_prefix <- "s3://earthlab-ls-fire/"
years <- 1984:2015

for(i in 1:length(years)){
  t00 <- Sys.time()
  t1 <- Sys.time()
  print("downloading")
  dl_file <-(paste0("BAECV_",years[i],"_v1.1_20170908.tar.gz"))
  system(paste0("aws s3 cp ",
                s3_prefix, "v1.1/", dl_file, " ",
                "data/", dl_file))
  print(Sys.time() - t1)
  
  ex_file <- paste0("BAECV_bc_",years[i],"_v1.1_20170908.tif")
  t1 <- Sys.time()
  print("extracting")
  system(paste0("tar -zxvf data/", dl_file, " ", ex_file))
  system(paste0("rm data/", dl_file))
  print(Sys.time()-t1)
  
  print(paste("beginning", years[i]))
  r <- raster(paste0("data/", ex_file))
  splits <- list()
  
  if (!exists("sp_grd")){
    pol <- st_as_sfc(st_bbox(r))
    grd <- st_make_grid(pol,n=c(corz,1))
    sp_grd <- sf::as_Spatial(grd)
  }
  
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
dir.create("scrap")
dir.create("results")
system(paste0("aws s3 cp ",
              "s3://earthlab-ls-fire/lyb/nomerge/ ",
              "scrap/ ",
              "--recursive"))

xmins <- c()
foreach(i = 1:length(sp_grd)) %dopar% {
  xmin <- substr(as.character(sp_grd[i]@bbox[[1]]),1,4)
  tifs <- Sys.glob(paste0("scrap/*", xmin,".tif"))
  stk <- raster::stack(tifs)
  clc <- raster::calc(stk, max)
  filename <- paste0("results/lyb_",xmin,"tif")
  writeRaster(clc, filename=filename)
}

t1 <- Sys.time()
spl_rcl <-list()
files <- list.files("results/")
for(p in 1:length(files)){
  spl_rcl[[p]] <- raster(paste0("results/",files[p]))
}
rcl_all <- do.call(raster::merge, spl_rcl)
print(Sys.time()-t1)
print("for merging")


t1 <- Sys.time()
writeRaster(rcl_all, "results/lyb_usa_baecv_1984_2015.tif")
print(Sys.time()-t1)
print("for writing")

t1 <- Sys.time()
system(paste0("aws s3 cp ",
              "results/lyb_usa_baecv_1984_2015.tif ",
              "s3://earthlab-ls-fire/lyb/lyb_usa_baecv_1984_2015.tif"))
print(Sys.time()-t1)
print("for sending to s3")
