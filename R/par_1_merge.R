library(raster)
library(rgdal)
library(parallel)
library(doParallel)
library(sp)
library(sf)

# functions---------------------------------------------------------------------------
# copied from raster vignette

thanks_internet <- function(x, a, filename) {
  # this function multiplies each cell of a raster by a
  # x: the raster, either a character string or already rastered in object
  # a: object to multiply each cell by
  # filename: if you don't already know please return my stolen computer
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

# Workflow ---------------------------------------------------------------------
# initial prep -----------------------------------------------------------------
setwd("~/baecv_parallelized")
dir.create("results")
dir.create("scrap")
dir.create("data")
raster::removeTmpFiles()
raster::rasterOptions(tmpdir = "/tmp/rast_temp")
system("rm -r /tmp/rast_temp")

tif_path <- "data/" # path to a folder containing tif files
result_path <- "results" # where you want the resulting rasters to go
corz <- detectCores()
s3_prefix <- "s3://earthlab-ls-fire/"
years <- 1984:2015

# big for loop begins ----------------------------------------------------------

for(i in 1:length(years)){
  t00 <- Sys.time()
  t1 <- Sys.time()
  
  # downloading and extracting the stuff ---------------------------------------
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
  r <- raster(paste0(ex_file))
  splits <- list()
  
  # making a grid with the same number of cells as we have cores ---------------
  if (!exists("sp_grd")){
    pol <- st_as_sfc(st_bbox(r))
    grd <- st_make_grid(pol,n=c(corz,1))
    sp_grd <- sf::as_Spatial(grd)
  }
  
  # splitting the raster into digestable (parallelizable) chunks ---------------
  t1 <- Sys.time()
  registerDoParallel(cores=corz)
  splits <- foreach(j=1:length(sp_grd)) %dopar% {raster::crop(r, sp_grd[j])}
  
  print("time for splitting")
  print(Sys.time() - t1)
  rm(r)
  
  # actually reclassifying from binary to the year of fire ---------------------
  
  year = as.integer(substr(splits[[1]]@data@names, 10,13)) 
  # needs to be changed away from these magic numbers
  
  spl_rcl <- list() #possibly unnecesary
  t1 <- Sys.time()
  registerDoParallel(cores=corz)
  print(paste("reclassifying"))
  
  spl_rcl <- foreach(k=1:length(splits)) %dopar% {
    # making a filename based on location
    xmin <- (substr(as.character(sp_grd[k]@bbox[[1]]),1,4))
    filename <- paste0("scrap/rcl", year,"_", xmin, ".tif")
    # applying the function
    spl_rcl[[k]] <- thanks_internet(splits[[k]], year, filename)
    splits[[k]] <- NULL # preserving memory/space
  }
  print("time for reclassifying")
  print(Sys.time()-t1)
  rm(splits)
  
  # sending the reclassified but unmerged files to the s3 bucket ---------------
  # and then deleting everything
  t1 <- Sys.time()
  system(paste0("aws s3 cp ",
                "scrap/ ",
                "s3://earthlab-ls-fire/lyb/nomerge/ ",
                "--recursive"))
  print(Sys.time()-t1)
  print("for sending to s3")
  
  system(paste("rm scrap/*"))
  system(paste("rm", ex_file))
  rm(spl_rcl)
  
  # going overboard trying to remove temp files
  raster::removeTmpFiles()
  system("rm -rf /tmp/rast_temp") 
  gc()
  print(Sys.time()-t00)
  print("for the whole thing")
}

# downloading all those things from s3 -----------------------------------------

dir.create("scrap")
dir.create("results")
system(paste0("aws s3 cp ",
              "s3://earthlab-ls-fire/lyb/nomerge/ ",
              "scrap/ ",
              "--recursive"))

# calculating last year burn on each chunk -------------------------------------
 # note: this requires a huge amount of ram(either 64 or 128 gb)
 # i got around it by splitting it into 2 
foreach(i = 1:length(sp_grd)) %dopar% {
  xmin <- substr(as.character(sp_grd[i]@bbox[[1]]),1,4) 
  tifs <- Sys.glob(paste0("scrap/*", xmin,".tif"))
  stk <- raster::stack(tifs)
  clc <- raster::calc(stk, max)
  filename <- paste0("results/lyb_",xmin,".tif")
  writeRaster(clc, filename=filename)
}

# merging to the final product (cant parallelize) ------------------------------

t1 <- Sys.time()
print("merging")
spl_rcl <-list()
files <- list.files("results/")
for(p in 1:length(files)){
  spl_rcl[[p]] <- raster(paste0("results/",files[p]))
}
rcl_all <- do.call(raster::merge, spl_rcl)
print(Sys.time()-t1)

# writing the raster -----------------------------------------------------------
t1 <- Sys.time()
writeRaster(rcl_all, "results/lyb_usa_baecv_1984_2015.tif")
print(Sys.time()-t1)
print("for writing")

# uploading to s3 --------------------------------------------------------------

t1 <- Sys.time()
system(paste0("aws s3 cp ",
              "results/lyb_usa_baecv_1984_2015.tif ",
              "s3://earthlab-ls-fire/lyb/lyb_usa_baecv_1984_2015.tif"))
print(Sys.time()-t1)
print("for sending to s3")
