##########################################################
# script for creating frequency maps for BAECV fire data #
# or whatever other fire data you want. 
# warning!! it has been bastardized
# Author: Adam Mahood
# Last update: June 1, 2017
##########################################################

library(raster)
library(rgdal)

tif_path = "/Users/computeruser/DATA/fire/usgs_baecv_tif/"
clip_path = "/Users/computeruser/DATA/background/ecoregions/CBR/"
wd_path = "/Users/computeruser/PROJECTS/lyb_study/"
result_path = "/Users/computeruser/DATA/fire/baecv_derived/"

setwd(wd_path)

# getting everything together

clip_region = readOGR(clip_path, "CBR")
tifs = Sys.glob(paste0("data/*.tif"))
stk = raster::stack(tifs)
clp_r = spTransform(clip_region, crs(stk))
clp = raster::crop(stk, extent(clp_r))

# Frequency

clc = raster::calc(stk, sum)

writeRaster(clc, paste0(result_path,"mcd64_ff_0017.tif"))

# only > 1

v1 = c(-999999, 1.9, NA, 
       1.9, 9999999, 1) 
m1 = matrix(v1, ncol=3, byrow=TRUE)
rcl = raster::reclassify(clc,m1)
writeRaster(rcl,paste0(result_path,"baecv_over1_8415.tif"))


# last year burned

elist=list()
for(i in 1:length(tifs)){
  year = as.integer(substr(tifs[[i]],43,46))
  v = c(0,0,0,
        0.9,367,year)
  m = matrix(v, ncol=3, byrow=TRUE)
  elist[[i]] = raster::reclassify(stk[[i]],m)
  print(paste("this better work", i, "times"))
}
lyb_stk = raster::stack(elist)
lyb = calc(lyb_stk, max)
writeRaster(lyb, "results/mcd64_lyb_0017.tif", overwrite=TRUE)
