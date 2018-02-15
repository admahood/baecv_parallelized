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
clip_path = "/Users/computeruser/DATA/background/romo_tracts/"
result_path = "/Users/computeruser/DATA/fire/baecv_derived/"

layer = "ROMO_boundary"

# getting everything together

clip_region = readOGR(clip_path, layer)
tifs = Sys.glob(file.path(tif_path, "*.tif"))
stk = raster::stack(tifs)
clp_r = spTransform(clip_region, crs(stk))
clp = raster::crop(stk, extent(clp_r))

# Frequency

v1 = c(-999999, 0, 0, 
       .9, 9999999, 1) 
m1 = matrix(v1, ncol=3, byrow=TRUE)
rcl = raster::reclassify(clp,m1)
clc = raster::calc(rcl, sum)

writeRaster(clc, paste0(result_path,"baecv_ff_0016_ROMO.tif"), overwrite=TRUE)

# last year burned

elist=list()
for(i in 1:length(tifs)){
  year = as.integer(substr(tifs[[i]],56,59))
  v = c(0,0,0,
        0.9,367,year)
  m = matrix(v, ncol=3, byrow=TRUE)
  elist[[i]] = raster::reclassify(clp[[i]],m)
  print(paste("this better work", i, "times"))
}
lyb_stk = raster::stack(elist)
lyb = calc(lyb_stk, max)
writeRaster(lyb, paste0(result_path,"/baecv_lyb_0016_ROMO.tif"), overwrite=TRUE);plot(lyb);unique(lyb)

# only > 1

v1 = c(-999999, 1.9, NA, 
       1.9, 9999999, 1) 
m1 = matrix(v1, ncol=3, byrow=TRUE)
rcl = raster::reclassify(clc,m1)
writeRaster(rcl,paste0(result_path,"baecv_over1_8415.tif"))


