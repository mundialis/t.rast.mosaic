# pip related packages
#sudo apt install python3-pip
pip3 install sentinelsat
pip3 install psutil
pip3 install GDAL==$(gdal-config --version)

# general GRASS GIS addons
g.extension extension=i.sentinel
g.extension extension=r.series.lwr
g.extension extension=i.histo.match
g.extension extension=i.zero2null

# GRASS GIS addon by mundialis
g.extension extension=t.sentinel url=https://github.com/mundialis/t.sentinel
