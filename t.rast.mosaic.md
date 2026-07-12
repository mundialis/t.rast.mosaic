## DESCRIPTION

*t.rast.mosaic* is a GRASS GIS addon Python script to remove clouds and
aims at filling raster gaps using *r.series* and *r.series.lwr* (local
weighted regression) and aggregates temporally the maps of a space time
raster dataset by a user defined granularity using *t.rast.aggregate*.

Various steps of *t.rast.mosaic* can be run in parallel with the
**nprocs** option. With this option set to `> 1`, different time steps
and scenes are executed in parallel where possible.

## EXAMPLE

```sh
t.rast.mosaic input=s2_B02_10m output=B02_mosaic clouds=s2clouds cloudbuffer=10 \
  granularity='1 months' method=median nprocs=4
```

## SEE ALSO

This module needs several
[requirements](https://github.com/mundialis/t.rast.mosaic/blob/main/requirements.sh)
to be installed.

*[r.series](https://grass.osgeo.org/grass-stable/manuals/r.series.html),
[r.series.lwr](r.series.lwr.md) (addon),
[r.patch](https://grass.osgeo.org/grass-stable/manuals/r.patch.html),
[t.rast.aggregate](https://grass.osgeo.org/grass-stable/manuals/t.rast.aggregate.html),
[r.mapcalc](https://grass.osgeo.org/grass-stable/manuals/r.mapcalc.html)*

## AUTHORS

Anika Weinmann and Markus Metz, [mundialis](https://www.mundialis.de/),
Germany
