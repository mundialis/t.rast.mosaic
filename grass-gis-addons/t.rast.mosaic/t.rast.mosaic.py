#!/usr/bin/env python3

############################################################################
#
# MODULE:       t.rast.mosaic
#
# AUTHOR(S):    Anika Bettge <bettge at mundialis.de>
#
# PURPOSE:      Removes clouds, try to fill raster gaps using r.series and
#               r.series.lwr and aggregates temporally the maps of a space
#               time raster dataset by a user defined granularity using
#               t.rast.aggregate
#
# COPYRIGHT:	(C) 2020 by mundialis and the GRASS Development Team
#
#		This program is free software under the GNU General Public
#		License (>=v2). Read the file COPYING that comes with GRASS
#		for details.
#
#############################################################################

#%Module
#% description: removes clouds, try to fill raster gaps using r.series and r.series.lwr and aggregates temporally the maps of a space time raster dataset by a user defined granularity using t.rast.aggregate.
#% keyword: temporal
#% keyword: aggregation
#% keyword: series
#% keyword: filtering
#% keyword: raster
#% keyword: time
#%end

#%option
#% key: input
#% type: string
#% required: yes
#% multiple: no
#% description: Name of the input space time raster dataset with e.g. Sentinel-2 scenes of one band
#% gisprompt: old,strds,strds
#%end

#%option
#% key: output
#% type: string
#% required: yes
#% multiple: no
#% key_desc: name
#% description: Name of the output space time raster dataset
#% gisprompt: new,strds,strds
#%end

#%option
#% key: clouds
#% type: string
#% required: no
#% multiple: no
#% description: Name of the input space time raster dataset with clouds
#%end

#%option
#% key: cloudbuffer
#% type: double
#% required: no
#% multiple: no
#% description: Radius of cloud buffer in raster cells (it is every time negativ)
#% answer: 1.01
#%end

#%option
#% key: shadows
#% type: string
#% required: no
#% multiple: no
#% description: Name of the input space time raster dataset with shadows
#%end

#%option
#% key: shadowbuffer
#% type: double
#% required: no
#% multiple: no
#% description: Radius of shadow buffer in raster cells (it is every time negativ)
#% answer: 1.01
#%end

#%option
#% key: method
#% type: string
#% required: yes
#% multiple: no
#% options: average,count,median,mode,minimum,min_raster,maximum,max_raster,stddev,range,sum,variance,diversity,slope,offset,detcoeff,quart1,quart3,perc90,quantile,skewness,kurtosis
#% description: Aggregate operation to be performed on the raster maps
#% answer: median
#%end

#%option
#% key: granularity
#% type: string
#% required: yes
#% multiple: no
#% description: Aggregation granularity, format absolute time "x years, x months, x weeks, x days, x hours, x minutes, x seconds" or an integer value for relative time
#% answer: 1 months
#%end

#%option
#% key: max
#% type: integer
#% required: no
#% multiple: no
#% description: Number of the maximum value for raster maps
#% answer: 10000
#% gisprompt: Number of the maximum value for raster maps
#%end

#%option
#% key: nprocs
#% type: integer
#% required: no
#% multiple: no
#% label: Number of parallel processes to use
#% answer: 1
#%end

#%flag
#% key: q
#% description: Use the first quartil additional to the median by patching
#%end

import atexit
from datetime import datetime
import multiprocessing as mp
import os
import re
import sys

import grass.script as grass
from grass.pygrass.modules import Module, ParallelModuleQueue


# initialize global vars
rm_regions = []
rm_vectors = []
rm_rasters = []
rm_strds = []


def cleanup():
    nuldev = open(os.devnull, 'w')
    kwargs = {
        'flags': 'f',
        'quiet': True,
        'stderr': nuldev
    }
    for rmr in rm_regions:
        if rmr in [x for x in grass.parse_command('g.list', type='region')]:
            grass.run_command(
                'g.remove', type='region', name=rmr, **kwargs)
    for rmv in rm_vectors:
        if grass.find_file(name=rmv, element='vector')['file']:
            grass.run_command(
                'g.remove', type='vector', name=rmv, **kwargs)
    for rmrast in rm_rasters:
        if grass.find_file(name=rmrast, element='raster')['file']:
            grass.run_command(
                'g.remove', type='raster', name=rmrast, **kwargs)
    for rmtr in rm_strds:
        strdsrasters = [x.split('|')[0] for x in grass.parse_command('t.rast.list', input=rmtr, flags='u')]
        for rmr in rmtr:
            if rmr in [x for x in grass.parse_command('g.list', type='region')]:
                grass.run_command(
                    'g.remove', type='region', name=rmr, **kwargs)
        grass.run_command('t.remove', inputs=rmtr, flags='rf', type='strds')


def test_nprocs():
    # Test nprocs settings
    nprocs = int(options['nprocs'])
    nprocs_real = mp.cpu_count()
    if nprocs > nprocs_real:
        grass.warning(
            "Using %d parallel processes but only %d CPUs available."
            % (nprocs, nprocs_real))


def main():

    # https://medium.com/sentinel-hub/how-to-create-cloudless-mosaics-37910a2b8fa8

    global rm_regions, rm_rasters, rm_vectors, rm_strds
    global name, scene_keys

    # parameters
    strds = options['input']
    clouds = options['clouds']
    shadows = options['shadows']
    strdsout = options['output']
    nprocs = int(options['nprocs'])

    test_nprocs()

    # test if necessary GRASS GIS addons are installed
    if not grass.find_program('r.series.lwr', '--help'):
        grass.fatal(_("The 'r.series.lwr' module was not found, install it first:") +
                    "\n" +
                    "g.extension r.series.lwr")
    if not grass.find_program('i.histo.match', '--help'):
        grass.fatal(_("The 'i.histo.match' module was not found, install it first:") +
                    "\n" +
                    "g.extension i.histo.match")

    strdsrasters = [x.split('|')[0] for x in grass.parse_command('t.rast.list', input=strds, flags='u')]
    strdstimes = [x.split('|')[2] for x in grass.parse_command('t.rast.list', input=strds, flags='u')]
    if clouds:
        cloudrasters = [x.split('|')[0] for x in grass.parse_command('t.rast.list', input=clouds, flags='u')]
        cloudtimes = [x.split('|')[2] for x in grass.parse_command('t.rast.list', input=clouds, flags='u')]
        if len(strdsrasters) != len(cloudrasters):
            grass.fatal(_("Number of raster in <input> strds and <clouds> strds are not the same."))
    if shadows:
        shadowrasters = [x.split('|')[0] for x in grass.parse_command('t.rast.list', input=shadows, flags='u')]
        shadowtimes = [x.split('|')[2] for x in grass.parse_command('t.rast.list', input=shadows, flags='u')]
        if len(strdsrasters) != len(shadowrasters):
            grass.fatal(_("Number of raster in <input> strds and <shadows> strds are not the same."))

    scenes = dict()
    for strdsrast, strdstime in zip(strdsrasters, strdstimes):
        scenes[strdsrast] = {'raster': strdsrast, 'date': strdstime}
        if clouds:
            if strdstime in cloudtimes:
                cloud_idx = cloudtimes.index(strdstime)
                scenes[strdsrast]['clouds'] = cloudrasters[cloud_idx]
            else:
                grass.warning(_("For <%s> at <%s> no clouds found") % (strdsrast, strdstime))
        if shadows:
            if strdstime in shadowtimes:
                shadow_idx = shadowtimes.index(strdstime)
                scenes[strdsrast]['shadows'] = cloudrasters[shadow_idx]
            else:
                grass.warning(_("For <%s> at <%s> no clouds found") %
                              (strdsrast, strdstime))
    scene_keys = 'raster'

    num_scenes = len(scenes)
    if options['clouds']:
        # parallelize
        queue_mapcalc = ParallelModuleQueue(nprocs=nprocs)
        scene_keys = 'noclouds'
        grass.message(_("Set clouds in rasters to null() ..."))
        for scene_key, num in zip(scenes, range(num_scenes)):
            grass.message(_("Scene %d of %d ...") % (num+1, num_scenes))
            scene = scenes[scene_key]
            if options['cloudbuffer']:
                noclouds = "%s_nocloudstmp" % scene['raster']
            else:
                noclouds = "%s_noclouds" % scene['raster']
            scenes[scene_key]['noclouds'] = noclouds
            rm_rasters.append(noclouds)
            expression = ("%s = if( isnull(%s) ||| %s == 0, %s, null() )"
                          % (noclouds, scene['clouds'], scene['clouds'],
                          scene['raster']))

            # grass.run_command('r.mapcalc', expression=expression, quiet=True)
            module_mapcalc = Module('r.mapcalc', expression=expression,
                                    run_=False)
            queue_mapcalc.put(module_mapcalc)
        queue_mapcalc.wait()

        # buffer
        if options['cloudbuffer']:
            # parallelize
            queue_buffer = ParallelModuleQueue(nprocs=nprocs)
            for scene_key, num in zip(scenes, range(num_scenes)):
                grass.message(_("Cloud buffer %d of %d ...") % (num+1, num_scenes))
                scene = scenes[scene_key]
                noclouds_buf = "%s_noclouds" % scene['raster']
                scenes[scene_key]['noclouds'] = noclouds_buf
                rm_rasters.append(noclouds_buf)
                if float(options['cloudbuffer']) < 0:
                    buffer = float(options['cloudbuffer'])
                else:
                    buffer = -1.0 * float(options['cloudbuffer'])

                # grass.run_command('r.grow', input=noclouds, output=noclouds_buf, radius=buffer, quiet=True)
                module_buffer = Module('r.grow', input=noclouds,
                                       output=noclouds_buf, radius=buffer,
                                       run_=False)
                queue_buffer.put(module_buffer)
            queue_buffer.wait()

    if options['shadows']:
        # parallelize
        queue_mapcalc = ParallelModuleQueue(nprocs=nprocs)
        old_key = scene_keys
        scene_keys = 'noshadows'
        grass.message(_("Set shadows in rasters to null() ..."))
        for scene_key, num in zip(scenes, range(num_scenes)):
            grass.message(_("Scene %d of %d ...") % (num+1, num_scenes))
            scene = scenes[scene_key]
            if options['shadowbuffer']:
                noshadows = "%s_noshadowtmp" % scene['raster']
            else:
                noshadows = "%s_noshadows" % scene['raster']
            rm_rasters.append(noshadows)
            scenes[scene_key]['noshadows'] = noshadows
            expression = ("%s = if( isnull(%s) ||| %s == 0, %s, null() )"
                          % (noshadows, scene['shadows'],
                          scene['shadows'], scene[old_key]))

            # grass.run_command('r.mapcalc', expression=expression, quiet=True)
            module_mapcalc = Module('r.mapcalc', expression=expression,
                                    run_=False)
            queue_mapcalc.put(module_mapcalc)
        queue_mapcalc.wait()

        # buffer
        if options['cloudbuffer']:
            # parallelize
            queue_buffer = ParallelModuleQueue(nprocs=nprocs)
            for scene_key, num in zip(scenes, range(num_scenes)):
                grass.message(_("Shadow buffer %d of %d ...") % (num+1, num_scenes))
                scene = scenes[scene_key]
                noshadows_buf = "%s_noshadows" % scene['raster']
                scenes[scene_key]['noshadows'] = noshadows_buf
                rm_rasters.append(noshadows_buf)  # TODO das hier scheint nicht zu funktionieren?!
                if float(options['shadowbuffer']) < 0:
                    buffer = float(options['shadowbuffer'])
                else:
                    buffer = -1.0 * float(options['shadowbuffer'])

                # grass.run_command('r.grow', input=noshadows, output=noshadows_buf, radius=buffer, quiet=True)
                module_buffer = Module('r.grow', input=noshadows,
                                       output=noshadows_buf,
                                       radius=buffer, run_=False)
                queue_buffer.put(module_buffer)
            queue_buffer.wait()

    # histogramm matching for ALL input scenes at once !
    # add histogramm matching as an option to t.rast.aggregate 
    # to be applied to the sets of maps to be aggregated
    grass.message(_("Compute histogramm matching ..."))
    nocloudnoshadows_rasters = [val[scene_keys] for key, val in scenes.items()]
    grass.run_command('i.histo.match', input=nocloudnoshadows_rasters,
                      suffix='match', max=options['max'], quiet=True)
                      # output='match',
    newscenekey = "%s.match" % scene_keys
    for scene_key in scenes:
        scenes[scene_key][newscenekey] = scenes[scene_key][scene_keys] + '.match'
        rm_rasters.append(scenes[scene_key][newscenekey])
    scene_keys = newscenekey

    nocloudnoshadows_rasters = [val[scene_keys] for key, val in scenes.items()]

    if flags['q']:
        grass.message(_("Compute quart1 for each pixel over time ..."))
        quart1 = "tmp_quart1_%s" % os.getpid()
        rm_rasters.append(quart1)
        grass.run_command('r.series', input=nocloudnoshadows_rasters, output=quart1, method='quart1')

    grass.message(_("Compute median for each pixel over time ..."))
    median = "tmp_median_%s" % os.getpid()
    rm_rasters.append(median)
    grass.run_command('r.series', input=nocloudnoshadows_rasters, output=median, method='median')

    grass.message(_("Compute approximations for each pixel over time ..."))
    lwr_param = [{'order': 2, 'dod': 5},
                 {'order': 2, 'dod': 4},
                 {'order': 2, 'dod': 3},
                 {'order': 2, 'dod': 2},
                 {'order': 2, 'dod': 1},
                 {'order': 1, 'dod': 5},
                 {'order': 1, 'dod': 4},
                 {'order': 1, 'dod': 3},
                 {'order': 1, 'dod': 2},
                 {'order': 1, 'dod': 1}]
    lwr_suffix_list = []
    # parallelize
    queue_lwr = ParallelModuleQueue(nprocs=nprocs)
    for lwr_p in lwr_param:
        if len(nocloudnoshadows_rasters) > (lwr_p['order'] + lwr_p['dod']):
            grass.message("Approximation with order %d and dod %s ..." % (lwr_p['order'], lwr_p['dod']))
            lwr = "tmp_%s_lwr_o%d_d%d" % (os.getpid(), lwr_p['order'], lwr_p['dod'])
            lwr_suffix_list.append(lwr)

            # grass.run_command('r.series.lwr', input=nocloudnoshadows_rasters, suffix=lwr, flags='i',**lwr_p, quiet=True)
            module_lwr = Module('r.series.lwr',
                                input=nocloudnoshadows_rasters,
                                suffix=lwr, flags='i', **lwr_p,
                                run_=False)
            queue_lwr.put(module_lwr)
    queue_lwr.wait()

    for lwr in lwr_suffix_list:
        lwr_raster_list = [x for x in grass.parse_command('g.list', type='raster', pattern="*%s" % lwr)]
        rm_rasters.extend(lwr_raster_list)

    grass.message(_("Patching each scene ..."))
    patched_rasters_list = []
    # parallelize
    queue_patch = ParallelModuleQueue(nprocs=nprocs)
    for scene_key, num in zip(scenes, range(num_scenes)):
        grass.message(_("Scene %d of %d ...") % (num+1, num_scenes))
        scene = scenes[scene_key]
        patch_list = [scene[scene_keys]]
        name = scene[scene_keys]
        patch_list.extend(["%s%s" % (name, x) for x in lwr_suffix_list])
        if flags['q']:
            patch_list.append(quart1)
        patch_list.append(median)
        patched = name.replace('_%s' % scene_keys, strdsout)
        # grass.run_command('r.patch', input=patch_list, output=patched)
        scenes[scene_key]['patched'] = patched
        patched_rasters_list.append(patched)
        module_patch = Module('r.patch', input=patch_list,
                              output=patched, run_=False)
        queue_patch.put(module_patch)
    queue_patch.wait()

    grass.message(_("Compute PATCHED count for each pixel over time ..."))
    count = "tmp_PATCHEDcount_%s" % os.getpid()
    rm_rasters.append(count)
    grass.run_command('r.series', input=patched_rasters_list,
                      output=count, method='count')
    r_univar = grass.parse_command('r.univar', map=count, flags='g')
    if int(r_univar['min']) < int(r_univar['max']):
        grass.warning(_("Not all gaps are closed"))
        # TODO fill gaps?

    grass.message(_("Create strds <%s> for patched filled scenes ...") % strdsout)
    input_info = grass.parse_command('t.info', input=strds)
    title, desc = False, False
    title_str, desc_str = None, None
    for key in input_info:
        if 'Title:' in key:
            title = True
        elif title:
            title = False
            if 'Description:' not in key:
                title_str = key.replace('|', '').strip()
        elif 'Description:' in key:
            desc = True
        elif desc:
            desc = False
            if 'Command history:' not in key:
                desc_str = key.replace('|', '').strip()
    grass.run_command('t.create', output=strdsout + '_tmp',
                      title=("%s gaps(/clouds/shadows) filled" % title_str) if title_str else "",
                      desc=("%s: gaps(/clouds/shadows) filled" % desc_str) if desc_str else "",
                      quiet=True)
    rm_strds.append(strdsout + '_tmp')

    # create register file
    registerfile = grass.tempfile()
    file = open(registerfile, 'w')
    for scene_key in scenes:
        scene = scenes[scene_key]
        file.write("%s|%s\n" % (scene['patched'], scene['date']))
        rm_rasters.append(scene['patched'])
    file.close()
    grass.run_command('t.register', input=strdsout + '_tmp',
                      file=registerfile, quiet=True)
    # remove registerfile
    grass.try_remove(registerfile)

    grass.message(_("Mosaicing the scenes with method %s ...") % options['method'])
    if not options['granularity'] == 'all':
        grass.run_command('t.rast.aggregate', input=strdsout + '_tmp',
                          output=strdsout, basename=strdsout,
                          granularity=options['granularity'],
                          method=options['method'], quiet=True,
                          nprocs=options['nprocs'])
    else:
        rasters = [x.split('|')[0] for x in grass.parse_command('t.rast.list', input=strdsout + '_tmp', flags='u')]
        grass.run_command('r.series', input=rasters, output=strdsout,
                          method=options['method'])
        grass.message(_("<%s> created") % strdsout)

    # grass.run_command('t.rast.aggregate', input=strdsout + '_tmp', output='Q1' + strdsout, basename='Q1' + strdsout, granularity=options['granularity'], method=options['method'], quiet=True, nprocs=options['nprocs'])
    # grass.run_command('t.rast.aggregate', input=strdsout + '_tmp', output='Q3' + strdsout, basename='Q3' + strdsout, granularity=options['granularity'], method='quart3', quiet=True, nprocs=options['nprocs'])
    # q1rasters = [x.split('|')[0] for x in grass.parse_command('t.rast.list', input='Q1' + strdsout, flags='u')]
    # q3rasters = [x.split('|')[0] for x in grass.parse_command('t.rast.list', input='Q3' + strdsout, flags='u')]
    # for q1,q3 in zip(q1rasters,q3rasters):
    #     formular = "(%s - 1.5*(%s - %s) )" % (q1, q3, q1)
    #     grass.run_command("r.mapcalc", expression="Q1_Q3Q1_%s = if (%s < 0, 0, %s)" % (q1.replace("Q1", ""), formular, formular))
    #     grass.message("<Q1_Q3Q1_%s> created" % q1.replace("Q1", ""))


if __name__ == "__main__":
    options, flags = grass.parser()
    atexit.register(cleanup)
    main()
