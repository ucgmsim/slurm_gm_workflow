# import shared_srf
# from createSRF import focal_mechanism_2_finite_fault

'''
Generates all the CSV needed for SeisFinder App

Call this script from RunFolder/user/runName so it can find params.py for specific simulations

Usage example: python workflow/$version/CSVFromSRF.py -location Darfield -datetime 2011-9-10:04:30:0 -exetime 201705011613 -save ~/Darfeild_csv_tmp
Please make sure the values of each args are corret for different events.


Known issue:
    several attribute are not reachable/displaced.Thus when appending value '0' is used.

Steps:
1. create the event folder on seis-finder server
2. make sure the permission is set to 777(or 755)
3. generate the metaData by using this script
4. upload the metadata to the seis-finder server under event name folder
5. make sure permission for MetaData is 755 and CSVs are 644
6. Upload the Vel to the server under seismo folder
7. make sure the permission for seismo is 755 and the seismograms are 644
'''

import re
import os
from string import digits
import csv
import argparse
import sys
import datetime as dtl
import types
import glob
import srf as shared_srf
import shared
import xyts as shared_xyts

# will be used in re module, see python "Regular Expression" document for detail
numeric_const_pattern = "[-+]?\d+[\.]?\d*[eE]?[-+]?\d*?"
# used in args_check
datetime_pattern = "%Y-%m-%d-%H:%M:%S"  # e.g. 2010-08-20-04:30:00
datetime_pattern_date_b = "%Y%b%d"  # %b = abbreviated name. e.g. Jan,Feb...,Dec
exetime_pattern = "%Y%m%d%H%M"  # e.g. 20170420153000

sys.path.append(os.path.abspath(os.path.curdir))

try:
    from params import *
    # import params
except ImportError:
    print "params.py is not available, please run this script at Runfolder."
    sys.exit()
# read params_vel.py from specific directory
try:
    execfile(os.path.expanduser(params_vel))
except IOError:
    print "execfile() Failed reaching :%s" % os.path.expanduser(params_vel)
    sys.exit()

# global variables
magnitude = None
mp = ''  # 'm'+str(mag).replace('.','p')
name_sim = ''
name_event_global = ''
future_datetime = "3333-03-03 00:00:00"
timezone = "UTC"
mag_prefix = "Mw"

# getting Outbin and xyts dir
srf_name = os.path.splitext(os.path.basename(srf_files[0]))[0]
lf_outbin_dir = os.path.join(lf_sim_root_dir, srf_name, "OutBin")
xyts_dir = os.path.join(lf_outbin_dir, run_name + "_xyts.e3d")
# print "LF/OutBin directory: %s"%lf_outbin_dir
# print "xyts: ",xyts_dir


# List of CSVs variables
save_csv_root = "CSV/"
CSVDocument = "Document.csv"
CSVEvent = "Event.csv"
CSVRupture = "Rupture.csv"
CSVSegment = "Rupturesegment.csv"
CSVSimulation = "Simulation.csv"
CSVStation = "Station.csv"
CSVStationSet = "StationSet.csv"
CSVStation_StaionSet = "Station_StaionSet.csv"
CSVVelocity = "Velocity.csv"
CSVCorners = "Corners.csv"
# statgrid_directory = ""
CSVSubsurfaceGeneral = "Subsurface_general.csv"
CSVSubsurfaceStation = "Subsurface_per_station.csv"


def subsurface_general():
    data = [["#site_amp_model", "flowcap", "fmid", "fmidbot", "fmin", "fhigh", "fhightop", "fmax", "dt", "ft_len"],
            [site_amp_model, site_flowcap, site_fmid, site_fmidbot, site_fmin, site_fhigh, site_fhightop, site_fmax, dt,
             hf_t_len]]

    csvfile = open(os.path.join(save_csv_root, CSVSubsurfaceGeneral), 'w')
    CSVwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    CSVwriter.writerows(data)
    csvfile.close()
    print "\rGenerating %s Done" % CSVSubsurfaceGeneral


def subsurface_per_station():
    header = ["#station", "HF_PGA_000", "HF_PGA_090", "HF_PGA_ver", "vs30"]
    # getting station list
    run_bb_ll_file = glob.glob('run_bb_mpi_*.ll')
    if len(run_bb_ll_file) == 0:
        print "Could not find a run_bb_mpi*.ll file here"
        exit(1)
    # read run_bb_ll_file to find the seismo.log0 directory
    seismo_dir = ""
    with open(run_bb_ll_file[0]) as ll_template:
        lines = ll_template.readlines()
        for line in lines:
            if line.startswith("poe"):
                values = line.split(" ")
                seismo_dir = values[-1].strip()

    if seismo_dir == "":
        print "No directory for seismo.log found"
        exit(1)
    # see if seismo_dir has seismo.log or seismo.log0
    seismo_log_file = ""
    possible_files = ["seismo.log", "seismo.log0"]
    for file in possible_files:
        possible_seismo_file = os.path.join(seismo_dir, file)
        if os.path.isfile(possible_seismo_file):
            seismo_log_file = possible_seismo_file

    if seismo_log_file == "":
        print "Not found seismo.log file"
        exit(1)

    # parse seismo_log_file and get HF for different stations
    station_values = {}
    with open(seismo_log_file) as seismo_log:
        lines = seismo_log.readlines()
        for line in lines:
            # line example: 4002b60 000 0.039587 500 413.059814453125
            station, component, HF_val, vs_ref, vs_30 = line.split(" ")
            try:
                station_values[station][component] = HF_val
            except KeyError:
                station_values[station] = {}
                station_values[station]["vs_ref"] = vs_ref
                station_values[station]["vs_30"] = vs_30.strip()
                station_values[station][component] = HF_val

    data = [header]
    for station in station_values.keys():
        dict_values = station_values[station]
        for_output = [station, dict_values["000"], dict_values["090"], dict_values["ver"], dict_values["vs_ref"],
                      dict_values["vs_30"]]
        data.append(for_output)

    csvfile = open(os.path.join(save_csv_root, CSVSubsurfaceStation), 'w')
    CSVwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    CSVwriter.writerows(data)
    csvfile.close()
    print "\rGenerating %s Done" % CSVSubsurfaceStation


def gen_corners(xyts_file):
    try:
        xyts = shared_xyts.XYTSFile(xyts_file)
    except:
        print "Error accured while creating XYTS object"
        sys.exit()
    corners = xyts.corners()
    print "corners: ", corners
    corners.insert(0, ("#lon", "lat"))

    csvfile = open(os.path.join(save_csv_root, CSVCorners), 'w')
    CSVwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    CSVwriter.writerows(corners)
    csvfile.close()
    print "Generating %s Done" % CSVCorners


def gen_rup_segments(srf_files):
    # initialize csv
    segments = [('#rup', 'depth', 'lat', 'lon', 'f_len', 'f_wid', 'd_top', 's_hyp', 'd_hyp', 'stk', 'dip', 'rak')]
    ruptures = [('#NAME(unique)', 'short_name', 'DT', 'R_TYPE', 'PATH')]
    planes_list = []
    rup_names = []

    for srf_file in srf_files:
        try:
            f = open(os.path.expanduser(srf_file))
        except IOError:
            print " Cannot Open file:%s" % srf_file
            sys.exit()
        else:
            planes = shared_srf.read_header(f)
            # f.seek(0)
            # skip the line in datafile "POINTS"
            f.readline()
            # f is now at start of point datablock
            dt_rup = shared_srf.get_lonlat(f, value='dt')[2]
            name = name_event_global + '_' + os.path.splitext(os.path.basename(srf_file))[0]
            short_name = os.path.splitext(os.path.basename(srf_file))[0]
            for p in planes:
                elon, elat, nstk, ndip, flen, fwid, stk, dip, dtop, shyp, dhyp = p
                segments.append((name, '0', elat, elon, flen, fwid, dtop, shyp, dhyp, stk, dip, '0'))
            # rewind to start of file before reading lines
            f.seek(0)
            rup_names.append(name)
            path_rup_plot = 'data/' + name_sim + '/srf_map1.png'
            ruptures.append((name, short_name, dt_rup, shared_srf.check_type(f), path_rup_plot))
            f.close()

    csvfile = open(os.path.join(save_csv_root, CSVRupture), 'w')
    CSVwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    CSVwriter.writerows(ruptures)
    csvfile.close()
    print "\rGenerating %s Done" % CSVRupture

    #    planes_list.append( shared_srf.read_header(f) )
    csvfile = open(os.path.join(save_csv_root, CSVSegment), 'w')
    CSVwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    CSVwriter.writerows(segments)
    csvfile.close()
    print "\rGenerating %s Done" % CSVSegment
    return rup_names


def args_check(parser):
    parser.add_argument("-location", help="the location of event, eg. Darfield")
    parser.add_argument("-datetime", help="the date and time of the earth quake event")
    # parser.add_argument("-real", help="1 for real station, 0 for virtual")
    parser.add_argument("-mag", help="magnitude of the event", type=float)
    parser.add_argument("-save", help="save location of CSVs, when user does not have permission to runfolder")
    parser.add_argument("-exetime", help="the datetime for when simulation is executed.")

    # args = vars(parser.parse_args())

    # for value in args.values():
    #    if value is None:
    #            parser.error('Argument missing. Please provide all args needed')
    #            sys.exit()


    args = parser.parse_args()
    ####check for formate integrity ####
    if args.location is None:
        print "please provide event location(epicenter)"
        sys.exit()
    if args.exetime is None:
        print "Please specify when the simulation is executed by using -exetime %s" % exetime_pattern
        sys.exit()
    else:
        try:
            args.exetime = dtl.datetime.strptime(args.exetime, exetime_pattern)
            args.exetime = args.exetime.strftime(exetime_pattern)
            print "args.exetime: ", args.exetime

        except ValueError:
            print "exetime formate error, please provide the correct formate for execution time"
            print exetime_pattern
            sys.exit()

    if args.datetime is not None:
        try:
            args.datetime = dtl.datetime.strptime(args.datetime, datetime_pattern)
            print "datetime: ", args.datetime
        except ValueError:
            if args.datetime == "Future":
                pass
            else:
                print "Datetime formate error, please provide the correct formate of date time"
                print datetime_pattern
                sys.exit()
    else:
        print "datetime not provided, attemping to get datetime from run_name"
        args.datetime = run_name.split('_')[0]
        print args.datetime
        try:
            args.datetime == datetime.datetime.strptime(args.datetime, datetime_pattern)

        except ValueError:
            print "failed generating datetime with %s." % datetime_pattern
            print "trying %s" % datetime_pattern_date_b
            try:
                datetime.datetime.strptime(args.datetime, datetime_pattern_date_b)
            except ValueError:
                print "Cannot generate datetime from both patterns. Please provide datetime value with -date"
                sys.exit()
    # virtual stations can be identified by name.
    # if ( args.real is not '1') and ( args.real is not '0' ) :
    #    print "Please provide correct value for -real (1 or 0)"
    #    sys.exit()


    ##########setting global variables### 
    global magnitude
    if args.mag is not None:
        magnitude = args.mag
        print "Magnitude is :%s" % magnitude
    else:
        # magnitude = mag
        # magnitude = args.mag
        # print "Magnitude is :%s"%magnitude
        print "Magnitude is not specified"
        sys.exit()

    global mp
    mp = 'm' + str(magnitude).replace('.', 'p')

    # run name rule may change in the future, change this will affect every field that uses run name
    if (type(args.datetime) == str) and ("Future" in args.datetime):
        d_tmp = args.datetime
    else:
        d_tmp = args.datetime.strftime('%Y%m%d')
    global name_sim
    name_sim = str(d_tmp) + '_' + str(args.location) + '_' + str(mp) + '_' + str(args.exetime)
    print "name_sim: ", name_sim

    global name_event_global
    if (type(args.datetime) == str) and ("Future" in args.datetime):
        datetime_temp = args.datetime
    else:
        datetime_temp = args.datetime.strftime("%Y%m%d")
    name_event_global = str(datetime_temp) + '_' + args.location + '_' + mp

    ####################################
    return args


def gen_statgrid():
    #    try:
    #        f = open(os.path.expanduser(stat_file))
    #    except IOError:
    #        print "cannot open stat file: %s"%stat_file
    # f = os.path.expanduser(stat_file)
    f = os.path.expanduser(FD_STATLIST)
    # initialize csv
    statgrid = [("#NAME", "CODE(unique)", "REAL(0-True,1-False)", "LATITUDE", "LONGITUDE", "LOCATION_GIS(empty)")]

    stat_list, lats, lons = shared.get_stations(f, locations=True)

    for i in range(0, len(stat_list)):
        # determind if each station is virtual or real
        stat_code = stat_list[i]
        if len(stat_code) < 7:
            # virtual station is fixed with 7 ch long code, so any name with less are real stations.
            real = 'TRUE'
        elif stat_code.isupper():
            # string.isupper() will only return True when all alphabet in string is upper case, which indicates real station
            real = 'TRUE'
        else:
            real = 'FALSE'
        statgrid.append(('', stat_list[i], real, lats[i], lons[i], ''))
    '''
    while 1:
        line = f.readline().split()
        #print line
        
        if not line:
            break
        name = "" 
        code = line[2]
        latitude = line[1]
        longitude = line[0]
        locGIS = ""
       
        statgrid.append( (name,code,real,latitude,longitude,locGIS) )
    '''
    csvfile = open(os.path.join(save_csv_root, CSVStation), 'w')
    CSVwriter = csv.writer(csvfile, delimiter=',')
    CSVwriter.writerows(statgrid)
    csvfile.close()
    print "\rGenerating %s: Done" % CSVStation

    # generating StatioSet.csv
    stationset = [("#Name", "")]

    station_set_name = os.path.splitext(os.path.basename(stat_file))[0] + '_' + name_sim
    # station_set_name = os.path.splitext(os.path.basename(FD_STATLIST))[0]
    # print "station name:", station_set_name
    stationset.append((station_set_name, ""))
    # print "statset:",stationset

    csvfile = open(os.path.join(save_csv_root, CSVStationSet), 'w')
    CSVwriter = csv.writer(csvfile, delimiter=',')
    CSVwriter.writerows(stationset)
    csvfile.close()
    print "\rGenerating %s: Done" % CSVStationSet
    return station_set_name


def gen_velmodel():
    velmodel = []

    # name_vel = model_version[model_version.find("_")+1:]+"VM_"+model_version[:model_version.find("_")]
    name_vel = name_event_global + '_' + model_version + '_' + str(int(float(hh) * 1000)) + 'm'
    short_name_vel = model_version + '_' + str(int(float(hh) * 1000)) + 'm'
    # name_vel = output_directory+model_version+topo_type+str(float(hh))
    velmodel.append(("#Name(unique)", "short_name",
                     "magnitude",
                     "latitude", "longitude", "rotation", "hh_gridspace", "x_length", "y_length",
                     "z_top", "z_bottom", "min_S_wave", "topo", "centroidDepth"))
    velmodel.append((name_vel, short_name_vel,
                     magnitude,
                     MODEL_LAT, MODEL_LON, MODEL_ROT, hh, extent_x, extent_y,
                     extent_zmax, extent_zmin, min_vs, topo_type, centroidDepth
                     ))
    ######value check, check if any variable is None (missing in params_vel.py)#####

    for v in velmodel:
        for i in v:
            if i == '' or i is None:
                print '#' * 20
                print "There are empty values in params_vel.py."
                print "Please manaully fill in value in CSV or fix params_vel in %s" % params_vel
                print '#' * 20

    #######################################

    # generate Velocity.csv
    csvfile = open(os.path.join(save_csv_root, CSVVelocity), 'w')
    CSVwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    CSVwriter.writerows(velmodel)
    csvfile.close()
    print "\rGenerating %s: Done" % CSVVelocity
    return name_vel


def gen_document(location, datetime, exetime):
    # document naming rule may change, currently using event_name.pdf
    document = []
    # d_tmp = datetime.strftime('%Y%m%d')
    # name_sim = str(d_tmp)+'_'+str(location)+'_'+str(mp)+'_'+str(exetime)
    name_doc = name_sim + '.pdf'
    path_doc = 'data/' + name_sim + '/doc'
    document.append(("#NAME(unique)", "FILE_PATH"))
    document.append((name_doc, path_doc))

    csvfile = open(os.path.join(save_csv_root, CSVDocument), 'w')
    CSVwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    CSVwriter.writerows(document)
    csvfile.close()
    print "\rGenerating %s: Done" % CSVDocument
    return name_doc


def gen_event(location, datetime, exetime):
    event = []
    # !!!!change the event name if the naming rule changed in the future!!!!
    # name_event = str(datetime)+'_'+str(location)+'_'+str(mp)+'_'+str(exetime)
    if datetime == 'Future':
        # event name will be created without any datetime, since it is more of a scenario.
        name_event = str(location) + " " + (mag_prefix + str(magnitude))
        # below line will create event name as a preset-future time, instead of a string "future". timezone is also appended to the end.
        # name_event = str(location)+" "+(mag_prefix+str(magnitude))+" "+str(future_datetime)+" "+timezone
    else:
        # name_event is the name that will display on seisfinder app.
        name_event = str(location) + " " + (mag_prefix + str(magnitude)) + " " + str(datetime) + " " + timezone
    # d_tmp = datetime.strftime('%Y%m%d')
    # name_sim = str(d_tmp)+'_'+str(location)+'_'+str(mp)+'_'+str(exetime)
    # !!!change the path if structur of files changed in the future
    event_path = "data/" + name_sim + "/pgv-small.png"
    event.append(('#LOCATION', 'MAGNITUDE', 'DATE TIME (IN YYYY-MM-DD HH-MM-SS FORMAT)', 'PATH',
                  'NAME (unique-COMBINE LOCATION&MAGNITUDE&DATE TIME)'))
    # magnitude = run_name[run_name.find("_m")+2:run_name[run_name.find("_m")+2:].find("_")+run_name.find("_m")+2].replace("pt",".")
    # datetime_f = datetime.strftime('%Y-%m-%d %H-%M-%S')
    # Important: if ever required to append timezone to future_datetime or datetime, prosgresSQL only takes +-0~12, no abbreviation
    if datetime == 'Future':
        event.append((location, magnitude, future_datetime, event_path, name_event))
    else:
        event.append((location, magnitude, datetime, event_path, name_event))
        # print event
    csvfile = open(os.path.join(save_csv_root, CSVEvent), 'w')
    CSVwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    CSVwriter.writerows(event)
    csvfile.close()
    print "\rGenerating %s: Done" % CSVEvent
    return name_event


def gen_sim(event, rups, vel, doc, stat):
    sim = []
    path_sim_output = 'data/' + name_sim + '/seismo'
    sim.append(('#event(name)', 'rupture(name)', 'velocity(name)', 'document(name)', 'stationset(name)',
                'output_path', 'dt', 'nt', 'fhm', 'show'))
    for rup in rups:
        sim.append((event, rup, vel, doc, stat,
                    path_sim_output, dt, nt, flo, 0))
    csvfile = open(os.path.join(save_csv_root, CSVSimulation), 'w')
    CSVwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    CSVwriter.writerows(sim)
    csvfile.close()
    print "\rGenerating %s: Done" % CSVSimulation


# if called from other script, use *.main(argv), where argv = ['', '-flag1', 'value1', 'flage2', 'value2',.....]
def main(argv):
    parser = argparse.ArgumentParser()
    # check if all required args are provided
    args = args_check(parser)
    if args.save is not None:
        global save_csv_root
        save_csv_root = os.path.join(args.save, save_csv_root)

    try:
        os.mkdir(save_csv_root)
    except OSError:
        # print "directory already exsit"
        # sys.exit()
        pass

    # print "mag: ",magnitude
    global xyts_dir
    gen_corners(xyts_dir)

    name_rup_list = gen_rup_segments(srf_files)

    name_stat = gen_statgrid()

    name_vel = gen_velmodel()

    name_event = gen_event(args.location, args.datetime, args.exetime)

    # not yet implemented, waiting for shamila
    name_doc = gen_document(args.location, args.datetime, args.exetime)

    gen_sim(name_event, name_rup_list, name_vel, name_doc, name_stat)

    # generate general subsurface parameters
    subsurface_general()

    # generate per station subsurface meta-data
    subsurface_per_station()

    '''testing lines
    if args is not None:
        print args
        print args.ename
    '''


if __name__ == "__main__":
    main(sys.argv)
