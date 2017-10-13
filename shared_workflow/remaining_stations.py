import glob
import sys
import os


def get_lines(stat_list_file, out_path, sample_size):
    ext = '.000'
    # out_path should be abspath
    old_outputs = glob.glob(os.path.join(out_path, '*%s' % ext))
    # we just need to look at the file size of at most (sample_size + 1) files
    old_output_samples = old_outputs[:sample_size + 2]
    #    print old_output_samples

    if len(old_output_samples) == 0:
        stations_completed = []
    else:
        file_sizes = [os.stat(v).st_size for v in old_output_samples]
        normal_size = max(file_sizes)
        old_outputs_good = [v for v in old_outputs if os.stat(v).st_size == normal_size]
        old_outputs_bad = list(set(old_outputs) - set(old_outputs_good))
        old_outputs_bad = [os.path.splitext(os.path.basename(x))[0] for x in
                           old_outputs_bad]  # just take the filename, no path, no extension name

        stations_completed = [os.path.splitext(os.path.basename(x).strip('hf_'))[0] for x in old_outputs_good]
        print ">>> Already done for %d stations" % len(stations_completed)
        print stations_completed

        print ">>> Incomplete stations(less than %d bytes)" % normal_size
        print old_outputs_bad

    # convert station list into a dictionary
    stations_dic = {}
    with open(stat_list_file, 'r') as fp:
        for line in fp.readlines():
            if line.startswith('#'):
                continue
            line_split = line.split()
            try:
                statcode = line_split[2]
            except IndexError:
                print >> sys.stderr, "Bad entry: %s" % line_split

            else:
                stations_dic[statcode] = [line, False]

    # mark stations that have been fully processed
    for s in stations_completed:
        try:
            stations_dic[s][1] = True
        except KeyError:
            print >> sys.stderr, "Bad key: %s" % s
            pass  # don't know what to do~

    # collect stations that need to be processed by excluding completed ones
    remaining_station_lines = []
    for key in stations_dic:
        line, completed = stations_dic[key]
        if not completed:
            remaining_station_lines.append(line)

    remaining_station_lines.sort()  # make sure we have a consistent order in the list
    print ">>> Stations to be processed"
    stations_names = [x.split()[-1].strip('\n') for x in remaining_station_lines]
    print stations_names

    return remaining_station_lines


def get_codes(stations_all, vel_path):
    ext = '.000'
    old_outputs = glob.glob(os.path.join(vel_path, '*%s' % ext))
    if len(old_outputs) == 0:
        stations_completed = []
    else:
        file_sizes = [os.stat(v).st_size for v in old_outputs]
        normal_size = max(file_sizes)

        old_outputs_good = [v for v in old_outputs if os.stat(v).st_size == normal_size]
        stations_completed = [os.path.splitext(os.path.basename(x))[0] for x in old_outputs_good]
        print ">>> Already done for %d stations" % len(stations_completed)
        print stations_completed

    #        print stations_completed
    #        print len(stations_completed)

    remaining_station_codes = list(set(stations_all) - set(stations_completed))
    remaining_station_codes.sort()
    print ">>> Stations to be processed"
    print remaining_station_codes
    return remaining_station_codes
