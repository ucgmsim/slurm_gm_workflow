
import argparse
from qcore.timeseries import BBSeis
import sys

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('bb_bin', type=str)
    parser.add_argument('fd_ll', type=str)
#    parser.add_argument('--verbose', action="store_true")
    args = parser.parse_args()

#    show_msg=args.verbose
    show_msg = True

    bb_bin = args.bb_bin
    fd_ll = args.fd_ll
    try:
        bb=BBSeis(bb_bin)
    except ValueError:
        if show_msg: print "cannot read %s"%bb_bin
        sys.exit(1)

    #check for len(fd) == len(hf.stations.name)
    try:
        f=open(args.fd_ll)
    except:
        if show_msg: print "cannot open %s"%fd_ll
        sys.exit(1)
    else:
        fd_count=len(f.readlines()) 
    
    if fd_count != len(bb.stations.name):
        #failed the count check
        if show_msg: print "the staion count did not match the fd_ll"
        sys.exit(1)
    
    #check for empty station names

    for station in bb.stations.name:
        if station == '':
            #failed
            if show_msg: print "empty staion name detected, bb failed"
            sys.exit(1)
        
    #pass both check
    if show_msg: print "BB passed"
    sys.exit(0)
    
    
    
