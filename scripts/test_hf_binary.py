import numpy as np
import argparse
from qcore.timeseries import HFSeis
import sys

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('hf_bin', type=str)
    parser.add_argument('fd_ll', type=str)
#    parser.add_argument('--verbose', action="store_true")
    args = parser.parse_args()

#    show_msg=args.verbose
    show_msg = True

    hf_bin = args.hf_bin
    fd_ll = args.fd_ll
    try:
        hf=HFSeis(hf_bin)
    except ValueError:
        if show_msg: print "cannot read %s"%hf_bin
        sys.exit(1)

    #check for len(fd) == len(hf.stations.name)
    try:
        f=open(args.fd_ll)
    except:
        if show_msg: print "cannot open %s"%fd_ll
        sys.exit(1)
    else:
        fd_count=len(f.readlines()) 
    
    if fd_count != len(hf.stations.name):
        #failed the count check
        if show_msg: print "the staion count did not match the fd_ll"
        sys.exit(1)
    
    #check for empty station names

    for station in hf.stations.name:
        if station == '':
            #failed
            if show_msg: print "empty staion name detected, hf failed"
            sys.exit(1)
    #check for and vs ==0 (failed)
    if np.min(hf.stations.vs) == 0:
        if show_msg: print "some vs == 0, hf incomplete"
        sys.exit(1)

        
    #pass both check
    if show_msg: print "HF passed"
    sys.exit(0)
    
    
    
