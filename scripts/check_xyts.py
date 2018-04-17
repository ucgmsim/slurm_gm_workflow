"""
Script to read a binary file and print the number of zeros, total 64 bit integers and percentage of zeros.
Author: Jason Motha
Email: jason.motha@canterbury.ac.nz
"""

import struct
import argparse
import os

n_zeros = 0
n_nums = 0

parser = argparse.ArgumentParser()
parser.add_argument("file")

args = parser.parse_args()

with open(args.file, 'rb') as f:
	for b in iter((lambda:f.read(64)),''):
                n_vals = len(b) / 8
		values = struct.unpack('q' * n_vals, b)
                for value in values:
	    	    if value is 0:
			n_zeros += 1
		    n_nums += 1
		
print os.path.realpath(args.file)
print "Zeros: %d\nTotal numbers: %d\nPercentage: %f%%\n" % (n_zeros, n_nums, 100.0 * n_zeros / n_nums)

