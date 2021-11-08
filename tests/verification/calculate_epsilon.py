#!/usr/bin/env python2

import pandas as pd
import numpy as np

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("sim", help="path to Simulated IM csv")
parser.add_argument("emp", help="path to Empirical IM csv")
parser.add_argument("out", help="path to output epsilon IM csv")

args = parser.parse_args()


sim_im_data = pd.read_csv(args.sim, index_col=0)
emp_im_data = pd.read_csv(args.emp, index_col=0)

matched_ims = set(sim_im_data.columns.values).intersection(emp_im_data.columns.values)
im_names = list(matched_ims)

emp_im_data.columns = ["emp_" + IM for IM in emp_im_data.columns]
merged_data = sim_im_data.merge(emp_im_data, left_index=True, right_index=True)


print(im_names)
epsilon = {}

for im in im_names:
    if im == "component":
        epsilon[im] = {}
        for station in sim_im_data.index.values:
            epsilon[im][station] = "geom"
    else:
        emp_sigma = "emp_" + im + "_sigma"
        im_epsilon = im + "_epsilon"
        emp_im = "emp_" + im
        merged_data[im_epsilon] = (
            np.log(merged_data[im].values) - np.log(merged_data[emp_im])
        ) / merged_data[emp_sigma]

merged_data.sort_index(inplace=True)
merged_data.to_csv(
    args.out,
    columns=[
        "component",
        "PGA_epsilon",
        "PGV_epsilon",
        "CAV_epsilon",
        "AI_epsilon",
        "Ds575_epsilon",
        "Ds595_epsilon",
        "pSA_0.02_epsilon",
        "pSA_0.05_epsilon",
        "pSA_0.1_epsilon",
        "pSA_0.2_epsilon",
        "pSA_0.3_epsilon",
        "pSA_0.4_epsilon",
        "pSA_0.5_epsilon",
        "pSA_0.75_epsilon",
        "pSA_1.0_epsilon",
        "pSA_2.0_epsilon",
        "pSA_3.0_epsilon",
        "pSA_4.0_epsilon",
        "pSA_5.0_epsilon",
        "pSA_7.5_epsilon",
        "pSA_10.0_epsilon",
    ],
)
