#!/usr/bin/env python
"""
Parse siteamp_diag.jsonl and print the cb_amp equations
with numerical values substituted in place of parameters.
"""

import json
import sys


def format_station_entry(entry):
    """Format a station-level entry from bb_sim.py."""
    print(f"  Station: {entry['station']}")
    print(f"  stat_vs={entry['stat_vs']}, vs30={entry['vs30']}, lfvs30ref={entry['lfvs30ref']}")
    print(f"  pga={entry['pga']}")
    print(f"  amp_func={entry['amp_func']}")


def format_cb_amp_entry(entry):
    """Format a cb_amp-level entry from siteamp_models.py, showing equations with numbers."""
    inp = entry["inputs"]
    calc = entry["a1100_calc"]

    print(f"  Inputs: vref={inp['vref']}, vsite={inp['vsite']}, vpga={inp['vpga']}, pga={inp['pga']}, version={inp['version']}")
    print(f"  Constants: scon_c={calc['scon_c']}, scon_n={calc['scon_n']}")
    print(f"  Coefficients at T=0: k1[0]={calc['k1_0']}, k2[0]={calc['k2_0']}, c10[0]={calc['c10_0']}")
    print()

    # fs_high(0) = (c10[0] + k2[0] * scon_n) * log(1100.0 / k1[0])
    c10_0 = calc["c10_0"]
    k2_0 = calc["k2_0"]
    scon_n = calc["scon_n"]
    k1_0 = calc["k1_0"]
    scon_c = calc["scon_c"]
    pga = inp["pga"]
    vpga = inp["vpga"]

    print(f"  fs_high(0) = (c10[0] + k2[0] * scon_n) * log(1100.0 / k1[0])")
    print(f"             = ({c10_0} + {k2_0} * {scon_n}) * log(1100.0 / {k1_0})")
    print(f"             = {calc['fs_high_0']}")
    print()

    branch = calc["vpga_branch"]
    print(f"  vpga={vpga} vs k1[0]={k1_0} => branch: {branch}")
    print()

    if branch == "fs_low" and "fs_low_detail" in calc:
        d = calc["fs_low_detail"]
        r = d["log_arg1 (vpga/k1[0])"]
        e = d["exp_term"]
        num = d["log_arg2_num (pga+scon_c*exp)"]
        den = d["log_arg2_den (pga+scon_c)"]
        log_arg2 = d["log_arg2 (num/den)"]

        print(f"  fs_low(T=0, vs30=vpga={vpga}, a1100=pga={pga}):")
        print(f"    = c10[0] * log(vs30 / k1[0]) + k2[0] * log((a1100 + scon_c * exp(scon_n * log(vs30 / k1[0]))) / (a1100 + scon_c))")
        print()
        print(f"    Part 1: log(vs30 / k1[0])")
        print(f"           = log({vpga} / {k1_0})")
        print(f"           = log({r})")
        print()
        print(f"    Part 2: exp(scon_n * log(vs30 / k1[0]))")
        print(f"           = exp({scon_n} * log({vpga} / {k1_0}))")
        print(f"           = {e}")
        print()
        print(f"    Part 3: numerator = a1100 + scon_c * exp_term")
        print(f"           = {pga} + {scon_c} * {e}")
        print(f"           = {num}")
        print()
        print(f"    Part 4: denominator = a1100 + scon_c")
        print(f"           = {pga} + {scon_c}")
        print(f"           = {den}")
        print()
        print(f"    Part 5: log_arg2 = numerator / denominator")
        print(f"           = {num} / {den}")
        print(f"           = {log_arg2}")
        if isinstance(log_arg2, (int, float)) and log_arg2 <= 0:
            print(f"           *** log({log_arg2}) would cause math domain error! ***")
        print()

    elif branch == "fs_mid":
        print(f"  fs_mid(T=0, vs30=vpga={vpga}):")
        print(f"    = (c10[0] + k2[0] * scon_n) * log(vpga / k1[0])")
        print(f"    = ({c10_0} + {k2_0} * {scon_n}) * log({vpga} / {k1_0})")
        print()

    elif branch == "fs_high":
        print(f"  fs_high(0) = {calc['fs_high_0']} (same as above)")
        print()

    print(f"  a1100 = pga * exp(fs_high(0) - fs_vpga(0))")
    print(f"        = {pga} * exp({calc['fs_high_0']} - fs_vpga(0))")
    print()


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <siteamp_diag.jsonl>")
        sys.exit(1)

    filepath = sys.argv[1]
    with open(filepath) as f:
        lines = f.readlines()

    entries = [json.loads(line) for line in lines if line.strip()]

    call_num = 0
    for entry in entries:
        if "station" in entry:
            print("=" * 70)
            format_station_entry(entry)
            print()
        elif "inputs" in entry:
            call_num += 1
            print(f"--- cb_amp call #{call_num} ---")
            format_cb_amp_entry(entry)
            print()


if __name__ == "__main__":
    main()
