"""Write README.md for one staged Dropbox folder, STAGING/<STAGE>/<FAULT>, from its MANIFEST.tsv.

The README says what the tars hold, how they were made (settings, code, jobs), and
any caveat that must travel with the data. Its facts are the FAULTS and STAGES
tables below: a fault's own "how" or "history" for a stage replaces the stage's.
The realisation list and sizes are read from MANIFEST.tsv.

Usage: python make_readme.py STAGE FAULT STAGING REPO_COMMIT
"""

import sys
from pathlib import Path

REPO = "github.com/ucgmsim/slurm_gm_workflow, branch nesi-cybershake-v26p6"

FAULTS = {
    "PalliserKai": {
        "where": "run on NeSI under RunFolder/Cybershake/v26p6 (Cybershake v26p5, processed as v26p6)",
        "stations": "17760 stations (fd_rt01-h0.100.ll)",
        "lf": "EMOD3D, run by Sung Bae on KISTI and uploaded to v26p5/LF/PalliserKai",
        "notes": [],
    },
    "WellTeast": {
        "where": "run on NeSI under RunFolder/Cybershake/v26p6 (Cybershake v26p5, processed as v26p6)",
        "stations": "18432 stations in HF (fd_rt01-h0.100.ll); 18431 in BB and IM",
        "lf": "EMOD3D, run by Sung Bae on KISTI and uploaded to v26p5/LF/WellTeast",
        "notes": [
            "REL04 has no LF (none was computed), so it has HF but no BB or IM.",
            "BB and IM omit station 320077e from every realisation. LF and HF disagree about it, so "
            "BB uses the canonical 18431-station list: HF's station order without 320077e "
            "(campaigns/cybershake_v26p6/wellteast_bb_stations_18431.txt).",
        ],
    },
    "HikWgtnmax": {
        "where": "run on NeSI under RunFolder/Cybershake/v26p5",
        "campaign": "campaigns/hikwgtnmax_v26p5/",
        "stations": "17186 stations (fd_rt01-h0.100.ll)",
        "lf": "EMOD3D, run on Cascade and uploaded as NetCDF (the new workflow's lf-to-xarray format) "
              "to v26p5/LF/HikWgtnmax/seis",
        "notes": [],
        "history": {
            "HF": [
                "Computed 2026-09-26/28 (array job 9325809). The HF had not been run before. The 1D "
                "model was named explicitly, because the v26p5 config gives a KISTI path that Sung "
                "Bae's generator drops on NeSI "
                "(campaigns/cybershake_v26p6/hf_1d_model_fallback.md).",
            ],
        },
    },
}

STAGES = {
    "HF": {
        "what": "stochastic high-frequency ground acceleration",
        "members": "<Rel>_HF.bin (qcore HFSeis format) and <Rel>_HF.log",
        "how": [
            "hf_sim.py from the old (pre-Cylc) workflow, as in NeSI's mrd87_4 environment, "
            "with the hb_high binary v5.4.5.3.",
            "1D velocity model Cant1D_v3-midQ_OneRay.1d, as every Cybershake config names it; "
            "direct rays only (rayset 1).",
            "dt 0.005, sdrop 50, kappa 0.045, rvfac 0.8, path_dur 11; each realisation's seed "
            "derived from its SRF name, as Sung Bae's generator does.",
        ],
        "history": [
            "These replace HF that Sung Bae ran in July 2026, which used hf_sim.py's default 1D model "
            "(Cant1D_v2-midQ_leer.1d) by accident "
            "(campaigns/cybershake_v26p6/hf_1d_model_fallback.md).",
            "They were rerun with OneRay on 2026-09-27/28 (job 9334892), with the original seeds "
            "and settings. The model change alters the waveforms by at most about 0.1%.",
        ],
    },
    "BB": {
        "what": "broadband ground acceleration (g)",
        "members": "<Rel>_BB.bin (qcore BBSeis format) and <Rel>_BB.log",
        "how": [
            "bb_sim.py from the old workflow at commit 2c6a79a8 of " + REPO + ". This reconciles "
            "the LF and HF station sets by name.",
            "HF: the OneRay runs uploaded to v26p5/HF/{fault}.",
            "flo 1.0 Hz, fmin 0.5, fmidbot 1.0, dt 0.005, no LF site amplification; site vs30 from "
            "non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.vs30.",
        ],
        "history": [
            "Built 2026-09-27/28 from the OneRay HF (jobs 9334893 and 9350278), replacing BB built on "
            "the leer HF. Every file's station records were checked byte for byte against the "
            "earlier BB. The waveforms differ from it by at most about 0.1%.",
        ],
    },
    "IM": {
        "what": "intensity measures",
        "members": "<Rel>.csv (one row per station and component), <Rel>_imcalc.info and <Rel>_im_calc.log",
        "how": [
            "IM_calculation's calculate_ims_mpi.py from NeSI's sarah2024 environment, run by Sung Bae's "
            "run_im_job_array.sl.",
            "Components 000, 090, ver, geom, rotd50 and rotd100_50. PGA, PGV, CAV, AI, Ds575, Ds595, "
            "MMI, and pSA at 31 periods from 0.01 to 10 s.",
        ],
        "history": [
            "Computed 2026-09-27/28 from the OneRay BB (jobs 9334894, 9348737 and 9350279-81). Against "
            "the leer-based IMs they replace, the median ratios were PGA 1.0003, PGV 1.0000 and "
            "pSA(0.1 s) 1.0003. 99.8% of station values moved by less than 0.12%.",
        ],
    },
}


def first_upper(text):
    return text[:1].upper() + text[1:]


def main():
    stage, fault, staging, commit = sys.argv[1], sys.argv[2], Path(sys.argv[3]), sys.argv[4]
    folder = staging / stage / fault
    rows = [ln.split("\t") for ln in (folder / "MANIFEST.tsv").read_text().splitlines()[1:] if ln.strip()]
    tars = sorted({r[0]: int(r[1]) for r in rows}.items())
    rels = [name[: -len(f"_{stage}.tar")] for name, _ in tars]
    f, s = FAULTS[fault], STAGES[stage]
    lines = [
        f"# Cybershake v26p5: {fault} {stage}",
        "",
        f"{first_upper(s['what'])} for {len(rels)} {fault} realisation{'s' if len(rels) != 1 else ''}: "
        f"{', '.join(rels)}.",
        f"{first_upper(f['where'])}. {first_upper(f['stations'])}.",
        "",
        f"Each realisation is one uncompressed tar, <Rel>_{stage}.tar, holding {s['members']}.",
        f"MANIFEST.tsv lists every member with its size and SHA-256, the SHA-256 of each tar, "
        f"and the NeSI path each member was packed from. In total: {sum(b for _, b in tars) / 1e9:.1f} GB.",
        "",
        "## How it was made",
        "",
        *[f"- {x.format(fault=fault)}" for x in f.get("how", {}).get(stage, s["how"])],
        f"- LF: {f['lf']}.",
        "",
        "## History",
        "",
        *[f"- {x}" for x in f.get("history", {}).get(stage, s["history"])],
    ]
    if f["notes"]:
        lines += ["", "## Notes", "", *[f"- {x}" for x in f["notes"]]]
    lines += ["", "## Provenance", "",
              f"Job scripts, checks and notes: {REPO}, `{f.get('campaign', 'campaigns/cybershake_v26p6/')}` "
              f"(commit {commit}).",
              "Packed and uploaded with `campaigns/dropbox_upload_v26p5/` from the same commit.", ""]
    (folder / "README.md").write_text("\n".join(lines))
    print(f"wrote {folder / 'README.md'} ({len(rels)} realisations)")


if __name__ == "__main__":
    main()
