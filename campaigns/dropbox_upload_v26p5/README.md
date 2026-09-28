# Uploading the Cybershake v26p5 results to Dropbox

The results finished on NeSI go into Dropbox's working layout for this version,
next to the LF, Sources and VMs already there:

    dropbox:/QuakeCoRE/gmsim_scratch/v26p5/<STAGE>/<Fault>/<Rel>_<STAGE>.tar

This follows the convention of v25p10, v25p11 and v26p4: one uncompressed tar
per realisation, holding files named after it, with the median as
`<Fault>_<STAGE>.tar`. Two additions go in each folder:
- a `README.md` saying how the results were made, with any caveats;
- a `MANIFEST.tsv` giving every member's size and SHA-256, each tar's SHA-256,
  and the NeSI path each member came from.

| Folder | Realisations | Approx. size |
|---|---|---|
| `HF/PalliserKai`, `BB/PalliserKai`, `IM/PalliserKai` | median + REL01–37 (38) | 474 GB, 479 GB, 3 GB |
| `HF/WellTeast` | median + REL01–32 (33, including REL04) | 432 GB |
| `BB/WellTeast`, `IM/WellTeast` | 32 (no REL04: it has no LF) | 424 GB, 3 GB |
| `HF/HikWgtnmax` (later `BB`, `IM`) | median + REL01–50 (51) | 747 GB (BB 753 GB) |

On NeSI these files live in `/home/arr65/dropbox_upload_v26p5/`. The staging area
mirrors the Dropbox layout under
`/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/dropbox_staging/`.

| File | Role |
|---|---|
| `pack_stage.py` | Packs one stage for a list of realisations and verifies every tar (below). |
| `pack_stage.sl` | Slurm wrapper: runs `pack_stage.py`, then `make_readme.py` for each fault folder. |
| `make_readme.py` | Writes a folder's `README.md` from the facts it holds and the folder's `MANIFEST.tsv`. |
| `upload_stage.sl` | Uploads one staged folder and verifies it on Dropbox (below). |

## Safeguards

**Packing** (`pack_stage.py`):
- Each source must be the finished output:
  - HF names the OneRay model and says "Simulation completed";
  - BB.bin has the size its header implies;
  - the IM CSV is not empty.
- Each member is SHA-256'd from its source as it is written. The tar is then
  re-read, and every member must give the same hash.
- Tars are written as `.partial` and renamed once verified, so a finished tar is
  never overwritten, and a rerun skips what's done.

**Uploading** (`upload_stage.sl`):
- Before copying:
  - every tar in `MANIFEST.tsv` must be present at its recorded size and SHA-256,
    with no stray or partial tar;
  - `README.md` must be present.
- `rclone copy --immutable`: a file already on Dropbox is never modified or
  deleted. A same-named file with other content stops the job.
- After copying, `rclone check --one-way` must report 0 differences, no file it
  couldn't hash, and a match for every local file. These are the checks in
  `old_workflow_lf_cleanup`'s `lfc_verify.sh`.
- Uploads run under one job name with `--dependency=singleton`, so only one talks
  to Dropbox at a time.

## Tests

- **Locally, on synthetic realisations:**
  - The packer packs good outputs and refuses a leer-model HF and a truncated BB.
  - It skips a realisation with only HF, and on a rerun skips what's done.
  - It rebuilds a leftover `.partial`.
  - Its manifest hashes match the extracted members.
- **Locally, uploading to a directory in place of Dropbox:**
  - A dry run copies nothing.
  - An upload verifies all files, and a rerun skips them.
  - A different file of the same name at the destination stops the copy
    ("immutable file modified").
  - A stray tar in the staged folder is refused.
- **On NeSI:** WellTeast REL01's HF, BB and IM are packed into a test staging
  folder (job 9353529), then a `--dry-run` is made against the real Dropbox
  destination.

## Running

The realisation lists are `targets.txt` from `../cybershake_v26p6/oneray_redo/`
(on NeSI in `/home/arr65/v26p6_oneray_redo/`) and from `../hikwgtnmax_v26p5/bb_im/`.

```bash
cd /home/arr65/dropbox_upload_v26p5
# pack: writes only under the staging area
for s in HF BB IM; do sbatch pack_stage.sl $s /home/arr65/v26p6_oneray_redo/targets.txt <commit>; done
# upload, one folder at a time
for s in HF BB IM; do for f in PalliserKai WellTeast; do
    sbatch --dependency=singleton upload_stage.sl $s $f; done; done
```

Each verified upload is recorded in `uploads.txt`. The staged tars can be
deleted once their upload is verified. They are copies, and `MANIFEST.tsv`
keeps their checksums.
