VERSION=v25p10; FAULT=HopeCW; BASE="/scratch/projects/rch-quakecore/Cybershake/setup_files_from_dropbox/$VERSION/large_temp_files/tar/$VERSION"; \
for p in \
"$BASE/LF/$FAULT" \
"$BASE/HF/$FAULT" \
"$BASE/Sources/${FAULT}_Sources.tar" \
"$BASE/VMs/${FAULT}_VM.tar"; do \
  [[ -e "$p" ]] && echo "OK: $p" || echo "MISSING: $p"; \
done