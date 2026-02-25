# BB Simulation Hanging Analysis

## Symptom

`bb_sim.py` runs for a few minutes with multiple MPI ranks on a cluster, then hangs
indefinitely until SLURM kills it at the wall clock limit. Succeeds occasionally.

## Root Causes Identified

### 1. Concurrent POSIX writes to shared BB.bin (PRIMARY)

At `workflow/calculation/bb_sim.py:403`, every MPI rank opens `BB.bin` for read+write
simultaneously:

```python
bin_data = open(args.out_file, "r+b")
```

Each rank then does interleaved seek+write in the main loop (lines 528-532). On a
network/parallel filesystem (Lustre, GPFS, NFS), this causes:

- Byte-range lock contention across all nodes writing to the same file
- Buffered writes from Python's `open()` amplify lock holding time
- The filesystem's distributed lock manager can deadlock or stall indefinitely
- No `flush()` between writes means buffered data accumulates and causes large
  contention bursts when flushed

### 2. Repeated open/seek/close on shared HF.bin

The qcore `timeseries.HFSeis.acc()` method opens, seeks, reads, and closes `HF.bin`
for **every station** processed:

```python
def acc(self, station, ...):
    with open(self.path, "r") as data:
        data.seek(...)
        ts = np.fromfile(data, ...)
```

With 12 ranks processing hundreds of stations each, this creates thousands of
concurrent file open/close cycles. Each `open()` requires a metadata lookup on the
network filesystem, and each `close()` may trigger cache invalidation.

LF files have the same pattern but are spread across multiple `seis-*.e3d` files,
reducing contention somewhat.

### 3. MPI Barrier amplifies any single-rank hang

At `bb_sim.py:541`:

```python
comm.Barrier()
```

If **any single rank** stalls on a filesystem operation, **all other ranks** hang at
this barrier forever. This explains the pattern: fast ranks finish and wait, while one
slow rank is stuck on I/O.

### 4. Pipe buffer saturation from capture_output in launcher

In `run_bb_command_for_realisation.py:84`:

```python
result = subprocess.run(command, shell=True, capture_output=True, text=True)
```

`capture_output=True` collects all stdout/stderr into memory via pipes. With `srun`
and multiple MPI ranks printing to stdout, the pipe buffer (typically 64KB on Linux)
can fill up, causing MPI processes to block on stdout writes. Secondary issue but can
exacerbate hangs.

## Solutions

### Applied: Solution A - flush() after writes (zero risk to numerics)

Added `bin_data.flush()` after each station's write in `bb_sim.py`. This forces
buffered data to the filesystem immediately, reducing lock holding time and preventing
large contention bursts.

### Applied: Solution D - Log file instead of capture_output (zero risk to numerics)

Changed `run_bb_command_for_realisation.py` to redirect srun output to a log file
(`BB/Acc/bb_run.log`) instead of capturing via pipes. Eliminates pipe buffer
saturation risk.

### If problem persists: Solution B - MPI I/O (recommended next step)

Replace raw POSIX `open()` with `MPI.File` which is designed for coordinated parallel
writes on HPC filesystems:

```python
# Replace:
bin_data = open(args.out_file, "r+b")
# ...
bin_data.seek(bin_seek[i])
bb_acc.tofile(bin_data)

# With:
mpi_file = MPI.File.Open(comm, args.out_file, MPI.MODE_WRONLY)
# In the loop:
mpi_file.Write_at(bin_seek[i], bb_acc)
mpi_file.Write_at(bin_seek_vsite[i], vs30s[stations_todo_idx[i]])
# After the loop:
mpi_file.Close()
```

Risk to numerics: None. Same data written at same offsets, just using the MPI I/O
layer which coordinates with the parallel filesystem properly.

### If problem persists: Solution C - Per-rank temporary files (safest I/O pattern)

Each rank writes its station data to a separate file (`BB.bin.rank_N`), then rank 0
merges them after the barrier. This completely eliminates shared file write contention.

Outline:
1. Each rank writes to `BB.bin.rank_{rank}` instead of the shared file
2. After `comm.Barrier()`, rank 0 reads each rank file and writes to the final
   `BB.bin` at the correct offsets
3. Rank 0 deletes the temporary files

This requires more code changes but completely eliminates the I/O contention problem.
Risk to numerics: None.

### If problem persists: Solution E - Stagger file reads in main loop

Apply the same batched-barrier pattern already used in initialization (lines 144-149)
to the main loop's HF/LF file reads. This would serialize reads in batches of N ranks
to reduce filesystem metadata contention:

```python
READ_BATCH_SIZE = 4
for batch_start in range(0, size, READ_BATCH_SIZE):
    if batch_start <= rank < batch_start + READ_BATCH_SIZE:
        lf_acc = np.copy(lf.acc(stat.name, dt=bb_dt))
        hf_acc = np.copy(hf.acc(stat.name, dt=bb_dt))
    comm.Barrier()
```

Downside: significantly slows computation by serializing reads. Only use as last
resort.

### Longer-term: Cache file handles in qcore timeseries classes

The open/seek/read/close-per-call pattern in `HFSeis.acc()` and `LFSeis.vel()` is
inherently inefficient for HPC workloads. Modifying these classes to keep file handles
open across calls would dramatically reduce filesystem metadata pressure. This would
need to be done in the `qcore` package (`qcore/timeseries.py`).
