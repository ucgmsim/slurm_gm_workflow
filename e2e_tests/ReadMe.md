## End to End test

The end to end test runs the automated workflow on a small subset of realisations and
checks that all result files are there, i.e. HF.bin, BB.bin and IM csv.  
The values of the IM csv are also compared against a benchmark file.

Running of the end to end test is straightforward and is done using the run_e2e_test.py 
script. 

```
usage: run_e2e_tests.py [-h] [--timeout TIMEOUT] [--sleep_time SLEEP_TIME]
                        [--stop_on_warning] [--stop_on_error] [--no_clean_up]
                        config_file

positional arguments:
  config_file           Config file for the end-to-end test

optional arguments:
  -h, --help            show this help message and exit
  --timeout TIMEOUT     The maximum time (in minutes) allowed for execution of
                        the slurm scripts
  --sleep_time SLEEP_TIME
                        Sleep time (in seconds) between mgmt db progress
                        checks.
  --stop_on_warning     Stop execution on warnings
  --stop_on_error       Stop execution on errors
  --no_clean_up         Prevent deletion of the test directory, even when
                        there are no errors.
```
 
The most important argument is the config file, which is discussed below.
For a larger end to end test timeout might also have to be increased, it is set to 
10 mins by default.

#### Config

The config file specifies, where the required data is located, i.e.
- VMs
- Sources
- fault list

and the benchmark data. 

```json
{
  "test_dir": "/nesi/nobackup/nesi00213/RunFolder/EndToEndTest/tests/",
  "data_dir": "/nesi/project/nesi00213/EndToEndTest/test_data_simple/Data",
  "fault_list": "/nesi/project/nesi00213/EndToEndTest/test_data_simple/list.txt",
  "bench_dir": "/nesi/project/nesi00213/EndToEndTest/test_1912/test_data_simple/benchmark",
  "test_checkpoint": true,
  "timeout": 10,
  "version": "16.1",
  "seed": 34580,
  "stat_file": "/nesi/project/nesi00213/StationInfo/archive/18p6_nonuniform/non_uniform_whole_nz_with_real_stations-hh400_v18p6.ll",
  "wrapper_config": "auto_config.yaml",
  "keep_dup_stations": false
}
```

The default config is good to go, and uses 2 realisations from the Hossack and 
RepongaereF4 fault, along with 1 realisation from the Mangatete fault.

To use a different set of realisations, create a new data dir and an different 
config file.  
It might be worth updating this approach in the future to allow specifying
which faults to run from the available test data.


#### Currently setup tests
##### Simple
5 realisations from the Hossack, RepongaereF4 and Mangatete fault.

#### Validation
Runs the realisations from the v19p3p12 validation run.