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
- cybershake_config
- fault list

and the benchmark data. 

```json
{
  "test_dir": "/nesi/nobackup/nesi00213/RunFolder/EndToEndTest/tests/",
  "data_dir": "/nesi/nobackup/nesi00213/RunFolder/EndToEndTest/test_data/Data",
  "cybershake_config": "/nesi/nobackup/nesi00213/RunFolder/EndToEndTest/test_data/cybershake_config.json",
  "fault_list": "/nesi/nobackup/nesi00213/RunFolder/EndToEndTest/test_data/list.txt",
  "bench_dir": "/nesi/nobackup/nesi00213/RunFolder/EndToEndTest/benchmark"
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