# jsmetrics_analysis_runner

[![DOI](https://zenodo.org/badge/705595703.svg)](https://zenodo.org/doi/10.5281/zenodo.10876824)

This code details an analysis runner for data on the JASMIN supercomputer. It runs jet latitude statistics using the _jsmetrics_ Python package.

### How to run analysis-runner:
Open up a bash terminal on JASMIN and run:
```
python run_cmip_Historical_npac.py
```
Alternatively, if you need more RAM to run the analysis, you can use SLURM on JASMIN:
```
sbatch run_cmip_Historical_npac
```

### How to change the specification of the analysis being run:
1. Create a specification file detailing all the data subsetting and a list of metrics you want to run. Store this file under `metric_dict/`. An example of the correct format expected is provided in `metric_dict/jsmetrics_all_jet_lats_standard_npac_20to70N.py`.
2. Copy across the content of `experiments/CMIP_Historical_npac/` to a new directory `experiments/[MY_NEW_EXPERIMENT]`
3. Change which specification file is imported in `experiments/[MY_NEW_EXPERIMENT]/main.py`, i.e.:
   ```
   from metric_dicts.[YOUR_NEW_SPECIFICATION_FILE] import METRIC_DICT
   ```
4. Create a new header file which runs the experiment i.e. in the format of run_cmip_Historical_npac.py like: `run_[MY_EXPERIMENT_NAME].py`
5. Run analysis as shown in "How to run analysis-runner"

**NOTE**: If you are planning on running multiple different subsets, then either extend the metric specification file or create multiple experiment directories (under `experiments/`) for each 
