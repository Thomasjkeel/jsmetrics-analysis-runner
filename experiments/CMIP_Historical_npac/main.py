# -*- coding: utf-8 -*-

"""
    Run Historical NPAC experiement

    NOTES ON EXPERIMENT TODO
"""

# imports
import logging
import os
import xarray
from utils import compute_jsmetrics, get_data, progress_loggers
from metric_dicts.jsmetrics_all_jet_lats_standard_npac_20to70N import METRIC_DICT

import experiments.CMIP_Historical_npac.get_sspxxx_data_list as get_ssp_data


import numpy as np  # TODO: remove
import matplotlib.pyplot as plt  # TODO: remove

# docs
__author__ = "Thomas Keel"
__email__ = "thomas.keel.18@ucl.ac.uk"
__status__ = "Development"


# globals for experiment

DATA_DIR = "/badc/cmip6/data/CMIP6/CMIP/"
EXPERIMENT = "historical"
SEARCH_CONDITIONS = "/**/**/%s/r*/day/ua/**/latest/*.nc" % (EXPERIMENT)
PATH_NAME = DATA_DIR + SEARCH_CONDITIONS
DATA_PATH_FILE = (
    "experiments/CMIP_Historical_npac/data_lists/ua_historical_latest_JASMIN.txt"
)
SUBSET_DATA_PATH_FILE = (
    "experiments/CMIP_Historical_npac/data_lists/ua_historical_latest_subset_JASMIN.txt"
)

START_DATE = "19500101"
END_DATE = "20151231"
OUTPUT_PATH = "experiments/CMIP_Historical_npac/outputs"

TEMPORARY_PLEV_SUBSET = slice(92500,70000) # TODO remove

assert (
    "/data_lists/" in DATA_PATH_FILE and "/data_lists/" in SUBSET_DATA_PATH_FILE
), "Data lists need to be in data_lists directory"


log = logging.getLogger(__name__)


def set_up_progress_logger():
    try:
        progress_logger = progress_loggers.DataListProgressLogger(SUBSET_DATA_PATH_FILE)
    except Exception as e:
        log.error("Unable to create progress logger. See below error message")
        log.error(e)
        raise e
    return progress_logger


def assert_logger_and_data_list_length_equal(progress_logger, data_list):
    data_list_length = progress_logger.get_length_of_data_list()
    assert data_list_length == len(
        data_list
    ), "Data list length not the same as the total in data paths input file"


def main():
    #  Step 0. Get data from JASMIN
    get_ssp_data.get_sspxxx_data_list(PATH_NAME, DATA_PATH_FILE)
    #  Step 1. Subset data list
    _ = make_data_list_date_subset(start_date=START_DATE, end_date=END_DATE)
    grouped_subset_data_paths = get_ssp_data.generate_grouped_sspxxx_data_paths(
        SUBSET_DATA_PATH_FILE, START_DATE, END_DATE
    )
    #  Step 3. Run experiment from subset list one by one
    for ind, data_path_group in enumerate(grouped_subset_data_paths):
        data_path_group_name = os.path.split(data_path_group[0])[-1][:-21]
        log.info(
            "Starting %s. %s out of %s. Total datsets in group: %s "
            % (
                data_path_group_name,
                ind + 1,
                len(grouped_subset_data_paths),
                len(data_path_group),
            )
        )
        # Step 3.1. read but not load data
        try:
            data = xarray.open_mfdataset(data_path_group)
            data = data.sel(plev=TEMPORARY_PLEV_SUBSET, time=slice(START_DATE[:4], END_DATE[:4]))
            log.info("Data head: %s" % (data.head()))
        except Exception as e:
            log.error('failed to load mfdataset, trying again with h5netcdf engine')
            try:
                data = xarray.open_mfdataset(data_path_group, engine='h5netcdf')
                data = data.sel(plev=TEMPORARY_PLEV_SUBSET, time=slice(START_DATE[:4], END_DATE[:4]))
                log.info("Data head (h5netcdf): %s" % (data.head()))
            except Exception as e:
                log.error(e)
                continue
        data_loaded = False
        # Step 3.2  Subset, run & save outputs of metric
        jsmetric_iterator = yield_metric_info_from_metric_dict(METRIC_DICT)
        for metric_info in jsmetric_iterator:
            metric_name = metric_info["name"]
            variable_name = metric_info["variable_name"]
            output_file_path = os.path.join(OUTPUT_PATH, data_path_group_name + metric_name + ".csv")
            if os.path.exists(output_file_path):
                log.info("output file already exists so assuming it has been calculated. File: %s" % (output_file_path))
                continue
            if not data_loaded:
                data.load()
                data_loaded = True
                log.info("%s sucessfully loaded" % (ind))
            ## Temporary fix before cf-array to rename poorly named dims
            if 'longitude' in data.coords:
                data = data.rename({'longitude':'lon'})
            if 'latitude' in data.coords:
                data = data.rename({'latitude':'lat'})
            #  Step 3.2.0 intialise the jsmetric computer
            try:
                jsmetric_computer = compute_jsmetrics.MetricComputer(data)
            except Exception as e:
                log.error("unable to make metric computer for %s" % (metric_name))
                log.error(e)
                continue

            #  Step 3.2.1  Subset data
            try:
                subset_data = jsmetric_computer.subset_data_for_metric(metric_info)
                log.info("subset for %s" % (metric_name))
                log.info("Subset data coords: %s" % (subset_data.coords))

            except Exception as e:
                log.error("unable to subset data for %s" % (metric_name))
                log.error(e)
                continue

            #  Step 3.2.2  Run metric on data
            try:
                output = jsmetric_computer.compute_metric_from_data(
                    metric_info, data=subset_data, to_subset=False
                )

                log.info("%s run" % (metric_name))
                log.info("Output data variables: %s" % (output.data_vars))
                # add metric X out of X run to progress_log file
            except Exception as e:
                log.error("unable to run %s" % (metric_name))
                log.error(e)
                continue

            #  Step 3.2.3  Save outputs
            try:
                print("saving to:", output_file_path)
                if metric_name == "Kerr et al. 2020 North Pacific":
                    print('taking mean for kerr')
                    output = output.mean('lon')
                save_output_to_file(output, variable_name, output_file_path)

                write_metadata_for_data_path_groups(
                    data_path_group, data_path_group_name
                )
                log.info("%s output saved to %s" % (metric_name, OUTPUT_PATH))
                # add metric X out of X run to progress_log file
            except Exception as e:
                log.error("unable to save output from %s" % (metric_name))
                log.error(e)
                continue
            print("%s done!" % (metric_name))  # TODO: remove
            # break  # TODO: remove
        print("%s done!" % (ind))  # TODO: remove
        # break  # TODO: remove


def make_data_list_date_subset(start_date, end_date):
    data_paths = []
    with open(DATA_PATH_FILE, "r") as path_file:
        for path in path_file:
            data_paths.append(path.strip(os.linesep))
    data_retriever = get_data.GlobDataPathRetrieverFromJASMIN(
        pathname="", data_paths=data_paths
    )
    log.info("Number of datasets found: %s" % (len(data_retriever.data_paths)))
    print("Number of datasets found:", len(data_retriever.data_paths))
    data_retriever.subset_data_paths_by_date_range(start_date, end_date, inplace=True)
    log.info(
        "Number of datasets after date range subset (%s to %s): %s"
        % (START_DATE, END_DATE, len(data_retriever.data_paths))
    )
    print("Number of datasets after date range subset:", len(data_retriever.data_paths))
    data_retriever.save_data_paths(SUBSET_DATA_PATH_FILE, ".txt")
    return data_retriever.data_paths


def yield_metric_info_from_metric_dict(metric_dict):
    for metric_info in metric_dict.values():
        yield metric_info

def write_metadata_for_data_path_groups(data_path_group, data_path_group_name):
    metadata_output_path = os.path.join(OUTPUT_PATH, "metadata")
    if not os.path.exists(metadata_output_path):
        os.mkdir(metadata_output_path)
    output_file_path = os.path.join(metadata_output_path, data_path_group_name + ".txt")
    with open(output_file_path, "w") as output_file:
        output_file.writelines("Metadata for:" + data_path_group_name + os.linesep)
        output_file.writelines(
            "The following datasets are used for this plot:" + os.linesep
        )
        for dpg in data_path_group:
            output_file.writelines(str(dpg) + os.linesep)
        output_file.close()


def save_output_to_file(output, max_lats_col, output_file_path):
    output_to_save = output[max_lats_col]
    output_to_save.to_dataframe().to_csv(output_file_path)
