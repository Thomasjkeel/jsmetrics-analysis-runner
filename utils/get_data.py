#  -*- coding: utf-8 -*-

"""
    Extract data from list of file locations
    Built originally for dealing with CMIP6 ensemble on NERC's JASMIN supercomputer
"""

#  imports
import glob
import os
import re
import datetime
import itertools

#  docs
__author__ = "Thomas Keel"
__email__ = "thomas.keel.18@ucl.ac.uk"
__status__ = "Development"


#  globals -> TODO: this needs to be passed as an argument 
# JASMIN_DATE_FORMAT = "%Y%m"  # YYYYMM for SSP585
# JASMIN_DATE_RANGE_PATTERN = r"\d{6}-\d{6}" # for SSP585

JASMIN_DATE_FORMAT = "%Y%m%d"  # YYYYMMDD
JASMIN_DATE_RANGE_PATTERN = r"\d{8}-\d{8}"

CAVEATS_IN_DATA = ["ua_day_CESM2_historical_r10i1p1f1_gn",\
                  "ua_day_CESM2_historical_r4i1p1f1_gn"] 
# ua_day_CESM2_historical_r10i1p1f1_gn -> not all dates and first one
# ua_day_CESM2_historical_r4i1p1f1_gn -> being included in CESM2-FV2 for some reason


class GlobDataPathRetriever:
    """
    Will retrieve paths of data files given a data_directory and path using
    glob. Has options to save paths to output file.
    """

    def __init__(self, pathname, data_paths=None):
        """
        Parameters
        ----------
        pathname : str
            Path to data directory for data files search (including wildcards for glob)
        data_paths : list
            List of paths to data files or directories (will set to empty if not provided)

        Raises
        ----------
        TypeError
            When 'pathname' is not a str OR 'data_path' is not a list
        """
        if not isinstance(pathname, str):
            raise TypeError("'pathname' input needs to be string type")
        if not data_paths:
            data_paths = []
        if not isinstance(data_paths, list):
            raise TypeError("'data_paths' input needs to be list type")

        #  set values
        self.pathname = pathname
        self._data_paths = data_paths

    @property
    def data_paths(self):
        return self._data_paths

    @data_paths.setter
    def data_paths(self, data_paths):
        if not isinstance(data_paths, list):
            raise TypeError("'data_paths' input needs to be list type")
        self._data_paths = data_paths

    def _check_some_data_exists(self):
        """
        Checks pathname has at least 1 path using glob.iglob
        """
        one_val = itertools.islice(glob.iglob(self.pathname), 0, 1, 1)
        if not sum(1 for _ in one_val) == 1:
            raise ValueError("No data exists in given data_directory")

    def retrieve_data_paths(self, inplace=False, one_realisation=True):
        """
        Will retrieve relevant data files or paths given a root directory
        and string pattern with wildcards (using glob).
        NOTE: will overwrite existing data paths
        TODO: implement a runtime limit?

        Parameters
        ----------
        inplace : Boolean
            Will ovewrite current objects value for data_paths

        Returns
        ----------
        data_paths : list
            List of data files from the given data directory
        """
        #  create output list
        data_path_iterator = glob.iglob(self.pathname)
        data_paths = []
        if one_realisation: # TODO: move to new func
            run_models_list = [] 
        for data_file in data_path_iterator:
            file_name = data_file.split("/")[-1]
            file_no_date = re.sub("_\d{8}-\d{8}.nc", "", file_name)
            if file_no_date in CAVEATS_IN_DATA:
                continue
            if one_realisation:
                current_model_name =  re.sub('r\d{1,2}i\d{1,2}p\d{1,2}f\d{1,2}', '', file_name)
                if current_model_name in run_models_list:
                    continue
                else:
                    run_models_list.append(current_model_name)
            data_paths.append(data_file)

        #  return or overwrite
        if inplace:
            self.data_paths = data_paths
        else:
            return data_paths

    def save_data_paths(self, save_filepath, extension=".txt"):
        """
        Saves list to given output location.

        Parameters
        ----------
        save_filepath : str
            Path where to save output file
        extension : str
            e.g. '.txt', '.csv', '.tsv', etc (default:'.txt')
        """
        #  check output save filename
        save_filepath = check_output_file_extension(save_filepath, extension)

        #  save file
        with open(save_filepath, "w") as output:
            for data_path in self.data_paths:
                output.write(str(data_path) + os.linesep)
            output.close()

    def _save_temporary_data_paths(self, save_filepath, prefix=".temp_"):
        """
        Saves list to given temporary location.

        Parameters
        ----------
        save_filename : str
            Path where to save output file
        prefix : str
            Prefix for file (default='.temp_')
        """
        # add prefix to save file
        save_filepath, save_filename = os.path.split(save_filepath)
        new_save_filename = prefix + save_filename
        new_save_filepath = os.path.join(save_filepath, new_save_filename)

        #  save file
        self.save_data_paths(new_save_filepath)


def check_output_file_extension(output_file_name, extension):
    """
    Parameters
    ----------
    output_file_name : str
        Name of output file (should contain a max of one '.')
    extension : str
        e.g. '.txt', '.csv', '.tsv', etc.

    Returns
    ----------
    output_file_name : str
        Name of output file containing correct extension
    """
    file_parts = output_file_name.split(".")
    if not file_parts[-1] == extension.split(".")[-1]:
        if len(file_parts) >= 2:
            raise ValueError(
                "output file needs to have only '%s' extention" % (extension)
            )
        else:
            output_file_name = output_file_name + extension
    return output_file_name


class GlobDataPathRetrieverFromJASMIN(GlobDataPathRetriever):
    """
    Will retrieve paths of data files froma a path on NERC's JASMIN supercomputer using glob.
    Has options to save paths to output file.
    """

    def __init__(self, pathname, data_paths=None):
        super().__init__(pathname, data_paths)

    def subset_data_paths_by_date_range(
        self, date_range_start, date_range_end, inplace=False
    ):
        """
        Subsets the data path list/file by a given date range
        Parameters
        ----------
        date_range_start : int or float
            Start date to subset data_paths by. Must be in JASMIN file date format (i.e. YYYYMMDD)
        date_range_end : int or float
            End data to subset data_paths by. Must be in JASMIN file date format (i.e. YYYYMMDD)
        inplace : Boolean
            Will ovewrite current objects value for data_paths

        Returns
        ----------
        new_data_paths : list
            Paths to data within the date range
        """
        if not self.data_paths:
            raise KeyError(
                "No 'data_paths' found yet, please add data paths to object or run '.retrieve_data_paths(inplace=True)'"
            )
        new_data_paths = []
        for path in self.data_paths:
            start_date_in_range = False
            end_date_in_range = False
            try:
                path_obj = FilePathFromJASMIN.with_check_file_start_end_date_in_range(
                    path, date_range_start, date_range_end
                )
                start_date_in_range, end_date_in_range = (
                    path_obj.start_date_in_range,
                    path_obj.end_date_in_range,
                )
            except Exception as e:
                print(e)
            if start_date_in_range or end_date_in_range:
                new_data_paths.append(path)

        if inplace:
            self.data_paths = new_data_paths
        else:
            return new_data_paths


class DataPathGrouperForJASMIN:
    """
    Will group data paths into a list of lists
    For using in xarray.open_mfdataset() i.e. 'multi-file' open
    """

    def __init__(self, data_paths=None):
        """
        Parameters
        ----------
        data_paths : list
            List of paths to data files or directories (will set to empty if not provided)

        Raises
        ----------
        TypeError
            When 'data_path' is not a list
        """
        if not data_paths:
            data_paths = []
        if not isinstance(data_paths, list):
            raise TypeError("'data_paths' input needs to be list type")

        self._data_paths = data_paths

    @classmethod
    def from_file(cls, file_path):
        with open(file_path, "r") as file:
            data_paths = file.read().splitlines()
        return cls(data_paths)

    @property
    def data_paths(self):
        return self._data_paths

    @data_paths.setter
    def data_paths(self, data_paths):
        if not isinstance(data_paths, list):
            raise TypeError("'data_paths' input needs to be list type")
        self._data_paths = data_paths

    def group_data_paths(self, date_range_start, date_range_end):
        """
        Will group data in the data path list/file in a given date range
        Parameters
        ----------
        date_range_start : int or float
            Start date to subset data_paths by. Must be in JASMIN file date format (i.e. YYYYMMDD)
        date_range_end : int or float
            End data to subset data_paths by. Must be in JASMIN file date format (i.e. YYYYMMDD)

        Returns
        ----------
        new_data_paths : list
            Paths to data within the date range
        """
        iterable_data_paths = iter(self.data_paths)
        grouped_data_paths = []
        grouped_data_paths = group_data_paths_from_iterable(
            iterable_data_paths, grouped_data_paths, date_range_start, date_range_end
        )
        return grouped_data_paths

        
def group_data_paths_from_iterable(
    data_path_iteratable,
    output_data_paths_list,
    date_range_start,
    date_range_end,
    current_grouped_data_paths=None,
):
    data_path_iterable_main, data_path_iterable_copy  = itertools.tee(data_path_iteratable)
    if not current_grouped_data_paths:
        current_grouped_data_paths = []
    try:
        first_data_path = next(data_path_iterable_main)
        _ = next(data_path_iterable_copy)
        in_date_range = check_data_path_in_date_range(
            first_data_path, date_range_start, date_range_end
        )
        if in_date_range:
            current_grouped_data_paths.append(first_data_path)
            current_data_path_no_date = remove_date_from_path_name_JASMIN(
                first_data_path
            )
            try:
                next_data_path = next(data_path_iterable_main)
            except StopIteration:
                output_data_paths_list.append(current_grouped_data_paths)
                raise StopIteration
            next_data_path_no_date = remove_date_from_path_name_JASMIN(next_data_path)
            to_append_to_group = (
                difference_between_two_strings(
                    current_data_path_no_date, next_data_path_no_date
                )
                == 0
            )  
            while to_append_to_group:
                if check_data_path_in_date_range(
                    next_data_path, date_range_start, date_range_end
                ):
                    _ = next(data_path_iterable_copy)
                    current_grouped_data_paths.append(next_data_path)
                try:
                    next_data_path = next(data_path_iterable_main)
                except StopIteration:
                    output_data_paths_list.append(current_grouped_data_paths)
                    raise StopIteration
                next_data_path_no_date = remove_date_from_path_name_JASMIN(
                    next_data_path
                )
                to_append_to_group = (
                    difference_between_two_strings(
                        current_data_path_no_date, next_data_path_no_date
                    )
                    == 0
                )
            output_data_paths_list.append(current_grouped_data_paths)
            current_grouped_data_paths = []
        return group_data_paths_from_iterable(
            data_path_iterable_copy,
            output_data_paths_list,
            date_range_start,
            date_range_end,
            current_grouped_data_paths=current_grouped_data_paths,
        )
    except StopIteration:
        return output_data_paths_list


def remove_date_from_path_name_JASMIN(path_name):
    date_range = re.findall(pattern=JASMIN_DATE_RANGE_PATTERN, string=path_name)
    path_name_w_removed_date = path_name.replace(date_range[0], "")
    return path_name_w_removed_date


def difference_between_two_strings(seq1, seq2):
    """
    https://stackoverflow.com/questions/28423448/counting-differences-between-two-strings
    """
    count = 0
    for i in range(len(seq1)):
        try:
            if seq1[i] != seq2[i]:
                count += 1
        except IndexError:
            break
    return count


def check_data_path_in_date_range(data_path_name, date_range_start, date_range_end):
    fp = FilePathFromJASMIN.with_check_file_start_end_date_in_range(
        data_path_name,
        date_range_start,
        date_range_end,
    )
    return fp.start_date_in_range or fp.end_date_in_range


class FilePathFromJASMIN:
    """
    File path from NERC's JASMIN supercomputer with methods to check for whether it exists, its size, and subsetting options
    """

    def __init__(self, file_name):
        """
        Parameters
        ----------
        file_name : str
            Name of file or filepath to extract start and end date from
        """
        self.file_name = file_name
        self.file_dir, self.file_name = self._split_path_from_file()

    @classmethod
    def with_start_end_date_values(cls, file_name):
        """
        Extracts start and end date values from file in JASMIN file format (i.e. YYYYMMDD)

        Parameters
        ----------
        file_name : str
            Name of file or filepath to extract start and end date from
        """
        obj = cls(file_name)
        obj.start_date, obj.end_date = obj.get_start_end_date_from_file_name()
        return obj

    @classmethod
    def with_check_file_start_end_date_in_range(
        cls, file_name, date_range_start, date_range_end
    ):
        """
        Extracts start and end values from file and checks if these are within a given date range in JASMIN file format (i.e. YYYYMMDD)

        Parameters
        ----------
        file_name : str
            Name of file or filepath to extract start and end date from
        date_range_start : str or datetime.datetime
            Start date of date range
        date_range_end : str or datetime.datetime
            End date of date range

        Returns
        ----------
        obj : FilePathFromJASMIN
            File path object with start_date_in_range and end_date_in_range values

        """
        obj = cls(file_name)
        obj.start_date, obj.end_date = obj.get_start_end_date_from_file_name()
        (
            obj.start_date_in_range,
            obj.end_date_in_range,
        ) = obj.check_file_start_end_date_in_range(date_range_start, date_range_end)
        return obj

    def _split_path_from_file(self):
        """
        Will split file path from file name if necessary

        Returns
        ----------
        file_dir : str
            Directory where the file is located
        file_name : str
            File name with out the directory
        """
        file_dir, file_name = os.path.split(self.file_name)
        return file_dir, file_name

    def get_start_end_date_from_file_name(self):
        """
        Get the start and end date from a filename.
        Format is specific to how data is stored in /badc/cmip6 on NERC's JASMIN supercomputer

        Returns
        ----------
        start_date : datetime.datetime
            Start datetime in filename
        end_date : datetime.datetime
            End datetime in filename
        """
        #  Find all date_date pattern
        date_range = re.findall(
            pattern=JASMIN_DATE_RANGE_PATTERN, string=self.file_name
        )
        assert (
            len(date_range) == 1
        ), "Only one start and end date required in filename and needs to be in the style of 00000000-00000000"

        #  Get startdate and enddate from date_range
        start_date, end_date = date_range[0].split("-")
        return start_date, end_date

    def check_file_start_end_date_in_range(self, date_range_start, date_range_end):
        """
        Checks if the given file is within a given date range in JASMIN file format (i.e. YYYYMMDD)

        Parameters
        ----------
        date_range_start : str or datetime.datetime
            Start date of date range
        date_range_end : str or datetime.datetime
            End date of date range

        Return
        ---------
        start_date_in_range : Boolean
            Whether or not start date is within given date range
        end_date_in_range : Boolean
            Whether or not end date is within given date range
        """
        if not hasattr(self, "start_date") and not hasattr(self, "end_date"):
            raise KeyError(
                "'start_date' and 'end_date' not found in object. Please run '.get_start_end_date_from_file_name()'"
            )

        start_date_in_range = self._check_file_start_date_in_range(
            date_range_start, date_range_end
        )
        end_date_in_range = self._check_file_end_date_in_range(
            date_range_start, date_range_end
        )
        
        if self._check_file_start_and_end_range_is_in_range(
            date_range_start, date_range_end
        ):
            ##  Will overwrite the values if the range is includes the range the func is looking for
            start_date_in_range = True
            end_date_in_range = True
        
        return start_date_in_range, end_date_in_range
    def _check_file_start_date_in_range(self, date_range_start, date_range_end):
        """
        Checks if the start date in a given file is within a given date range in JASMIN file format (i.e. YYYYMMDD)

        Parameters
        ----------
        date_range_start : str or datetime.datetime
            Start date of date range
        date_range_end : str or datetime.datetime
            End date of date range

        Return
        ---------
        start_date_in_range : Boolean
            Whether or not start date is within given date range
        """
        if not hasattr(self, "start_date"):
            raise KeyError(
                "'start_date' not found in object. Please run '.get_start_end_date_from_file_name()'"
            )

        start_date_in_range = check_date_in_range(
            self.start_date,
            date_range_start,
            date_range_end,
            date_format=JASMIN_DATE_FORMAT,
        )
        return start_date_in_range

    def _check_file_end_date_in_range(self, date_range_start, date_range_end):
        """
        Checks if the end date in a given file is within a given date range in JASMIN file format (i.e. YYYYMMDD)

        Parameters
        ----------
        date_range_start : str or datetime.datetime
            Start date of date range
        date_range_end : str or datetime.datetime
            End date of date range

        Return
        ---------
        end_date_in_range : Boolean
            Whether or not start date is within given date range
        """
        if not hasattr(self, "end_date"):
            raise KeyError(
                "'end_date' not found in object. Please run '.get_start_end_date_from_file_name()'"
            )

        end_date_in_range = check_date_in_range(
            self.end_date,
            date_range_start,
            date_range_end,
            date_format=JASMIN_DATE_FORMAT,
        )
        return end_date_in_range

    def _check_file_start_and_end_range_is_in_range(self, date_range_start, date_range_end):
        """
        Checks if the start date and end date passes over the range in a given file is within a given date range in JASMIN file format

        Parameters
        ----------
        date_range_start : str or datetime.datetime
            Start date of date range
        date_range_end : str or datetime.datetime
            End date of date range

        Return
        ---------
        start_date_in_range : Boolean
            Whether or not start date is within given date range
        """
        if not hasattr(self, "start_date"):
            raise KeyError(
                "'start_date' not found in object. Please run '.get_start_end_date_from_file_name()'"
            )

        if not hasattr(self, "end_date"):
            raise KeyError(
                "'end_date' not found in object. Please run '.get_start_end_date_from_file_name()'"
            )
        
        date_format=JASMIN_DATE_FORMAT
        start_date = convert_to_datetime_no_error(self.start_date, date_format)
        end_date = convert_to_datetime_no_error(self.end_date, date_format)
        date_range_start = convert_to_datetime_no_error(date_range_start, date_format)
        date_range_end = convert_to_datetime_no_error(date_range_end, date_format)
        
        if start_date <= date_range_start and end_date >= date_range_end:
            return True
        else:
            return False
            

def check_date_in_range(date_to_check, date_range_start, date_range_end, date_format):
    """
    Parameters
    ----------
    date_to_check : str or datetime.datetime
        Date to check if within range
    date_range_start : str or datetime.datetime
        Start date of date range
    date_range_end : str or datetime.datetime
        End date of date range
    date_format : str
        Format of date for datetime.datetime.strptime

    Returns
    ----------
    output : boolean
        True or False if date is in range given
    """
    #  Convert to datetime
    date_to_check = convert_to_datetime_no_error(date_to_check, date_format)
    date_range_start = convert_to_datetime_no_error(date_range_start, date_format)
    date_range_end = convert_to_datetime_no_error(date_range_end, date_format)

    #  Check all can be used for date range comparison
    if not all([date_to_check, date_range_start, date_range_end]):
        raise ValueError("Some dates could not be converted to datetime")
    if date_to_check >= date_range_start and date_to_check <= date_range_end:
        return True
    else:
        return False


def convert_to_datetime_no_error(date, date_format):
    """
    Convert date to datetime without throwing error
    Parameters
    ----------
    date : str or datetime.datetime
        Date to convert
    date_format : str
        Format of date for datetime.datetime.strptime
    """
    if not isinstance(date, datetime.datetime):
        try:
            date = datetime.datetime.strptime(date, date_format)
        except Exception as e:
            print("Cannot convert %s to datetime.datetime" % (date), e)
            return
    return date
