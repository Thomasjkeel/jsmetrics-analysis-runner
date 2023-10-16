# -*- coding: utf-8 -*-

"""
    Logger
"""

# imports
import os


# docs
__author__ = "Thomas Keel"
__email__ = "thomas.keel.18@ucl.ac.uk"
__status__ = "Development"


class DataListProgressLogger:
    def __init__(self, path_to_data_list_file):
        if not os.path.isfile(path_to_data_list_file):
            error_msg = "'%s' does not exist" % (path_to_data_list_file)
            raise FileNotFoundError(error_msg)
        self._input_data_list_file = path_to_data_list_file
        self._path, self._input_file_name = self._split_input_data_list_file()
        #  check that input is stored in correct place ('data_list' folder)
        self._check_input_stored_in_data_list_dir()
        #  get path to progress log dir so that logs can be output
        self._path_to_progress_logs = self._get_path_to_progress_logs()
        self.path_to_progress_log_file = os.path.join(
            self._path_to_progress_logs, "progress_log_" + self._input_file_name
        )
        self._make_progress_log_file()

    def _split_input_data_list_file(self):
        path, file_name = os.path.split(self._input_data_list_file)
        return path, file_name

    def _check_input_stored_in_data_list_dir(self):
        full_path = os.path.split(self._path)
        if not full_path[-1] == "data_lists":
            raise NameError(
                "'path_to_data_list_file' should be in the 'data_list' directory within given experiment"
            )

    def _get_path_to_progress_logs(self):
        full_path = os.path.split(self._path)
        path_to_progress_logs = os.path.join(*full_path[:-1], "progress_logs")
        if not os.path.exists(path_to_progress_logs):
            raise NotADirectoryError(
                "%s does not exist. Please make this a directory so progress logs can be stored"
                % (path_to_progress_logs)
            )
        return path_to_progress_logs

    def _make_progress_log_file(self):
        make_duplicate_file(self._input_data_list_file, self.path_to_progress_log_file)

    def _open_log_file(self, mode):
        return open(self.path_to_progress_log_file, mode=mode)

    def get_length_of_data_list(self):
        the_file = self._open_log_file(mode="r")
        counter = 0
        # Reading from file
        content = the_file.read()
        content_list = content.split("\n")
        for i in content_list:
            if i:
                counter += 1
        return counter

    def write_line(self, line_index_to_change, info_to_add):
        """
        TODO: make sure that info_to_add not already in string
        TODO: replace 'loaded data...' with 'metric run...' and then 'output_saved...' then 'done!'

        line_index_to_change : int
            Index of line in file to append string information too
        info_to_add : str
            Information to append to line
        """
        if not isinstance(info_to_add, str):
            raise TypeError("'info_to_add' needs to be string")
        path, file = os.path.split(self.path_to_progress_log_file)
        temp_file = os.path.join(path, "temp" + file)
        make_duplicate_file(self._input_data_list_file, temp_file)
        with self._open_log_file("r") as log_file:
            with open(temp_file, "w") as updated_log_file:
                for line_index, line in enumerate(log_file):
                    if line_index == line_index_to_change:
                        #  remove ... from line to update latest progress on file
                        line = line.split("...")[0]
                        line = (
                            line.split(os.linesep)[0] + "..." + info_to_add + os.linesep
                        )
                    updated_log_file.write(line)
            updated_log_file.close()
        log_file.close()
        os.remove(self.path_to_progress_log_file)
        os.rename(temp_file, self.path_to_progress_log_file)


def make_duplicate_file(path_to_file1, path_to_duplicate):
    with open(path_to_file1, "r") as file1:
        with open(path_to_duplicate, "w") as duplicate:
            for line in file1:
                duplicate.write(line)
