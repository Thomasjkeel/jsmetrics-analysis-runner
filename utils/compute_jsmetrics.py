# -*- coding: utf-8 -*-

"""
    Contains the MetricComputer class and functions that run inside that class for subsetting and compute metrics from standardised netcdf data
"""

import numpy

__author__ = "Thomas Keel"
__email__ = "thomas.keel.18@ucl.ac.uk"
__status__ = "Development"


ROUNDING_THRESHOLD = 3 # number of decimal places to round for coord check. Previously had a problem with 70000.00001 

EQUIVALENT_PLEV_UNITS = {
    "Pa": ["Pa", "Pascals", "mbar", "millibars"],
    "Pascals": ["Pa", "Pascals", "mbar", "millibars"],
    "mbar": ["Pa", "Pascals", "mbar", "millibars"],
    "millibars": ["Pa", "Pascals", "mbar", "millibars"],
}


class MetricComputer:
    """
    Metric Computer class for interacting, subsetting and calculating metrics from climate model outputs.

    Q: Why make a data formatter class and not just use functions to handle and format the data?
    A: When the data is in an object form, it can ... . Also, allows for information hiding.

    (see https://www.datacamp.com/community/tutorials/docstrings-python for docstring format)
    """

    def __init__(self, data):
        """ """
        self.data = data
        self.get_variable_list()
        self.swap_all_coords()

    def get_variable_list(self):
        """ """
        self.variable_list = []
        for var in self.data.keys():
            if "_bnds" not in var:
                self.variable_list.append(var)

    def swap_all_coords(self):
        """ """
        for coord in self.data.coords:
            if not self.data[coord].count() == 1:
                self.data = swap_coord_order(self.data, coord)

    def subset_data_for_metric(self, metric_info, ignore_coords={}):
        """
        Parameters
        ----------
        metric_info : dict
            jetstream metric information about metric name, subsetting, required variables, function location
        ignore_coords : array-like
            coordiantes to not subset

        Returns
        ----------
        subset: xarray.Dataset
            Subset of data using info from the jetstream metric dict
        """
        return subset_data_using_metric_coords(self.data, metric_info, ignore_coords)

    def compute_metric_from_data(
        self, metric_info, data=None, to_subset=True, ignore_coords={}
    ):
        """
        Parameters
        ----------
        metric_info : dict
            jetstream metric information about metric name, subsetting, required variables, function location
        to_subset : Boolean
            Whether the data needs to be subset or not
        ignore_coords : array-like
            coordiantes to not subset

        Returns
        ----------
        result : xarray.Dataset
            Result from metric
        """
        if to_subset:
            data = self.subset_data_for_metric(metric_info, ignore_coords)
        return compute_metric_using_metric_info(data, metric_info)


def subset_data_using_metric_coords(data, metric_info, ignore_coords=None):
    """
    Will subset the data based on the metric chosen

    Parameters
    ----------
    data : xarray.Dataset
        Data to subset using a given metric requirement
    metric_info : dict
        jetstream metric information about metric name, subsetting, required variables, function location
    ignore_coords : array-like
        coordiantes to not subset

    Returns
    ----------
    subset : xarray.Dataset
        Subset of input data given coords in metric_info
    """
    # overwrite which coords will be changed
    coords_to_subset = get_coords_to_subset(ignore_coords, metric_info)
    subset = data.copy()
    # check if subset is still possible
    if len(coords_to_subset) != 0:
        for coord in metric_info["coords"].keys():
            if coord in coords_to_subset:
                min_val = float(metric_info["coords"][coord][0])
                max_val = float(metric_info["coords"][coord][1])
                if (
                    min_val == max_val
                    and subset[coord].size == 1
                    and float(subset[coord].data) == min_val
                ):
                    continue
                elif min_val > max_val:
                    min_val += 0.01
                    subset = roll_coords_by_min_max_coord(
                        subset, coord, min_val, max_val
                    )
                else:
                    max_val += 0.01
                    selection = {coord: slice(min_val, max_val)}
                    subset = subset.sel(selection)
        subset = flatten_dims(subset)
    return subset


def get_coords_to_subset(ignore_coords, metric_info):
    if ignore_coords:
        coords_to_subset = set(metric_info["coords"].keys())
        coords_to_subset = coords_to_subset.difference(set(ignore_coords))
        coords_to_subset = list(coords_to_subset)
    else:
        coords_to_subset = list(metric_info["coords"].keys())
    return coords_to_subset


def check_plev_units(self, metric_info):
    if self.data["plev"].units in EQUIVALENT_PLEV_UNITS.keys():
        pass


def roll_coords_by_min_max_coord(data, coord, min_val, max_val):
    """
    Roll coords by a min and maximum value. Useful for longitude if coords to subset cross 0. i.e. 60W-60E
    """
    assert min_val > max_val, "Can only roll coords if min_val is more than max_val"
    idx = numpy.searchsorted(data[coord], min_val - max_val, side="left")

    try:
        rolled_data = data.roll({coord: idx}, roll_coords=True)\
        .sel({coord: slice(min_val, max_val)})\
        .sortby(coord) # TODO: check 0.1 as it was added to ensure that the slice does not exclude vals
            
    except:
        min_difference_array = numpy.absolute(data['lon']-min_val)
        max_difference_array = numpy.absolute(data['lon']-max_val)
        # find the index of minimum element from the array
        closest_to_min_index = int(min_difference_array.argmin())
        closest_to_max_index = int(max_difference_array.argmin())
        min_lon = data['lon'][closest_to_min_index]
        max_lon = data['lon'][closest_to_max_index]
        rolled_data = data.roll({coord: idx}, roll_coords=True)\
        .sel({coord: slice(min_lon, max_lon)})\
        .sortby(coord)
    return rolled_data


def flatten_dims(data):
    """
    Supports subset and will flatten coordinates of an Xarray DataSet/DataArray with one value (so they are standardised)
    """
    for dim in data.dims:
        if data.dims[dim] == 1:
            selection = {dim: 0}
            data = data.isel(selection)
    return data


def swap_coord_order(data, coord, ascending=True):
    """
    Will reverse the dimension if a higher number is first

    Parameters
    ----------
    data : xarray.Dataset
        climate data
    coord : str
        name from coord to change

    Useage
    ----------
    new_data = swap_coord_order(data, "lat")
    """
    first_val = 0
    last_val = -1
    if not ascending:
        first_val = -1
        last_val = 0
    if data[coord][first_val] > data[coord][last_val]:
        data = data.reindex(**{coord: list(reversed(data[coord]))})
    return data


def compute_metric_using_metric_info(
    data,
    metric_info,
    return_coord_error=False,
):
    """
    Write function description

    Parameters
    ----------
    data : xarray.Dataset
        Data to calculate metric from
    metric_info : dict
        jetstream metric information about metric name, subsetting, required variables, function location
    """
    #  check that you can actually compute metrics
    if check_all_coords_available(data, metric_info, return_coord_error)[
        0
    ] and check_all_variables_available(data, metric_info):
        pass
    else:
        raise ValueError(
            "cannot calculate %s metric from data provided" % (metric_info["name"])
        )  # TODO have this return a useful message

    # calculate metric
    result = metric_info["metric"](data)
    return result


def check_all_variables_available(data, metric):
    """
    Checks if all variables required to compute metric
    exist in the data.
    """
    for var in metric["variables"]:
        if var not in data.variables:
            return False
    return True


def check_all_coords_available(data, metric_info, return_coord_error=True):
    """
    Checks if all coords required to compute metric
    exist in the data.
    """
    coord_error_message = ""
    metric_usable = True
    try:
        assert (
            len(metric_info["coords"]) >= 1
        ), "Metric dictionary has less than 1 coordinate"
    except Exception as e:
        raise e(metric_usable, "Metric has no coordinates to subset")

    # Loop over each coordinate in all metric dictionary and check if the coords exist in data and can be used for the metric calculation
    for coord in metric_info["coords"].keys():
        if coord in data.coords:
            coord_vals = metric_info["coords"][coord]
            coord_available = check_if_coord_vals_meet_reqs(data, coord, coord_vals)
            # if coord fails check, provide user information why
            if return_coord_error and not coord_available:
                coord_error_message += " '%s' needs to be between %s and %s." % (
                    str(coord),
                    str(coord_vals[0]),
                    str(coord_vals[1]),
                )
            elif not coord_available:
                metric_usable = False
                break
        else:
            # if it does not exist then break loop as it is required for the metric
            metric_usable = False
            break
    return metric_usable, coord_error_message


def check_if_coord_vals_meet_reqs(data, coord, coord_vals):
    """
    Checks if the data has the correct coordinate values required
    for the metric.
    # TODO: expand to be more strict about plev!!
    """
    # Round all values to a global precision
    min_val = round(float(coord_vals[0]), ROUNDING_THRESHOLD)
    max_val = round(float(coord_vals[1]), ROUNDING_THRESHOLD)
    data[coord] = data[coord].astype(float).round(ROUNDING_THRESHOLD)
    
    # check for rolled data (TODO: new func?)
    if min_val > max_val:
        coord_val_avaialable_top = data[coord].loc[min_val : data[coord].max()]
        coord_val_avaialable_bottom = data[coord].loc[data[coord].min() : max_val]
        if len(coord_val_avaialable_top) != 0 or len(coord_val_avaialable_bottom) != 0:
            return True
        else:
            return False

    # check that the data is more than one value
    elif data[coord].count() > 1:
        coord_val_avaialable = data[coord].loc[min_val:max_val]
        if len(coord_val_avaialable) != 0:
            return True
        return False
    else:
        return data[coord].values >= min_val and data[coord].values <= max_val


class MetricComputerWithAllMetrics(MetricComputer):
    def __init__(self, data, all_metrics=None):
        super().__init__(data)
        self.all_metrics = all_metrics

    @classmethod
    def with_available_metrics(cls, data, all_metrics):
        data_with_available_metrics = cls(data)
        data_with_available_metrics.get_available_metrics(all_metrics)
        return data_with_available_metrics

    def get_available_metrics(self, return_coord_error=False):
        self.available_metrics = get_available_metric_list(
            self.data, self.all_metrics, return_coord_error
        )
        print("%s metrics available for this dataset:" % (len(self.available_metrics)))
        print("Metrics available:", self.available_metrics)

    def compute_all_metrics(self):
        """
        will go through and compute all metric which are available
        """
        if not hasattr(self, "available_metrics"):
            try:
                self.get_available_metrics()
            except Exception as e:
                raise KeyError(
                    "A dictionary of all metrics is required. Error is: %s " % (e)
                )

    def sel(self, inplace=False, **kwargs):
        """
        Exposes the xarray .sel function
        """
        new_data = self.data.copy()
        new_data = new_data.sel(**kwargs)
        if inplace:
            return MetricComputerWithAllMetrics.with_available_metrics(
                new_data, self.all_metrics
            )

        return MetricComputerWithAllMetrics(new_data, self.all_metrics)

    def isel(self, **kwargs):
        """
        Exposes the xarray .sel function
        """
        new_data = self.data.copy()
        new_data = new_data.isel(**kwargs)
        if hasattr(self, "available_metrics"):
            return MetricComputerWithAllMetrics.with_available_metrics(
                new_data, self.all_metrics
            )
        else:
            return MetricComputerWithAllMetrics(new_data, self.all_metrics)


def get_available_metric_list(data, all_metrics=None, return_coord_error=False):
    """
    Checks which variables can be used by the data

        Parameters
        ----------
        data : xr.Dataset or similar
            Xarray dataset

        all_metrics : dict (default: None)
            dictionary of jet-stream metrics

        return_coord_error : bool
            whether a message about where the correct coords but wrong
            coord values should be returned in available metrics list
            e.g. wrong pressure level (plev)

        Returns
        -------
        metric_available_list : list

        Usage
        -----
        m_list = get_available_metric_list(vwind_data, js_metrics)

    """
    available_metrics = []
    for metric_name in all_metrics:
        metric_is_usuable = {metric_name: "usuable"}
        if check_all_variables_available(data, metric=all_metrics[metric_name]):
            # check that all coords exists in xarray data i.e. plev, lat, etc.
            metric_usable, coord_error_message = check_all_coords_available(
                data, all_metrics[metric_name], return_coord_error
            )
            # will make return error message
            if return_coord_error and len(coord_error_message) > 0:
                metric_is_usuable = {
                    metric_name: "To use this metric" + coord_error_message
                }
            if metric_usable:
                available_metrics.append(metric_is_usuable)

    return available_metrics
