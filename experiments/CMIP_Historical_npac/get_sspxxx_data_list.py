from utils import get_data


def get_sspxxx_data_list(path_name, output_file_name):
    data_path_retriever = get_data.GlobDataPathRetrieverFromJASMIN(path_name)
    data_path_retriever.retrieve_data_paths(inplace=True, one_realisation=False)
    data_path_retriever.save_data_paths(output_file_name)


def generate_grouped_sspxxx_data_paths(path_name, date_range_start, date_range_end):
    data_path_grouper = get_data.DataPathGrouperForJASMIN.from_file(path_name)
    grouped_data_paths = data_path_grouper.group_data_paths(
        date_range_start, date_range_end
    )
    return grouped_data_paths
