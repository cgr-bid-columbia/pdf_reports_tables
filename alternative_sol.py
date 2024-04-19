from tqdm import tqdm
from glob import glob
from itertools import groupby
import pandas as pd
import math

def find_consecutive_values(list_data: list) -> list:
    """
    Return consecutive values in list of integers
    """
    
    # enumerate and get differences between counter—integer pairs
    # group by differences (consecutive integers have equal differences)  
    groups_by_diffs = groupby(enumerate(list_data), key=lambda item: item[0] - item[1])
    
    # repack elements from each group into list
    all_groups = ([index[1] for index in group] for _, group in groups_by_diffs)
    
    # obtain out one element lists 
    output = list(filter(lambda x: len(x) > 1, all_groups))

    return output


def table_1_indexes_nans(excel_file: str) -> tuple:
    """
    Returns the indexes of all rows w consecutive nan values,
    of the headers they correspond to and the remaining headers
    """

    # reading file
    data = pd.read_excel(excel_file)

    # finding indexes of all nans in first column (col "row")
    indexes_nans = []
    for index, row in data.iterrows(): 
        if str(row["row"]) == "nan": 
            indexes_nans.append(index)

    # finding indexes of all rows with consecutive nans
    indexes_consec_nans = find_consecutive_values(indexes_nans)

    # obtaining indexes of headers to fix
    headers_to_fix_indexes = []
    for indexes in indexes_consec_nans:
        headers_to_fix_indexes.append(indexes[0] - 1)

    # obtaining indexes of remaining headers
    remaining_indexes = [index for index in list(range(len(data))) if index not in indexes_nans 
                         and index not in headers_to_fix_indexes]

    return indexes_consec_nans, headers_to_fix_indexes, remaining_indexes


def table_1_collapse_nans(excel_file: str, indexes_consec_nans: list, headers_to_fix_indexes: list, remaining_indexes: list) -> pd.DataFrame:
    """
    Creates a new dataframe after collapsing all the nan rows 
    into their corresponding headers
    """

    # reading file
    data = pd.read_excel(excel_file)

    # defining the indexes w headers
    header_indexes = headers_to_fix_indexes + remaining_indexes
    header_indexes.sort()

    collapsed_df = [] # placeholder for final df
    index_consec_indexes = 0 # index of consecutive indexes list
    for index in header_indexes:
        
        if index in remaining_indexes: # adding the headers that don't need fix
            current_vals = {
                "name": data.iloc[index, data.columns.get_loc("row")],
                "value": data.iloc[index, data.columns.get_loc("value")]
            }

        else: # headers that need to be fixed
            current_vals = {
                "name": data.iloc[index, data.columns.get_loc("row")],
            }

            # collapsing value of current header
            collapsed_value = ""
            indexes_w_value = [index] + indexes_consec_nans[index_consec_indexes]
            for index in indexes_w_value:
                collapsed_value += str(data.iloc[index, data.columns.get_loc("value")]) + " "

            index_consec_indexes += 1 # updating index for next consecutive indexes list
            current_vals["value"] = collapsed_value # storing collapsed value
        
        collapsed_df.append(current_vals) # stacking dicts w data

    collapsed_df = pd.DataFrame(collapsed_df)

    return collapsed_df


if __name__ == "__main__":
    input_path = r"/Users/mg4558/Downloads/second_round_jacob"
    output_path = r"/Users/mg4558/Downloads/second_round_jacob/output_test"
    files_paths = glob(input_path + "/*.xlsx", recursive=True) # reading pdfs from input path
    
    file_counter = 0
    for item in tqdm(files_paths[:2]):
        print("item: ", item)
        indexes_consec_nans, headers_to_fix_indexes, remaining_indexes = table_1_indexes_nans(item)

        print("indexes_consec_nans: ", indexes_consec_nans)
        print("headers_to_fix_indexes: ", headers_to_fix_indexes)
        print("remaining_indexes: ", remaining_indexes)

        parsing_layer_1 = table_1_collapse_nans(item, indexes_consec_nans, headers_to_fix_indexes, remaining_indexes)
        parsing_layer_1.to_excel(output_path + f"/test_{file_counter}.xlsx")

        file_counter += 1