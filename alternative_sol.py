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
    output = list(filter(lambda x: len(x) >= 1, all_groups))

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
    in "value" column into their corresponding headers
    """

    # reading file
    data = pd.read_excel(excel_file)

    # defining the indexes w headers
    header_indexes = headers_to_fix_indexes + remaining_indexes
    header_indexes.sort()

    collapsed_df = [] # placeholder for final df
    index_consec_indexes = 0 # index of consecutive indexes list
    for index in header_indexes:
        
        if index in remaining_indexes: # concatenating the headers that don't need fix
            current_vals = {
                "name": data.iloc[index, data.columns.get_loc("row")],
                "value": data.iloc[index, data.columns.get_loc("value")]
            }

        else: # concatenating headers that need to be fixed
            current_vals = {
                "name": data.iloc[index, data.columns.get_loc("row")],
            }

            collapsed_value = "" # placeholder for collapsed value
            indexes_w_value = [index] + indexes_consec_nans[index_consec_indexes] # joining all indexes belonging to a single header
            for index_val in indexes_w_value: # collapsing values of current header
                collapsed_value += str(data.iloc[index_val, data.columns.get_loc("value")]) + " "

            index_consec_indexes += 1 # updating index for next consecutive indexes list
            current_vals["value"] = collapsed_value # storing collapsed value
            
        collapsed_df.append(current_vals) # stacking dicts w data
        
    collapsed_df = pd.DataFrame(collapsed_df)

    return collapsed_df


def table_1_get_indexes_for_collapse(data: pd.DataFrame) -> tuple:
    """
    Returns a tuple w the indexes of all rows that need to be collapsed and
    that don't
    """

    indexes_no_coll = [] # placeholder for indexes that don't need collapse
    indexes_coll_grouped = [] # placeholder for groupped items that need to be collapsed
    coll_group = [] # aux list of current group of items that need to be collapsed
    for index, row in data.iterrows(): 

        # finding all indexes of rows that end w have colons
        if str(row["name"])[-1] == ":": 
            indexes_no_coll.append(index)

            # reset current group of no_colon indexes if
            # (1) current index has a colon and
            # (2) current group is not empty
            if len(coll_group) != 0: 
                indexes_coll_grouped.append(coll_group) # storing no_colon group
                coll_group = [] # resetting no_colon group
        
        else:
            coll_group.append(index) # storing no colon index in aux list

    # omitting all indexes w colon that belong to a header
    for coll_group_indexes in indexes_coll_grouped:
        
        # adding index of last item belonging to group
        last_item = coll_group_indexes[-1] + 1 
        coll_group_indexes.append(last_item)

        # erasing last_item from indexes_no_coll
        indexes_no_coll.remove(last_item)
        
    return indexes_coll_grouped, indexes_no_coll


def table_1_collapse_colons(data: pd.DataFrame, indexes_no_colons_grouped: list, indexes_colons: list) -> pd.DataFrame:
    """
    Creates a new dataframe after collapsing all the nan rows 
    in "value" column into their corresponding headers
    """

    # defining the indexes w headers
    header_indexes_to_be_fixed = [group[0] for group in indexes_no_colons_grouped]
    header_indexes = indexes_colons + header_indexes_to_be_fixed

    collapsed_df = [] # placeholder for final df
    group_index = 0 # index of current group of indexes to collapse
    for index in header_indexes:
        
        if index in indexes_colons: # concatenating the headers that don't need fix
            current_vals = {
                "name": data.iloc[index, data.columns.get_loc("name")],
                "value": data.iloc[index, data.columns.get_loc("value")]
            }

        else: # concatenating headers that need to be fixed
            
            current_vals = { # storing data from "value" column
                "value": data.iloc[index, data.columns.get_loc("value")],
            }

            collapsed_name = "" # placeholder for collapsed value
            for index_val in indexes_no_colons_grouped[group_index]: # collapsing names of current header
                collapsed_name += str(data.iloc[index_val, data.columns.get_loc("name")]) + " "

            group_index += 1 # updating index for next consecutive indexes list
            current_vals["name"] = collapsed_name # storing collapsed value
            
        collapsed_df.append(current_vals) # stacking dicts w data
        
    collapsed_df = pd.DataFrame(collapsed_df)

    return collapsed_df



if __name__ == "__main__":
    input_path = r"/Users/mg4558/Downloads/second_round_jacob"
    output_path = r"/Users/mg4558/Downloads/second_round_jacob/output_test"
    files_paths = glob(input_path + "/*.xlsx", recursive=True) # reading pdfs from input path
    
    file_counter = 0
    for item in tqdm(files_paths):
        
        print(item)
        
        # obtaining indexes for fixing the data
        indexes_consec_nans, headers_to_fix_indexes, remaining_indexes = table_1_indexes_nans(item)

        # collapsing nans
        parsing_layer_1 = table_1_collapse_nans(item, indexes_consec_nans, headers_to_fix_indexes, remaining_indexes)
        parsing_layer_1.to_excel(output_path + f"/test_{file_counter}_layer_1.xlsx")

        indexes_no_colons_grouped, indexes_colons = table_1_get_indexes_for_collapse(parsing_layer_1)
        print("indexes_no_colons_grouped: ", indexes_no_colons_grouped)
        print("indexes_colons: ", indexes_colons)
        
        parsing_layer_2 = table_1_collapse_colons(parsing_layer_1, indexes_no_colons_grouped, indexes_colons)

        parsing_layer_2.to_excel(output_path + f"/test_{file_counter}_layer_2.xlsx")

        file_counter += 1