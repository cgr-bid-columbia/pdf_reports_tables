from tqdm import tqdm
from glob import glob
from itertools import groupby
import pandas as pd
import os, math, json, logging

JSON_FILE = "paths.json"
pdf_tables_file = open(JSON_FILE, "r")
pdf_tables_dict = json.load(pdf_tables_file)
TASK_ID = os.environ.get("SLURM_ARRAY_TASK_ID")
LOGS_FOLDER = "./logs"
LOGNAME = f"/pdf_table_1_parser_{TASK_ID}.log"
NUM_JOBS = pdf_tables_dict["num_jobs"]

logging.basicConfig(filename=LOGS_FOLDER + LOGNAME,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)


def divide_chunks(list_input: list, num_chunks: int) -> list:
    """
    Divides list into chucks of size num_chunks
    """
    for index in range(0, len(list_input), num_chunks):
        yield list_input[index:index + num_chunks]


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
    
    original_num_rows = len(data) # obtaining original num of rows (for output_report)
    
    return indexes_consec_nans, headers_to_fix_indexes, remaining_indexes, original_num_rows


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


def parse_table_1(file_path: str, output_path: str, output_sufix="_parsed") -> pd.DataFrame:
    """
    Parses an excel table from a CGR report regardless of their
    format type
    """

    output_report = {"file_path": file_path} # placeholder for output report

    try: # ignoring invalid files

        ## first layer: parsing "value" column
        # obtaining indexes of rows w nans on "value" column
        indexes_consec_nans, headers_to_fix_indexes, remaining_indexes, original_num_rows = table_1_indexes_nans(file_path)

        # collapsing rows w nans on "value" column
        parsing_layer_1 = table_1_collapse_nans(file_path, indexes_consec_nans, headers_to_fix_indexes, remaining_indexes)
        
        ## second layer: parsing "name" column
        # obtaining indexes of rows w colons at the end on "name" column
        indexes_no_colons_grouped, indexes_colons = table_1_get_indexes_for_collapse(parsing_layer_1)
        
        # collapsing rows w/out colons at the end on "name" column
        parsing_layer_2 = table_1_collapse_colons(parsing_layer_1, indexes_no_colons_grouped, indexes_colons)

        # storing original file name
        file_df = pd.DataFrame(list(zip(["Original file"], [file_path])), columns=["name", "value"])
        parsing_layer_2 = pd.concat([parsing_layer_2, file_df], ignore_index=True)

        # write corrected dataframe back to an Excel sheet 
        file_name = file_path.split("/")[-1].split(".")[0]
        parsing_layer_2.to_excel(output_path + "/" + file_name + output_sufix + ".xlsx", index=False)

        output_report["status"] = "parsed" # update status of the parsing
        output_report["errors"] = "-" # update status of the parsing
        output_report["original_num_rows"] = original_num_rows # storing length of original df
        output_report["new_num_rows"] = len(parsing_layer_2) # storing length of new df
    
    except Exception as e:
        output_report["status"] = "error" # update status of the parsing
        output_report["errors"] = str(e) # update status of the parsing
        output_report["original_num_rows"] = "-"
        output_report["new_num_rows"] = "-"
    

    return output_report
        

if __name__ == "__main__":

    # read paths
    input_path = pdf_tables_dict["input_path"] 
    output_path = pdf_tables_dict["output_path"]

    files_paths = glob(input_path + "/*.xlsx", recursive=True) # reading excels from input path

    # run function `parse_table_1` on all the excel files that contain "first_table" string
    first_tables_paths = [path for path in files_paths if "table_1" in path.lower() and "raw" not in path.lower()][:6]

    # splitting paths into lists that can be evaluated per job
    #TASK_ID = 1
    paths_splitted = list(divide_chunks(first_tables_paths, int(len(first_tables_paths)/NUM_JOBS)))
    paths_for_job = paths_splitted[int(TASK_ID)-1]

    logging.info(f"starting parsing of table 1 (table index: 0)")
    list_reports = [] # creating of reports with all the parsing reports 
    for file_path in tqdm(paths_for_job):
        logging.info(f"parsing: file {file_path}")

        output_report = parse_table_1(file_path, output_path)
        output_report_df = pd.DataFrame.from_dict(output_report, orient="index").transpose()
        list_reports.append(output_report_df)
    
    pd.concat(list_reports).to_excel(output_path + f"/parsing_reports_job_{TASK_ID}.xlsx", index=False) # saving parsing reports
    logging.info(f"ending the parsing of table 1 files")