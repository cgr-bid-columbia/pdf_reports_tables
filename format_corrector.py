from glob import glob
from tqdm import tqdm
import pandas as pd 
import logging, json, os

JSON_FILE = "paths.json"
pdf_tables_file = open(JSON_FILE, "r")
pdf_tables_dict = json.load(pdf_tables_file)
TASK_ID = os.environ.get("SLURM_ARRAY_TASK_ID")
LOGS_FOLDER = "./logs"
LOGNAME = f"/pdf_metadata_parser_{TASK_ID}.log"
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


def parse_table_one(excel_file: str, output_path: str, output_sufix="_parsed") -> dict:
    """
    Parses the first table of the "Informe de Control" and writes 
    the parsed table to a new Excel file.

    Note: the file name should NOT include the ".xlsx" suffix
    """

    # read Excel file into pandas dataframe 
    df = pd.read_excel(excel_file)

    output_report = {"file_path": excel_file} # placeholder for output report
    try:
        # fix "row" column 
        df.at[0, "row"] += " " + df.at[1, "row"]
        df.at[2, "row"] += " " + str(df.at[3, "row"]) 
        df.at[5, "row"] += " " + df.at[6, "row"]
        df.at[7, "row"] += " " + df.at[8, "row"]
        df.at[13, "row"] += " " + df.at[14, "row"]
        df.at[17, "row"] += " " + df.at[18, "row"]
        df.at[19, "row"] += " " + df.at[20, "row"]

        # fix "value" colum 
        df.at[2, "value"] += " " + df.at[3, "value"] + " " + df.at[4, "value"] 
        df.at[7, "value"] += " " + df.at[8, "value"] + " " + df.at[9, "value"] + " " + df.at[10, "value"] + " " + df.at[11, "value"] 
        # special case for 15, 16 
        indicator_words = df.at[15, "value"].split() 
        value_words = df.at[16, "value"].split() 
        value_words = value_words[0:2] + [value_words[2] + " " + value_words[3]]
        df.at[15, "value"] = str([indicator_words[i] + " " + value_words[i] for i in range(len(indicator_words))])
        df.at[19, "value"] += " " + df.at[20, "value"]

        # drop irrelevant rows 
        df.drop([1, 3, 4, 6, 8, 9, 10, 11, 14, 16, 18, 20], inplace=True) 

        # renumber initial column 
        df.iloc[:, 0] = range(0, len(df)) 

        output_report["status"] = "parsed" # update status of the parsing
        output_report["errors"] = "-" # update status of the parsing
    except Exception as e:
        output_report["status"] = "parser didn't work" # update status of the parsing
        output_report["errors"] = str(e) # update status of the parsing


    # write corrected dataframe back to an Excel sheet 
    file_name = excel_file.split("/")[-1].split(".")[0]
    df.to_excel(output_path + "/" + file_name + output_sufix + ".xlsx", index=False) 

    return output_report 


if __name__ == "__main__":
    # read paths
    input_path = pdf_tables_dict["input_path"] 
    output_path = pdf_tables_dict["output_path"]

    # read all the pdfs from input path
    files_paths = glob(input_path + "/*.xlsx", recursive=True) # reading pdfs from input path

    # run function `parse_table_1` on all the excel files that contain "first_table" string
    first_tables_paths = [path for path in files_paths if "table_1" in path.lower() and "raw" not in path.lower()][:6]

    # splitting paths into lists that can be evaluated per job
    paths_splitted = list(divide_chunks(first_tables_paths, int(len(first_tables_paths)/NUM_JOBS)))
    paths_for_job = paths_splitted[int(TASK_ID)-1]

    list_reports = [] # creating of reports with all the parsing reports 

    logging.info(f"starting parsing of table 1 (table index: 0)")
    for file_path in tqdm(paths_for_job):
        logging.info(f"parsing: file {file_path}")

        output_report = parse_table_one(file_path, output_path) # parsing data

        # storing parsing reports
        output_report_df = pd.DataFrame.from_dict(output_report, orient="index").transpose()
        list_reports.append(output_report_df)

    pd.concat(list_reports).to_excel(output_path + f"/parsing_reports_job_{TASK_ID}.xlsx", index=False) # saving parsing reports

    logging.info(f"ending the parsing of pdfs") 

