# from Levenshtein import distance
from tqdm import tqdm
from glob import glob
import pandas as pd
import unicodedata, logging, json, os

JSON_FILE = "paths.json"
pdf_tables_file = open(JSON_FILE, "r")
pdf_tables_dict = json.load(pdf_tables_file)
# TASK_ID = os.environ.get("SLURM_ARRAY_TASK_ID")
TASK_ID = 1
LOGS_FOLDER = "./logs"
LOGNAME = f"/data_corrector_{TASK_ID}.log"
NUM_JOBS = pdf_tables_dict["num_jobs"]

logging.basicConfig(filename=LOGS_FOLDER + LOGNAME,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)


def table_2_data_corrector(excel_file: str, output_path: str) -> pd.DataFrame:
    """
    Formats the data in the table 1 of the csv file
    and returns the data as a csv alongside a report 
    of the formatting
    """

    # reading file
    data = pd.read_excel(excel_file).iloc[:, 1:] # reading the file and removing the first column

    report = {"filename": excel_file, "num_rows": len(data)} # placeholder for output report

    # obtaining the columns of the dataframe
    # original_cols_list = list(data["row"].values)

    # cols_list = [] # placeholder for list of standardized column names
    # cols_mapping = {} # mapping of original to standardized column names

    # remove first row if invalid value found
    if "item" in str(data["row"].values[0]).lower(): 
        data = data.iloc[1:] 
    else: 
        pass

    # removing second column
    data = data.iloc[:, 1:]

    report["num_control_types"] = 0
    cleaned_data = {"control_type": [], "indicator": []}
    for _, row in data.iterrows():
        
        if " X" in str(row.values):
            cleaned_data["control_type"].append(row.values[0].replace(" X", "").strip().lower())
            cleaned_data["indicator"].append("yes")
            report["num_control_types"] += 1
        else:
            cleaned_data["control_type"].append(row.values[0].strip().lower())
            cleaned_data["indicator"].append("no")

    cleaned_df = pd.DataFrame(cleaned_data)
    report["num_rows_cleaned"] = len(cleaned_df)
    
    cleaned_df.to_csv(output_path + f"/{excel_file.split('/')[-1]}" + ".csv", index=False, delimiter="|")
    return report


def divide_chunks(list_input: list, num_chunks: int) -> list:
    """
    Divides list into chucks of size num_chunks
    """
    for index in range(0, len(list_input), num_chunks):
        yield list_input[index:index + num_chunks]


if __name__ == "__main__":

    # read paths
    input_path = pdf_tables_dict["input_path"] 
    output_path = pdf_tables_dict["output_path"]
    reports_path = pdf_tables_dict["output_path"]
    
    files_paths = glob(input_path + "/*.xlsx", recursive=True) # reading pdfs from input path
    paths_splitted = list(divide_chunks(files_paths, int(len(files_paths)/NUM_JOBS)))
    paths_for_job = paths_splitted[int(TASK_ID)-1]

    formatting_reports = []
    for item in tqdm(paths_for_job):
        if "table_2." in item and  "~" not in item and "parsing_reports" not in item:
            current_report = table_2_data_corrector(item, output_path)
            formatting_reports.append(pd.DataFrame(current_report))
    
    # exporting formatting reports
    pd.DataFrame(pd.concat(formatting_reports)).to_csv(reports_path + r"\formatting_reports" + f"_{TASK_ID}.csv", sep="|", index=False)