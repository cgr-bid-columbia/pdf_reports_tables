# from Levenshtein import distance
from tqdm import tqdm
from glob import glob
import pandas as pd
import math, unicodedata, logging, json, os

JSON_FILE = "paths.json"
pdf_tables_file = open(JSON_FILE, "r")
pdf_tables_dict = json.load(pdf_tables_file)
TASK_ID = os.environ.get("SLURM_ARRAY_TASK_ID")
LOGS_FOLDER = "./logs"
LOGNAME = f"/data_corrector_{TASK_ID}.log"
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


def remove_accents_and_lowercase(input_str):
    """
    Removes accents from a string and converts it to lowercase, as 
    well as the accents 
    """
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    without_accents = "".join([c.replace(":", "").replace("°", "") for c in nfkd_form if not unicodedata.combining(c)])
    return without_accents.lower()


def table_1_data_corrector(csv_file: str, output_path: str) -> pd.DataFrame:
    """
    Formats the data in the table 1 of the csv file
    and returns the data as a csv alongside a report 
    of the formatting
    """
    # reading file
    data = pd.read_csv(csv_file, sep="|")

    # obtaining the columns of the dataframe
    original_cols_list = list(data["name"].values)

    cols_list = [] # placeholder for list of standardized column names
    cols_mapping = {} # mapping of original to standardized column names

    for col in original_cols_list: # standardizing the column names
        updated_col_name = remove_accents_and_lowercase(col).strip()
        cols_mapping[updated_col_name] = col
        cols_list.append(updated_col_name)
        

    benchmark_cols = [
        "n de", # informe u oficio
        "codigo", # investment code of the project
        "titulo", 
        "objeto", # same as "asunto"/"detalle" in other versions of the table
        "detalle", # same as "asunto"/"objeto" in other versions of the table
        "asunto", # same as "detalle"/"objeto" in other versions of the table
        "descripcion", # more detailed "asunto"/"detalle"
        "entidad", # same as "entidad auditada" in older versions of the table
        "ubigeo",
        "monto",
        "fecha de emision",
        "unidad organica", # same as "entidad unidad organica que emite el" + "informe"/"oficio" in older versions of the table
        "original file"
    ]

    # formatting data
    matched_cols = []
    # matched_cols_dict = {"original_col": [], "matched_bench_col": []}
    formatted_data = {"name": [], "value": []}
    unmatched_cols = cols_list.copy() # placeholder for unmatched columns
    unmatched_bench_cols = benchmark_cols.copy() # placeholder for unmatched benchmark columns
    num_empty_vals_formatted = 0 # counter of empty vals
    for col in cols_list:
        for bench_col in unmatched_bench_cols:
            if bench_col in col:

                matched_cols.append(col) # storing matched cols
                unmatched_cols.remove(col) # removing matched cols from unmatched cols

                if "n de" in col or "codigo" in col: # formatting "n de" as "doc_number"
                    formatted_data["name"].append("doc_number")

                elif "titulo" in col: # formatting "titulo" as "title"
                    formatted_data["name"].append("title")

                elif "codigo" in col: # formatting "codigo" as "investment_code"
                    formatted_data["name"].append("investment_code")

                elif "detalle" in col or "asunto" in col or "objeto" in col: # formatting "detalle" as "subject"
                    formatted_data["name"].append("subject")

                elif "descripcion" in col: # formatting "descripcion" as "description"
                    formatted_data["name"].append("description")
                
                elif "entidad" in col: # formatting "entidad" as "entity"
                    formatted_data["name"].append("entity")

                elif "ubigeo" in col: # formatting "ubigeo" as "ubigeo"
                    formatted_data["name"].append("ubigeo")
                
                elif "monto" in col: # formatting "monto" as "amount"
                    formatted_data["name"].append("amount")
                
                elif "fecha de emision" in col: # formatting "fecha de emision" as "issue_date"
                    formatted_data["name"].append("issue_date")
                
                elif "unidad organica" in col: # formatting "unidad organica" as "organic_unit"
                    formatted_data["name"].append("organic_unit")

                elif "original file" in col: # formatting "original file" as "path"
                    formatted_data["name"].append("path")

                # storing the var value
                var_value = data[data["name"] == cols_mapping[col]]["value"].iloc[0] # getting the value of the variable
                if str(var_value) == "nan": # handling missing values
                    var_value = ""
                formatted_data["value"].append(var_value) # appending the value of the variable
                
                if var_value == "": # counting empty values
                    num_empty_vals_formatted += 1

                # storing the doc type
                if "titulo" in col and "doc_type" not in formatted_data["name"]: # detecting the doc type from the title var
                    formatted_data["name"].append("doc_type")
                    if "informe" in col:
                        formatted_data["value"].append("informe")
                    elif "oficio" in col:
                        formatted_data["value"].append("oficio")
                    else:
                        formatted_data["value"].append("unspecified")

                unmatched_bench_cols.remove(bench_col)
                break

    # adding rows for new variable names with no values
    new_vars = ["doc_number", "title", "investment_code", "subject", "description", "entity", "ubigeo", "amount", "issue_date", "organic_unit", "path"]
    for var in new_vars:
        if var not in formatted_data["name"]:
            formatted_data["name"].append(var)
            formatted_data["value"].append("")
            num_empty_vals_formatted += 1

    formatted_data = pd.DataFrame(formatted_data) # converting formatted data to dataframe
    # file_name = csv_file.split("\\")[-1].split(".")[0] # getting the name of the file | Windows
    file_name = csv_file.split("/")[-1].split(".")[0] # getting the name of the file | Linux
    formatted_data.to_csv(output_path + f"/{file_name}.csv", sep="|", index=False) # exporting formatted data

    # sorting unmatched cols
    unmatched_cols.sort()
    unmatched_bench_cols.sort()


    report = {
        "num_cols": [len(cols_list)],
        "num_matched_cols": [len(matched_cols)],
        "perc_matched_cols": [round(len(matched_cols) / len(cols_list) * 100, 2)],
        "num_unmatched_cols": [len(unmatched_cols)],
        "unmatched_cols": [str(unmatched_cols)],
        "unmatched_bench_cols": [str(unmatched_bench_cols)],
        "num_empty_vals_formatted": [num_empty_vals_formatted], 
        "perc_empty_vals_formatted": [round(num_empty_vals_formatted / len(new_vars) * 100, 2)], 
        "file_path": [data[data["name"] == "Original file"]["value"].values[0]] # storing var value
    }

    return report


if __name__ == "__main__":

    # read paths
    input_path = pdf_tables_dict["input_path"] 
    output_path = pdf_tables_dict["output_path"]
    
    files_paths = glob(input_path + "/*.csv", recursive=True) # reading pdfs from input path
    paths_splitted = list(divide_chunks(files_paths, int(len(files_paths)/NUM_JOBS)))
    paths_for_job = paths_splitted[int(TASK_ID)-1]

    formatting_reports = []
    for item in tqdm(paths_for_job):
        if "~" not in item and "parsing_reports" not in item:
            current_report = table_1_data_corrector(item, output_path)
    
            formatting_reports.append(pd.DataFrame(current_report))
    
    # exporting formatting reports
    pd.DataFrame(pd.concat(formatting_reports)).to_csv(output_path + r"\formatting_reports" + f"_{TASK_ID}.csv", sep="|", index=False)

    # pd.DataFrame(output).to_excel(r"D:\Descargas\PDF tables aux\Parsed" + r"\\" + file_name + "_formatted" + ".xlsx", index=False)