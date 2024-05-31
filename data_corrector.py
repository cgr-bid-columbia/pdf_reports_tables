# from Levenshtein import distance
from tqdm import tqdm
from glob import glob
import pandas as pd
import  unicodedata, logging, json, os

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
        
    new_vars = ["doc_number", "title", "investment_code", "subject", "description", "entity", "ubigeo", "amount", "issue_date", "organic_unit", "path"]

    # formatting data
    num_matched_cols = 0
    formatted_data = {"name": [], "value": []}
    unmatched_cols = cols_list.copy() # placeholder for unmatched columns
    unmatched_new_vars = new_vars.copy() # placeholder for unmatched benchmark columns
    num_empty_vals_formatted = 0 # counter of empty vals

    for col in cols_list:
        if ("n de" in col or "codigo" in col) and ("doc_number" in unmatched_new_vars): # formatting "n de" as "doc_number"
            formatted_data["name"].append("doc_number")
            unmatched_new_vars.remove("doc_number")
            num_matched_cols += 1

        elif ("titulo" in col) and ("title" in unmatched_new_vars): # formatting "titulo" as "title"
            formatted_data["name"].append("title")
            unmatched_new_vars.remove("title")
            num_matched_cols += 1

        elif ("codigo" in col) and ("investment_code" in unmatched_new_vars): # formatting "codigo" as "investment_code"
            formatted_data["name"].append("investment_code")
            unmatched_new_vars.remove("investment_code")
            num_matched_cols += 1

        elif ("detalle" in col or "asunto" in col or "objetivo" in col or "objeto" in col) and ("subject" in unmatched_new_vars): # formatting "detalle" as "subject"
            formatted_data["name"].append("subject")
            unmatched_new_vars.remove("subject")
            num_matched_cols += 1

        elif ("descripcion" in col) and ("description" in unmatched_new_vars): # formatting "descripcion" as "description"
            formatted_data["name"].append("description")
            unmatched_new_vars.remove("description")
            num_matched_cols += 1
        
        elif ("entidad" in col) and ("entity" in unmatched_new_vars): # formatting "entidad" as "entity"
            formatted_data["name"].append("entity")
            unmatched_new_vars.remove("entity")
            num_matched_cols += 1

        elif ("ubigeo" in col) and ("ubigeo" in unmatched_new_vars): # formatting "ubigeo" as "ubigeo"
            formatted_data["name"].append("ubigeo")
            unmatched_new_vars.remove("ubigeo")
            num_matched_cols += 1
        
        elif ("monto" in col) and ("amount" in unmatched_new_vars): # formatting "monto" as "amount"
            formatted_data["name"].append("amount")
            unmatched_new_vars.remove("amount")
            num_matched_cols += 1
        
        elif ("fecha de emision" in col) and ("issue_date" in unmatched_new_vars): # formatting "fecha de emision" as "issue_date"
            formatted_data["name"].append("issue_date")
            unmatched_new_vars.remove("issue_date")
            num_matched_cols += 1
        
        elif ("unidad organica" in col) and ("organic_unit" in unmatched_new_vars): # formatting "unidad organica" as "organic_unit"
            formatted_data["name"].append("organic_unit")
            unmatched_new_vars.remove("organic_unit")
            num_matched_cols += 1

        elif ("original file" in col) and ("path" in unmatched_new_vars): # formatting "original file" as "path"
            formatted_data["name"].append("path")
            unmatched_new_vars.remove("path")
            num_matched_cols += 1

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

        unmatched_cols.remove(col) # removing matched cols from list of unmatched cols

    # adding rows for new variable names with no values
    for var in new_vars:
        if var not in formatted_data["name"]:
            formatted_data["name"].append(var)
            formatted_data["value"].append("")
            num_empty_vals_formatted += 1

    formatted_data = pd.DataFrame(formatted_data) # converting formatted data to dataframe
    # file_name = csv_file.split("\\")[-1].split(".")[0] # getting the name of the file | Windows
    file_name = csv_file.split("/")[-1].split(".")[0] # getting the name of the file | Linux
    print(f"file_name: {file_name}")
    formatted_data.to_csv(output_path + f"/{file_name}.csv", sep="|", index=False) # exporting formatted data

    # sorting unmatched cols
    unmatched_cols.sort()


    report = {
        "num_cols": [len(cols_list)],
        "num_matched_cols": [num_matched_cols],
        "perc_matched_cols": [round(num_matched_cols/ len(cols_list) * 100, 2)],
        "num_unmatched_cols": [len(unmatched_cols)],
        "unmatched_cols": [str(unmatched_cols)],
        "num_unmatched_new_cols": [len(unmatched_new_vars)],
        "unmatched_new_cols": [str(unmatched_new_vars)],
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