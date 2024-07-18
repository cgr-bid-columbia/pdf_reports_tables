# from Levenshtein import distance
from tqdm import tqdm
from glob import glob
import pandas as pd
import logging, json, os

JSON_FILE = "paths.json"
pdf_tables_file = open(JSON_FILE, "r")
pdf_tables_dict = json.load(pdf_tables_file)
TASK_ID = os.environ.get("SLURM_ARRAY_TASK_ID")
# TASK_ID = 1
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

    logging.info(f"reading file: {excel_file}, num_rows: {len(data)}")
    report = {"filename": excel_file, "num_rows": len(data), 
              "num_control_types": 0, "num_rows_post_cleaning": "-", "format": "-"} # placeholder for output report

    # placeholder for cleaned data
    cleaned_data = {"control_type": [], "indicator": []}

    try: 
        # checking format of first rows
        if len(data) >= 2:
            first_item_row = data["row"].values[0]
            first_item_values = data["value"].values[0]
            second_item_row = data["row"].values[1]

            if ("item" in str(first_item_row).lower() or "unnamed" in str(first_item_row).lower()) and ("contrato" not in str(second_item_row).lower()): # format 1
                report["format"] = "1"

                # remove first row    
                data = data.iloc[1:] 

                # removing first column
                data = data.iloc[:, 1:]

                # finding the control type(s)
                for _, row in data.iterrows():
                    if " X" in str(row.values): # tagging the control type
                        cleaned_data["control_type"].append(row.values[0].replace(" X", "").strip().lower())
                        cleaned_data["indicator"].append("yes")
                        report["num_control_types"] += 1
                    else:
                        cleaned_data["control_type"].append(row.values[0].strip().lower())
                        cleaned_data["indicator"].append("no")
                
            elif "contrato" in str(first_item_row).lower() or "contrato" in str(second_item_row).lower(): # format 2

                if "contrato" in str(first_item_row).lower(): # format 2.1
                    if "bien" in str(first_item_values).lower(): # format 2.1.1
                        report["format"] = "2.1.1"

                        # removing first column
                        data = data.iloc[:, 1:]

                        # finding the control type(s)
                        for _, row in data.iterrows(): 
                            if "X" in str(row.values): # tagging the control type
                                cleaned_data["control_type"].append(row.values[0].replace("X", "").strip().lower())
                                cleaned_data["indicator"].append("yes")
                                report["num_control_types"] += 1

                            else:
                                cleaned_data["control_type"].append(row.values[0].strip().lower())
                                cleaned_data["indicator"].append("no")
                    
                    elif "bien" in str(second_item_row).lower(): # format 2.1.2
                        report["format"] = "2.1.2"

                        for _, row in data.iterrows():
                            if "X" in str(row.values): # tagging the control type
                                cleaned_data["control_type"].append(row.row.strip().lower())
                                cleaned_data["indicator"].append("yes")
                                report["num_control_types"] += 1

                            else:
                                cleaned_data["control_type"].append(row.row.strip().lower())
                                cleaned_data["indicator"].append("no")
                    
                    else:
                        report["format"] = "2.1-no-format"
                
                elif "contrato" in str(second_item_row).lower(): # format 2.2

                    # remove first and second rows
                    data = data.iloc[2:]
                    
                    if "bien" in str(first_item_values).lower(): # format 2.2.1
                        report["format"] = "2.2.1"

                        # removing first column
                        data = data.iloc[:, 1:]

                        # finding the control type(s)
                        for _, row in data.iterrows(): 
                            if "X" in str(row.values): # tagging the control type
                                cleaned_data["control_type"].append(row.values[0].replace("X", "").strip().lower())
                                cleaned_data["indicator"].append("yes")
                                report["num_control_types"] += 1

                            else:
                                cleaned_data["control_type"].append(row.values[0].strip().lower())
                                cleaned_data["indicator"].append("no")
                    
                    elif "bien" in str(second_item_row).lower(): # format 2.2.2
                        report["format"] = "2.2.2"

                        for _, row in data.iterrows():
                            if "X" in str(row.values): # tagging the control type
                                cleaned_data["control_type"].append(row.row.strip().lower())
                                cleaned_data["indicator"].append("yes")
                                report["num_control_types"] += 1

                            else:
                                cleaned_data["control_type"].append(row.row.strip().lower())
                                cleaned_data["indicator"].append("no")
                    
                    else:
                        report["format"] = "2.2-no-format"
            
            else:
                report["format"] = "no-format"

            cleaned_df = pd.DataFrame(cleaned_data)
            cleaned_df.to_csv(output_path + f"/{excel_file.split("/")[-1].split(".xlsx")[0]}" + ".csv", index=False, sep="|")
            report["num_rows_post_cleaning"] = len(cleaned_df)
            
    except:
        report["num_control_types"] = "-"
    
    return pd.DataFrame([report])


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
    reports_path = pdf_tables_dict["reports_path"]
    
    files_paths = glob(input_path + "/*.xlsx", recursive=True) # reading pdfs from input path
    paths_splitted = list(divide_chunks(files_paths, int(len(files_paths)/NUM_JOBS)))
    paths_for_job = paths_splitted[int(TASK_ID)-1]

    formatting_reports = []
    for item in tqdm(paths_for_job):
        if "table_2." in item and  "~" not in item and "parsing_reports" not in item:
            current_report = table_2_data_corrector(item, output_path)
            formatting_reports.append(current_report)
    
    # exporting formatting reports
    pd.concat(formatting_reports).to_csv(reports_path + r"/formatting_reports" + f"_{TASK_ID}.csv", sep="|", index=False)