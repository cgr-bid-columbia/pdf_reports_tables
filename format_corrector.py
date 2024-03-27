from glob import glob
from tqdm import tqdm
import pandas as pd 
import json

def parse_table_one(file_name: str, output_path: str, output_sufix="_parsed") -> None:
    """
    Parses the first table of the "Informe de Control" and writes 
    the parsed table to a new Excel file.

    Note: the file name should NOT include the ".xlsx" suffix
    """

    # specify Excel file formatted incorrectly 
    excel_file = file_name + ".xlsx"

    # read Excel file into pandas dataframe 
    df = pd.read_excel(excel_file)

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

    # write corrected dataframe back to an Excel sheet 
    df.to_excel(output_path + "/" + file_name + output_sufix + ".xlsx", index=False) 


if __name__ == "__main__":
    # 1. read json file
    pdf_tables_file = open("pdf_tables.json", "r")
    pdf_tables_dict = json.load(pdf_tables_file)

    # 2. read paths
    input_path = pdf_tables_dict["input_path"] 
    output_path = pdf_tables_dict["output_path"]

    # 3. read all the pdfs from input path
    files_paths = glob(input_path + "/*.pdf", recursive=True) # reading pdfs from input path
    first_tables_paths = [path for path in files_paths if "first_table" in path.lower()]

    # 4. run your function `parse_table_1` on all the excel files that contain "first_table" string (e.g. "something_first_table.xlsx")
    for file_path in tqdm(first_tables_paths):
        parse_table_one(input_path + "/" + file_path, output_path)
