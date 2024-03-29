from Levenshtein import distance
from tqdm import tqdm
from glob import glob
import pandas as pd
import math

def table_1_parser(excel_file: str, formatted_names: list) -> pd.DataFrame:

    # reading file
    data = pd.read_excel(excel_file)

    corrected_data = {"name": [], "value": []} # placeholder for the corrected data
    length_of_df = len(data) # getting the length of the dataframe for iterations
    
    for index, row in data.iterrows(): # fixing issues in column with table values
        
        row_is_header = (row["row"] != "") and (row["value"] != "") # detecting rows w header values

        if row_is_header: # working only w header values

            current_name = row["row"] # placeholder for the value of the name column after fixes
            current_value = row["value"] # placeholder for the value of the name column after fixes

            if index + 1 < length_of_df: # checking if there are more post rows to iterate on dataset
                sub_index = index + 1
            
                # defining conditions for the fix implemented on while loop
                available_name = str(data.loc[sub_index, "row"]) != "nan"
                available_data = str(data.loc[sub_index, "value"]) != "nan"

                while not available_name and available_data and sub_index < length_of_df: # detecting rows w empty names and w data
                    current_value += " " + data.loc[sub_index, "value"]
                    
                    if sub_index + 1 <= length_of_df: # checking if there are more rows to iterate
                        sub_index += 1 # updating sub_index and conditions
                        available_name = str(data.loc[sub_index, "row"]) != "nan"
                        available_data = str(data.loc[sub_index, "value"]) == "nan"
                
            corrected_data["name"].append(current_name)
            corrected_data["value"].append(current_value)

    print("--- post first fix ---")
    print("corrected_data['name'] length: ", len(corrected_data['name']))
    print("corrected_data['value'] length: ", len(corrected_data['value']))
    print("corrected_data['name']: ", corrected_data['name'])
    print("corrected_data['value']: ", corrected_data['value'])

    # fixing issues in column with table names
    corrected_names = [] # placeholder for the fixed names
    formatted_names_aux = formatted_names.copy() # creating a copy of the formatted names to simplify the search
    for item in corrected_data["name"]:
        
        if len(formatted_names_aux) > 1: # checking if there are more than one name to compare
            lev_distances = [] 
            for name in formatted_names_aux: # calculating levensthein distances to actual names
                lev_distances.append(distance(item, name))
                
            min_index = lev_distances.index(min(lev_distances)) # getting the index of the minimum distance
            corrected_names.append(formatted_names_aux[min_index]) # appending the fixed name
            formatted_names_aux.remove(formatted_names_aux[min_index]) # removing the used name to simplify the next search
        else:
            corrected_names.append(formatted_names_aux[0])

    corrected_data["name"] = corrected_names

    print("--- post second fix ---")
    print("corrected_data['name'] length: ", len(corrected_data['name']))
    print("corrected_data['value'] length: ", len(corrected_data['value']))
    print("corrected_data['name']: ", corrected_data['name'])
    print("corrected_data['value']: ", corrected_data['value'])

    return corrected_data


if __name__ == "__main__":
    input_path = r"D:\Descargas\PDF tables aux\Original"
    files_paths = glob(input_path + "/*.xlsx", recursive=True) # reading pdfs from input path
    corrected_names = ["N° de informe", "Título del informe", "Detalle", "Entidad auditada", "Monto de la materia de control", "Ubigeo", "Fecha de emisión de informe", "Unidad orgánica que emite el informe"]

    for item in tqdm(files_paths[:2]):
        print("item: ", item)
        output = table_1_parser(item, corrected_names)
        file_name = item.split("\\")[-1].split(".")[0]
        print("file_name: ", file_name)

        pd.DataFrame(output).to_excel(r"D:\Descargas\PDF tables aux\Parsed" + r"\\" + file_name + "_formatted" + ".xlsx", index=False)