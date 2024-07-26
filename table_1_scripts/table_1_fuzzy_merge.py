import pandas as pd
import numpy as np
import psycopg2, json, datetime, tqdm, pickle
import matplotlib.pyplot as plt
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process 

def create_pickle(object_name, file_name: str, path: str) -> None:
    """
    Creates a pickle file for object. Note: Path should have no slash 
    at the end
    """
    with open(path + f"/{file_name}", "wb") as storing_output:
        pickle.dump(object_name, storing_output)
        storing_output.close()


def read_pickle(file_name: str, path: str) -> None:
    """
    Reads pickle file from specified path 
    """
    pickle_file = open(path + f"/{file_name}", "rb")
    output = pickle.load(pickle_file)
    pickle_file.close()
    return output


def select_observations(host, database, user, password, schema, table, num_observations):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    
    # Create a cursor object to interact with the database
    cur = conn.cursor()
    
    # Build the SQL query to select the specified number of observations
    if num_observations != "*":
        query = f'SELECT * FROM "{schema}".{table} LIMIT {num_observations}'
    else:
        query = f'SELECT * FROM "{schema}".{table}'
    
    observations = pd.read_sql_query(query,con=conn)
   
    # Close the cursor and the database connection
    cur.close()
    conn.close()
    
    # Return the selected observations
    return observations


def clean_amount_val(amount: str, exchange_rate=3.8) -> None:
    """
    Obtains the amount values from the table_1 object
    """

    ## cleaning amount date var
    if type(amount) == str:
        if "S/" in amount: # cleaning soles
            current_val = amount.replace("S/", "").strip()
            if not any(char.isalpha() for char in current_val):
                return float(current_val)
            else:
                return None
        elif "US$" in amount: # cleaning dollars
            current_val = amount.replace("US$", "").strip()
            if not any(char.isalpha() for char in current_val):
                return float(current_val) * exchange_rate
            else:
                return None
    else:
        return amount


def clean_date_val(date: str) -> None:
    """
    Obtains the date values from the table_1 object
    """

    ## cleaning issue date var
    if type(date) == str:
        current_date = date.strip()
        if not any(char.isalpha() for char in current_date):
            formatted_date = datetime.datetime.strptime(current_date, "%d/%m/%Y")
            return int(formatted_date.split("/")[-1])
        else:
            return None
    else:
        return date
    

def find_match(item: str, mapping: dict) -> str:
    """
    Finds the match for the item in the mapping dictionary
    """

    if type(item) is str:
        fixed_item = item.strip().lower()
        if fixed_item in mapping.keys():
            return mapping[fixed_item]
        else:
            return "No match found"
    else:
        return "No match found"


if __name__ == "__main__":
    # read the parameters from the JSON file
    with open("paths.json") as json_file:
        parameters = json.load(json_file)

    # extract the connection string, azure and google credentials
    host = parameters["host"]
    database_public_firms = parameters["database_public_firms"]
    user = parameters["user"]
    password = parameters["password"]
    schema_public_firms = parameters["schema_public_firms"]
    table_public_firms = parameters["table_public_firms"]
    database_table_1 = parameters["database_table_1"]
    schema_table_1 = parameters["schema_table_1"]
    table_table_1 = parameters["table_table_1"]

    # obtaining data from public firms
    public_firms = pd.read_csv("public_firms.csv", sep="|")
    
    # obtaining data from table 1
    table_1 = pd.read_csv("table_1.csv", sep="|")
    print("table 1 - unique vals entity: ", len(table_1["entity"].unique()))
    table_1["amount"] = table_1["amount"].apply(lambda amount: clean_amount_val(amount))

    # obtaining data from MEF dataset
    sector_entities = pd.read_excel("Entidades-Sector.xlsx")

    # obtaining data from public firms
    entidades_mef_22 = pd.read_excel("resultados_2022_21.xlsx", sheet_name="INCO 2022 - ENTIDADES")


    ########### fuzzy matching ###########

    # reading public entities from table 1: converting strings to list
    reports_entities_names = table_1["entity"].str.lower().to_list()
    reports_entities_names = [str(name.strip()) for name in reports_entities_names if type(name) == str]

    # reading public entities from master table: converting strings to list
    public_entities_names = public_firms["cent_entidad"].to_list()
    public_entities_names = [name.strip().lower() for name in public_entities_names]

    # reading public entities from MEF dataset: converting strings to list
    sector_entities_names = sector_entities["NOMBRE_ENTIDAD"].str.lower().to_list()
    sector_entities_names = [str(name.strip()) for name in sector_entities_names if type(name) == str]

    # reading public entities from MEF dataset: converting strings to list
    entidades_mef_22_names = entidades_mef_22["ENTIDAD PÚBLICA"].str.lower().to_list()
    entidades_mef_22_names = [str(name.strip()) for name in entidades_mef_22_names if type(name) == str]

    # erasing duplicates
    reports_entities_names = list(set(reports_entities_names))

    # finding entities that are in both lists
    entities_both = []
    for public_entity in public_entities_names:
        if public_entity in reports_entities_names:
            entities_both.append(public_entity)

    create_pickle(entities_both, "entities_both.pkl", ".") # saving entities that are in both lists

    threshold = 80 # fuzzy merge: taking the threshold as 80 

    # placeholders for fuzzy merge
    dict_closest_matches = {}
    
    # finding entities that are not in both lists
    reports_entities_names_fuzzy = [name for name in reports_entities_names if name not in entities_both]
    print("len reports_entities_names_fuzzy: ", len(reports_entities_names_fuzzy))
    
    # finding closest match for each entity
    for report_entity in tqdm.tqdm(reports_entities_names_fuzzy):
        match = process.extractOne(report_entity, public_entities_names, scorer=fuzz.ratio)

        if match[1] >= threshold: # keeping if similarity is above threshold
            dict_closest_matches[report_entity] = match[0]
        else:
            dict_closest_matches[report_entity] = "No match found"
    
    print("len dict_closest_matches 1: ", len(dict_closest_matches.keys()))

    #### no-fuzzy match entities ####
    # saving the matches with no need of fuzzy
    for report_entity in tqdm.tqdm(reports_entities_names):
        if report_entity in entities_both:
            dict_closest_matches[report_entity] = report_entity

    print("len dict_closest_matches 2: ", len(dict_closest_matches.keys()))

    # storing fuzzy and no-fuzzy match reports entities names 
    create_pickle(dict_closest_matches, "dict_closest_matches_1.pkl", ".") 

    # storing unmatched entities
    unmatched_entities_1 = [name for name in dict_closest_matches.keys() if dict_closest_matches[name] == "No match found"]

    print("len dict_closest_matches 2: ", len(dict_closest_matches.keys()))
    print("len unmatched_entities_1: ", len(unmatched_entities_1))

    ## unmatched data 1

    # placeholders for fuzzy merge - 2nd round
    dict_closest_matches_2 = {}

    # matching data w sector_entities_names
    for report_entity in tqdm.tqdm(unmatched_entities_1):
        match = process.extractOne(report_entity, sector_entities_names, scorer=fuzz.ratio)

        if match[1] >= threshold: # keeping if similarity is above threshold
            dict_closest_matches_2[report_entity] = match[0]
        else:
            dict_closest_matches_2[report_entity] = "No match found"

    print("len dict_closest_matches_2: ", len(dict_closest_matches_2.keys()))
    # storing fuzzy match: reports entities names fuzzy
    create_pickle(dict_closest_matches_2, "dict_closest_matches_2.pkl", ".") 


    # storing unmatched entities
    unmatched_entities_2 = [name for name in dict_closest_matches_2.keys() if dict_closest_matches_2[name] == "No match found"]
    print("len unmatched_entities_2: ", len(unmatched_entities_2.keys()))

    ## unmatched data 2

    # placeholders for fuzzy merge - 3rd round
    dict_closest_matches_3 = {}

    # matching data w sector_entities_names
    for report_entity in tqdm.tqdm(unmatched_entities_2):
        match = process.extractOne(report_entity, entidades_mef_22_names, scorer=fuzz.ratio)

        if match[1] >= threshold: # keeping if similarity is above threshold
            dict_closest_matches_3[report_entity] = match[0]
        else:
            dict_closest_matches_3[report_entity] = "No match found"

    print("len dict_closest_matches_3: ", len(dict_closest_matches_3.keys()))
    # storing fuzzy match: reports entities names fuzzy
    create_pickle(dict_closest_matches_3, "dict_closest_matches_3.pkl", ".") 

