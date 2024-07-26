import pandas as pd
import numpy as np
from aux_tools import *
import psycopg2, json, datetime, tqdm
import matplotlib.pyplot as plt
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process 

### loading the data ### 

# reading the matchesss
dict_closest_matches_1 = read_pickle("dict_closest_matches_1.pkl", ".")
dict_closest_matches_2 = read_pickle("dict_closest_matches_2.pkl", ".")
dict_closest_matches_3 = read_pickle("dict_closest_matches_3.pkl", ".")
entities_both = read_pickle("entities_both.pkl", ".")

# reading dataframes w entity codes
public_firms = pd.read_csv("public_firms.csv", sep="|")
sector_entities = pd.read_excel("Entidades-Sector.xlsx")
entidades_mef_22 = pd.read_excel("resultados_2022_21.xlsx", sheet_name="INCO 2022 - ENTIDADES")

# reading table 1
table_1 = pd.read_csv("table_1.csv", sep="|")

### checking the data for unmatched obs ###

# checking num of obs per dict w/out mach
no_matches_1 = [entity for entity in dict_closest_matches_1.keys() if dict_closest_matches_1[entity] == "No match found"]
print("len(no_matches_1): ", len(no_matches_1))
print("% no_matches_1: ", len(no_matches_1)/len(dict_closest_matches_1.keys()))
print("total obs: ", len(dict_closest_matches_1.keys()))
no_matches_2 = [entity for entity in dict_closest_matches_2.keys() if dict_closest_matches_2[entity] == "No match found"]
print("len(no_matches_2): ", len(no_matches_2))
print("% no_matches_2: ", len(no_matches_2)/len(dict_closest_matches_2.keys()))
print("total obs: ", len(dict_closest_matches_2.keys()))
no_matches_3 = [entity for entity in dict_closest_matches_3.keys() if dict_closest_matches_3[entity] == "No match found"]
print("len(no_matches_3): ", len(no_matches_3))
print("% no_matches_3: ", len(no_matches_3)/len(dict_closest_matches_3.keys()))
print("total obs: ", len(dict_closest_matches_3.keys()))

### obtaining the matches for each entity name in table 1 ###

# obtaining entity names from table 1
reports_entities_names = table_1["entity"].str.lower().to_list()
reports_entities_names = [str(name.strip()) for name in reports_entities_names if type(name) == str]
reports_entities_names = list(set(reports_entities_names))

# combining the dicts
final_dict = {}
count_matches = 0
for entity in reports_entities_names: # TODO: fix this, it should consider all unique entities in table 1
    if dict_closest_matches_1[entity] != "No match found":
        final_dict[entity] = dict_closest_matches_1[entity]
        count_matches += 1
    elif dict_closest_matches_2[entity] != "No match found":
        final_dict[entity] = dict_closest_matches_2[entity]
        count_matches += 1
    elif dict_closest_matches_3[entity] != "No match found":
        final_dict[entity] = dict_closest_matches_3[entity]
        count_matches += 1
    else:
        final_dict[entity] = "No match found"

# saving the final dict as df
final_df = pd.DataFrame.from_dict(final_dict, orient="index").reset_index()
final_df.columns = ["entity", "matches"]
print("unique vals entity: ", len(final_df["entity"].unique()))
print("count_matches: ", count_matches)
final_df.to_csv("final_entities_df.csv", sep="|", index=False)

# matching the data (first dict)
matches_df_table_1 = table_1[["entity"]]
matches_df_table_1["matches"] = matches_df_table_1["entity"].apply(lambda name: find_match(name, dict_closest_matches_1))

# keeping unmatched obs in a slice of the df
matched_df_1 = matches_df_table_1[matches_df_table_1["matches"] != "No match found"]
unmatched_df_1 = matches_df_table_1[matches_df_table_1["matches"] == "No match found"]

# keeping unmatched obs in a slice of the df
matches_df_table_2 = unmatched_df_1.copy()
matches_df_table_2["matches"] = matches_df_table_2["entity"].apply(lambda name: find_match(name, dict_closest_matches_2))
matched_df_2 = matches_df_table_2[matches_df_table_2["matches"] != "No match found"]
unmatched_df_2 = matches_df_table_2[matches_df_table_2["matches"] == "No match found"]

# keeping unmatched obs in a slice of the df
matches_df_table_3 = unmatched_df_2.copy()
matches_df_table_3["matches"] = matches_df_table_3["entity"].apply(lambda name: find_match(name, dict_closest_matches_3))
matched_df_3 = matches_df_table_3[matches_df_table_3["matches"] != "No match found"]
unmatched_df_3 = matches_df_table_3[matches_df_table_3["matches"] == "No match found"]

# concatenating the matched data
all_matches = pd.concat([matched_df_1, matched_df_2, matched_df_3], axis=0)
print("length all matches:", len(all_matches))
print("length matches:", len(all_matches["matches"].unique()))

# concatenating all the data
all_obs = pd.concat([all_matches, unmatched_df_3], axis=0)
all_obs.rename(columns={"matches": "cent_entidad"}, inplace=True)

# summarizing entity names used for matching
#all_obs["cent_entidad"].value_counts().to_csv("matches_counts.csv", sep="|")

### adding the entity codes ###

# converting pkls to dataframes
matches_public_firms = pd.DataFrame(dict_closest_matches_1.items(), columns=["table_1_entity", "cent_entidad"])
matches_sector_entities = pd.DataFrame(dict_closest_matches_2.items(), columns=["table_1_entity", "NOMBRE_ENTIDAD"])
matches_entidades_mef_22 = pd.DataFrame(dict_closest_matches_3.items(), columns=["table_1_entity", "ENTIDAD PÚBLICA"])

# cleaning the entity names in dataframes w entity codes
public_firms["cent_entidad"] = public_firms["cent_entidad"].apply(lambda cent_entidad: cent_entidad.strip().lower())
sector_entities["NOMBRE_ENTIDAD"] = sector_entities["NOMBRE_ENTIDAD"].apply(lambda NOMBRE_ENTIDAD: NOMBRE_ENTIDAD.strip().lower())
entidades_mef_22["ENTIDAD PÚBLICA"] = entidades_mef_22["ENTIDAD PÚBLICA"].apply(lambda ENTIDAD_PUBLICA: ENTIDAD_PUBLICA.strip().lower())

# merging the data with the table 1 entity names w the other tables
public_firms_codes = pd.merge(public_firms, matches_public_firms, on="cent_entidad", how="right")[["cent_entidad", "cent_codigo"]]
sector_entities_codes = pd.merge(sector_entities, matches_sector_entities, on="NOMBRE_ENTIDAD", how="right")[["NOMBRE_ENTIDAD", "CODIGO_ENTIDAD"]].rename(columns={"NOMBRE_ENTIDAD": "cent_entidad", "CODIGO_ENTIDAD": "cent_codigo"})
entidades_mef_22_codes = pd.merge(entidades_mef_22, matches_entidades_mef_22, on="ENTIDAD PÚBLICA", how="right")[["ENTIDAD PÚBLICA", "COD ENTIDAD"]].rename(columns={"ENTIDAD PÚBLICA": "cent_entidad", "COD ENTIDAD": "cent_codigo"})

# keeping only obs w codes
public_firms_codes = public_firms_codes[public_firms_codes["cent_entidad"] != "No match found"]
sector_entities_codes = sector_entities_codes[sector_entities_codes["cent_entidad"] != "No match found"]
entidades_mef_22_codes = entidades_mef_22_codes[entidades_mef_22_codes["cent_entidad"] != "No match found"]

public_firms_codes.to_csv("public_firms_codes.csv", sep="|", index=False)
sector_entities_codes.to_csv("sector_entities_codes.csv", sep="|", index=False)
entidades_mef_22_codes.to_csv("entidades_mef_22_codes.csv", sep="|", index=False)

# concatenating the data w entity codes
data_codes = pd.concat([public_firms_codes, sector_entities_codes, entidades_mef_22_codes], axis=0)
data_codes.to_csv("data_codes_w_duplicates.csv", sep="|", index=False)
data_codes["cent_entidad"].value_counts().to_csv("data_codes_count_names.csv", sep="|")

data_codes.drop_duplicates(subset="cent_entidad", keep="first", inplace=True)
data_codes.to_csv("data_codes_no_duplicates.csv", sep="|", index=False)
print("length data codes: ", len(data_codes))
print("len cent_entidad unique: ", len(data_codes["cent_entidad"].unique()))
print("len cent_codigo unique: ", len(data_codes["cent_codigo"].unique()))

# merging the data w entity codes
all_obs = pd.merge(all_obs, data_codes, on="cent_entidad", how="left")

# exporting the data
all_obs.to_csv("all_obs_post_match.csv", sep="|", index=False)
all_obs["cent_entidad"].value_counts().to_csv("all_obs_count_names.csv", sep="|")
all_obs["cent_codigo"].value_counts().to_csv("all_obs_count_codes.csv", sep="|")