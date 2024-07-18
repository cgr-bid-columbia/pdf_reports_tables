import pandas as pd
import numpy as np
from pdf_reports_tables.table_1_scripts.aux_tools import *
import psycopg2, json, datetime, tqdm
import matplotlib.pyplot as plt
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process 
    
    
# loading the data
dict_closest_matches_1 = read_pickle("dict_closest_matches_1.pkl", ".")
dict_closest_matches_2 = read_pickle("dict_closest_matches_2.pkl", ".")
dict_closest_matches_3 = read_pickle("dict_closest_matches_3.pkl", ".")

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

# combining the dicts
final_dict = {}
for entity in dict_closest_matches_1.keys():
    if dict_closest_matches_1[entity] != "No match found":
        final_dict[entity] = dict_closest_matches_1[entity]
    elif dict_closest_matches_2[entity] != "No match found":
        final_dict[entity] = dict_closest_matches_2[entity]
    elif dict_closest_matches_3[entity] != "No match found":
        final_dict[entity] = dict_closest_matches_3[entity]
    else:
        final_dict[entity] = "No match found"

# saving the final dict as df
final_df = pd.DataFrame.from_dict(final_dict, orient="index").reset_index()
final_df.columns = ["entity", "matches"]
final_df.to_csv("final_entities_df.csv", sep="|", index=False)


# matching the data (first dict)
table_1 = pd.read_csv("table_1.csv", sep="|")
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

all_matches = pd.concat([matched_df_1, matched_df_2, matched_df_3], axis=0)
print("length all matches:", len(all_matches))

all_obs = pd.concat([all_matches, unmatched_df_3], axis=0)
all_obs.to_csv("all_obs_post_match.csv", sep="|", index=False)