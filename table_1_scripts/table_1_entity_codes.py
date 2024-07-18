import pandas as pd
from pdf_reports_tables.table_1_scripts.aux_tools import *

   
# loading the data
dict_closest_matches_1 = read_pickle("dict_closest_matches_1.pkl", ".")
dict_closest_matches_2 = read_pickle("dict_closest_matches_2.pkl", ".")
dict_closest_matches_3 = read_pickle("dict_closest_matches_3.pkl", ".")

# checking num of obs per dict w/out mach
matches_public_firms = pd.DataFrame(dict_closest_matches_1.items(), columns=["table_1_entity", "cent_entidad"]).drop_duplicates()
matches_sector_entities = pd.DataFrame(dict_closest_matches_2.items(), columns=["table_1_entity", "NOMBRE_ENTIDAD"]).drop_duplicates()
matches_entidades_mef_22 = pd.DataFrame(dict_closest_matches_3.items(), columns=["table_1_entity", "ENTIDAD PÚBLICA"]).drop_duplicates()

# reading the data
public_firms = pd.read_csv("public_firms.csv", sep="|")
sector_entities = pd.read_excel("Entidades-Sector.xlsx")
entidades_mef_22 = pd.read_excel("resultados_2022_21.xlsx", sheet_name="INCO 2022 - ENTIDADES")

# cleaning the entity names
public_firms["cent_entidad"] = public_firms["cent_entidad"].apply(lambda cent_entidad: cent_entidad.strip().lower())
sector_entities["NOMBRE_ENTIDAD"] = sector_entities["NOMBRE_ENTIDAD"].apply(lambda NOMBRE_ENTIDAD: NOMBRE_ENTIDAD.strip().lower())
entidades_mef_22["ENTIDAD PÚBLICA"] = entidades_mef_22["ENTIDAD PÚBLICA"].apply(lambda ENTIDAD_PUBLICA: ENTIDAD_PUBLICA.strip().lower())

# merging the data with the table 1 entity names w the other tables
public_firms_matches = pd.merge(public_firms, matches_public_firms, on="cent_entidad", how="right")[["table_1_entity", "cent_entidad", "cent_codigo"]].rename(columns={"table_1_entity": "entity"})
sector_entities_matches = pd.merge(sector_entities, matches_sector_entities, on="NOMBRE_ENTIDAD", how="right")[["table_1_entity", "NOMBRE_ENTIDAD", "CODIGO_ENTIDAD"]].rename(columns={"table_1_entity": "entity", "NOMBRE_ENTIDAD": "cent_entidad", "CODIGO_ENTIDAD": "cent_codigo"})
entidades_mef_22_matches = pd.merge(entidades_mef_22, matches_entidades_mef_22, on="ENTIDAD PÚBLICA", how="right")[["table_1_entity", "ENTIDAD PÚBLICA", "COD ENTIDAD"]].rename(columns={"table_1_entity": "entity", "ENTIDAD PÚBLICA": "cent_entidad", "COD ENTIDAD": "cent_codigo"})

# concatenating and exporting the data
all_matches = pd.concat([public_firms_matches, sector_entities_matches, entidades_mef_22_matches], axis=0)
all_matches.drop_duplicates(subset="entity", keep="first", inplace=True)
all_matches.to_csv("entity_matches.csv", sep="|", index=False)

#  counting the number of reports per entity (using the name found in table 1)
table_1 = pd.read_csv("table_1.csv", sep="|")

table_1 = table_1[~table_1["entity"].isnull()]
table_1["entity"] = table_1["entity"].apply(lambda entity: entity.strip().lower())
table_1["entity"].value_counts().to_csv("table_1_entity_counts.csv", sep="|")

#  counting the number of reports per entity (using the name matched from the government)
table_1_post_merge = table_1.merge(all_matches, on="entity", how="left") 

table_1_post_merge["cent_entidad"].value_counts().to_csv("table_1_matched_entity_counts.csv", sep="|")

table_1_post_merge[table_1_post_merge["cent_entidad"] == "No match found"]["entity"].value_counts().to_csv("table_1_no_match_entity_counts.csv", sep="|")
