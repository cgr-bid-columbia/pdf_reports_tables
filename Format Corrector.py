import pandas as pd 

# read Excel file into pandas dataframe 
df = pd.read_excel("000002-2021-CG-APP-ficha_resumen_url_table_1.xlsx")

# fix 'row' column 
df.at[0, 'row'] += " " + df.at[1, 'row']
df.at[2, 'row'] += " " + str(df.at[3, 'row']) 
df.at[5, 'row'] += " " + df.at[6, 'row']
df.at[7, 'row'] += " " + df.at[8, 'row']
df.at[13, 'row'] += " " + df.at[14, 'row']
df.at[17, 'row'] += " " + df.at[18, 'row']
df.at[19, 'row'] += " " + df.at[20, 'row']

