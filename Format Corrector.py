import pandas as pd 

# specify Excel file formatted incorrectly 
excel_file = "000002-2021-CG-APP-ficha_resumen_url_table_1.xlsx"

# read Excel file into pandas dataframe 
df = pd.read_excel(excel_file)

# fix 'row' column 
df.at[0, 'row'] += " " + df.at[1, 'row']
df.at[2, 'row'] += " " + str(df.at[3, 'row']) 
df.at[5, 'row'] += " " + df.at[6, 'row']
df.at[7, 'row'] += " " + df.at[8, 'row']
df.at[13, 'row'] += " " + df.at[14, 'row']
df.at[17, 'row'] += " " + df.at[18, 'row']
df.at[19, 'row'] += " " + df.at[20, 'row']

# fix 'value' colum 
df.at[2, 'value'] += " " + df.at[3, 'value'] + " " + df.at[4, 'value'] 
df.at[7, 'value'] += " " + df.at[8, 'value'] + " " + df.at[9, 'value'] + " " + df.at[10, 'value'] + " " + df.at[11, 'value'] 
# special case for 15, 16 
indicator_words = df.at[15, 'value'].split() 
value_words = df.at[16, 'value'].split() 
df.at[15, 'value'] = [indicator_words[i] + " " + value_words[i] for i in range(len(indicator_words))]

# drop irrelevant rows 
df.drop([1, 3, 4, 6, 8, 9, 10, 11, 14, 16, 18, 20], inplace=True) 

# renumber initial column 
df.iloc[:, 0] = range(0, len(df)) 

# write corrected dataframe back to an Excel sheet 
df.to_excel("corrected_excel_file.xlsx", index=False) 