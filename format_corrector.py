import pandas as pd 

# specify Excel file formatted incorrectly 
excel_file = "000002-2021-CG-APP-ficha_resumen_url_table_1.xlsx"

# read Excel file into pandas dataframe 
df = pd.read_excel(excel_file)

# fix 'row' column 

# create variable to store current phrase being evaluated 
current_phrase = "" 

# iterate through row column 
for index, row in df.iterrows(): 
    cell_value = row['row'] 

    if cell_value.endswith(":"): 
        # add current cell ending with : to current_phrase 
        current_phrase += cell_value 

        # update cell with full combined phrase 
        df.at[index, 'row'] = current_phrase 

        current_phrase = "" 
    else: 
        # add cell to current phrase until colon is found 
        current_phrase += cell_value 

# drop empty cells in column 
df['row'].replace('', pd.NA, inplace=True) 

# fix 'value' column 
df.at[2, 'value'] += " " + df.at[3, 'value'] + " " + df.at[4, 'value'] 
df.at[7, 'value'] += " " + df.at[8, 'value'] + " " + df.at[9, 'value'] + " " + df.at[10, 'value'] + " " + df.at[11, 'value'] 
# special case for 15, 16 
indicator_words = df.at[15, 'value'].split() 
value_words = df.at[16, 'value'].split() 
value_words = value_words[0:2] + [value_words[2] + " " + value_words[3]]
df.at[15, 'value'] = str([indicator_words[i] + " " + value_words[i] for i in range(len(indicator_words))])
df.at[19, 'value'] += " " + df.at[20, 'value']

# drop irrelevant rows 
df.drop([1, 3, 4, 6, 8, 9, 10, 11, 14, 16, 18, 20], inplace=True) 

# renumber initial column 
df.iloc[:, 0] = range(0, len(df)) 

# write corrected dataframe back to an Excel sheet 
df.to_excel("corrected_excel_file.xlsx", index=False) 