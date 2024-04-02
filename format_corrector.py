import pandas as pd 

# specify Excel file formatted incorrectly 
excel_file = "000002-2021-CG-APP-ficha_resumen_url_table_1.xlsx"

# read Excel file into pandas dataframe 
df = pd.read_excel(excel_file)

# fix 'row' column 

# create variable to store current phrase being evaluated 
current_phrase = "" 

# iterate through row column 
for index, cell_value in df['row'].items(): 
    cell_value = str(cell_value)

    if cell_value.endswith(":"): 
        # add current cell ending with : to current_phrase 
        current_phrase += cell_value 

        # update cell with full combined phrase 
        df.at[index, 'row'] = current_phrase 

        current_phrase = "" 
    else: 
        # add cell to current phrase until colon is found 
        current_phrase += cell_value 

# fix 'value' column 
df['value'] = df['value'].astype(str) 
df.iloc[2, 2] += " " + df.iloc[3, 2] + " " + df.iloc[4, 2] 
df.iloc[7, 2] += " " + df.iloc[8, 2] + " " + df.iloc[9, 2] + " " + df.iloc[10, 2] + " " + df.iloc[11, 2] 
df.iloc[15, 2] += " " + df.iloc[16, 2] 
df.iloc[19, 2] += " " + df.iloc[20, 2]

# drop irrelevant rows 
df.dropna(subset=['row'], inplace=True)
df.dropna(subset=['value'], inplace=True)


# renumber initial column 
df.iloc[:, 0] = range(0, len(df)) 

# write corrected dataframe back to an Excel sheet 
df.to_excel("corrected_excel_file.xlsx", index=False) 