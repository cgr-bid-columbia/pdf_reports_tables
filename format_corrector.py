import pandas as pd 

# specify Excel file formatted incorrectly 
excel_file = "000002-2021-CG-APP-ficha_resumen_url_table_1.xlsx"

# read Excel file into pandas dataframe 
df = pd.read_excel(excel_file)

def column_one_parser(dataframe): 
## formats 'row' column and cleans up empty cells, works for all files 

    # create variable to store current phrase being evaluated 
    current_phrase = "" 

    # iterate through row column 
    for index, cell_value in df['row'].items(): 
       # fill empty rows with empty strings 
       df.fillna({'row': ''}, inplace=True)
       if cell_value.endswith(":"): 
            # add current cell ending with : to current_phrase 
            current_phrase += cell_value
           
            # update cell with full combined phrase 
            df.at[index, 'row'] = current_phrase 
            current_phrase = "" 
        
       elif cell_value != '':
            # add cell to current phrase until colon is found 
            current_phrase += cell_value + " "
            # replace incomplete rows with empty strings 
            df.at[index, 'row'] = "" 
       
    # brings non-empty rows to the top, maintaining their order 
    df['row'] = sorted(df['row'], key=lambda x: x == '')

    return df 

### FUNCTION FOR VALUE COLUMN 

def column_two_format_one_parser(dataframe): 
## formats 'value' column, works for file format one 
    
    # fill empty value rows with empty strings 
    df.fillna({'value': ''}, inplace=True) 

    # turn all value columns to strings 
    df['value'] = df['value'].astype(str) 

    # manually corrects and cleans rows 
    df.iloc[2, 2] += " " + df.iloc[3, 2] + " " + df.iloc[4, 2] 
    df.iloc[3:5,2] = "" 
    df.iloc[7, 2] += " " + df.iloc[8, 2] + " " + df.iloc[9, 2] + " " + df.iloc[10, 2] + " " + df.iloc[11, 2] 
    df.iloc[8:12,2] = "" 
    df.iloc[15, 2] += " " + df.iloc[16, 2] 
    df.iloc[16,2] = "" 
    df.iloc[19, 2] += " " + df.iloc[20, 2] 
    df.iloc[20,2] = "" 

    # push empty rows to bottom 
    df['value'] = sorted(df['value'], key=lambda x: x == '')

    return df 

# renumber initial column 
df.iloc[:, 0] = range(0, len(df)) 

df = column_one_parser(df) 
df = column_two_format_one_parser(df) 
print(df)

# write corrected dataframe back to an Excel sheet 
df.to_excel("corrected_excel_file.xlsx", index=False) 