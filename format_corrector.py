import pandas as pd 
import json 

# define file path 
file_path = "/Users/jacobposada/columbia/econ research/Project-Report-Format/"

# specify Excel file formatted incorrectly 
excel_file = "20-2018-OCI-MPP-ficha_resumen_url_table_1.xlsx"

# read Excel file into pandas dataframe 
df = pd.read_excel(file_path + excel_file)


### FUNCTION TO PARSE COLUMN 1 

def parse_column_1(df): 
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


### FUNCTIONS TO PARSE COLUMN 2 BY FORMAT TYPE 

def parse_column_2_format_1(df): 
## formats 'value' column, works for file format 2 
    
    # fill empty value rows with empty strings 
    df.fillna({'value': ''}, inplace=True) 

    # turn all value columns to strings 
    df['value'] = df['value'].astype(str) 

    # manually corrects and cleans rows 
    df.iloc[2,2] += " " + df.iloc[3, 2] + " " + df.iloc[4, 2] + " " + df.iloc[5, 2] + " " + df.iloc[6, 2] 
    df.iloc[3:7, 2] = "" 
    df.iloc[8,2] += " " + df.iloc[9, 2] 
    df.iloc[9,2] = ""

    # push empty rows to bottom 
    df['value'] = sorted(df['value'], key=lambda x: x == '') 

    return df 


def parse_column_2_format_2(df): 
## formats 'value' column, works for file format 3 
    
    # fill empty value rows with empty strings 
    df.fillna({'value': ''}, inplace=True) 

    # turn all value columns to strings 
    df['value'] = df['value'].astype(str) 

    # manually corrects and cleans rows 
    df.iloc[1,2] += df.iloc[2,2]
    df.iloc[2,2] = "" 
    df.iloc[4,2] += df.iloc[5,2] 
    df.iloc[5,2] = "" 

    # push empty rows to bottom 
    df['value'] = sorted(df['value'], key=lambda x: x == '') 

    return df 

def parse_column_2_format_3(df): 
## formats 'value' column, works for file format 1 
    
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

def parse_column_2_format_4(df): 

    # fill empty value rows with empty strings 
    df.fillna({'value': ''}, inplace=True) 

    # turn all value columns to strings 
    df['value'] = df['value'].astype(str) 

    # manually corrects and cleans rows 
    df.iloc[1,2] += " " + df.iloc[2,2] + " " + df.iloc[3,2] + " " + df.iloc[4,2] + " " + df.iloc[5,2] + " " + df.iloc[6,2] 
    df.iloc[2,2] = " "
    df.iloc[3:7, 2] = "" 
    df.iloc[11, 2] += " " + df.iloc[12,2] 
    df.iloc[12,2] = "" 

    # push empty rows to bottom 
    df['value'] = sorted(df['value'], key=lambda x: x == '')

    return df 

def parse_column_2_format_5(df): 

    # fill empty value rows with empty strings 
    df.fillna({'value': ''}, inplace=True) 

    # turn all value columns to strings 
    df['value'] = df['value'].astype(str) 

    # manually corrects and cleans rows 
    df.iloc[1,2] += " " + df.iloc[2,2] + " " + df.iloc[3,2] 
    df.iloc[2:4, 2] = "" 
    df.iloc[8, 2] += df.iloc[9,2] 
    df.iloc[9,2] = "" 
    df.iloc[14,2] += df.iloc[15,2] 
    df.iloc[15,2] = "" 
    
    # push empty rows to bottom 
    df['value'] = sorted(df['value'], key=lambda x: x == '')

    return df 


### FUNCTIONS TO FORMAT ENTIRE TABLE BY FORMAT TYPE 

def parse_format_1(df): 
## formats all columns, works for file format 1 
    
    # parse column 1 titled 'row' 
    df = parse_column_1(df) 

    # parse column 2 titled 'value' 
    df = parse_column_2_format_1(df) 

    # relabel index column 
    df.iloc[:, 0] = range(0, len(df['row'])) 

    # write corrected dataframe back to an Excel sheet 
    df.to_excel(file_path + excel_file + "_parsed.xlsx", index=False) 

    return df 


def parse_format_2(df): 
## formats all columns, works for file format 2 
    
    # parse column 1 titled 'row' 
    df = parse_column_1(df)

    # parse column 2 titled 'value' 
    df = parse_column_2_format_2(df) 

    # relabel index column 
    df.iloc[:, 0] = range(0, len(df['row'])) 

    # write corrected dataframe back to an Excel sheet 
    df.to_excel(file_path + excel_file + "_parsed.xlsx", index=False) 

    return df 


def parse_format_3(df): 
## formats all columns, works for file format 2 
    
    # parse column 1 titled 'row' 
    df = parse_column_1(df)

    # parse column 2 titled 'value' 
    df = parse_column_2_format_3(df) 

    # relabel index column 
    df.iloc[:, 0] = range(0, len(df['row'])) 

    # write corrected dataframe back to an Excel sheet 
    df.to_excel(file_path + excel_file + "_parsed.xlsx", index=False) 

    return df 

def parse_format_4(df): 
## formats all columns, works for file format 2 
    
    # parse column 1 titled 'row' 
    df = parse_column_1(df)

    # parse column 2 titled 'value' 
    df = parse_column_2_format_4(df) 

    # relabel index column 
    df.iloc[:, 0] = range(0, len(df['row'])) 

    # write corrected dataframe back to an Excel sheet 
    df.to_excel(file_path + excel_file + "_parsed.xlsx", index=False) 

    return df 

def parse_format_5(df): 
## formats all columns, works for file format 2 
    
    # parse column 1 titled 'row' 
    df = parse_column_1(df)

    # parse column 2 titled 'value' 
    df = parse_column_2_format_5(df) 

    # relabel index column 
    df.iloc[:, 0] = range(0, len(df['row'])) 

    # write corrected dataframe back to an Excel sheet 
    df.to_excel(file_path + excel_file + "_parsed.xlsx", index=False) 

    return df 

df = parse_column_1(df) 
df.to_excel(file_path + excel_file + "_parsed.xlsx", index=False) 
