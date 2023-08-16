#! /usr/bin/env python3
# -------------------------------------------------------------------------------
# Name:        features
# Purpose:     Common features for report generating apps
#
# Author:      phenomroman
#
# Created:     16-08-2023
# Copyright:   (c) phenomroman 2023
# Licence:     BSD
# -------------------------------------------------------------------------------
from time import sleep
import pandas as pd

# function to show loading animation
def loading(done, message="Loading: ", symbols=['\\', '|', '/', '-']):
    i = 0
    while not done.is_set():
        print(message, end="")
        print(f"{symbols[i]}", flush=True, end="\r")
        sleep(0.25)
        i = (i + 1) % len(symbols)

# function to take user input for branch choices
def user_input(question):
    check = input(f"{question} Y/N: ").lower().strip()
    try:
        if check[0] == 'y':
            return True
        elif check[0] == 'n':
            return False
        else:
            print("Invalid input")
            print("Please enter a valid answer between Y and N")
            return user_input(question)
    except Exception as e:
        print(e)
        print("Please enter a valid answer between Y and N")
        return user_input(question)
    
def auto_column_width(sheet, dataframe, ignore_list=[]):
    for column in dataframe.columns:
        # get desired column width
        column_length = max(dataframe[column].astype(str).map(len).max(), len(column))
        adjusted_width = (column_length + 2) * 1.11
        # modify width of column A through Z besides ignore list
        for col in [chr(i) for i in range(ord('A'), ord('Z')) if chr(i) not in ignore_list]:
            sheet.column_dimensions[col].width = adjusted_width

def html_to_xl(outfile, url, table_range, cols, ignore_list=[]):
    # read html file tables and concatenate required tables into one dataframe
    tables = pd.read_html(url)
    df = pd.concat(tables[table_range], ignore_index=True)
    # set the column headers and convert required column data into numeric values
    df.columns = cols
    # convert required columns into numeric value
    for name in [col for col in cols if col not in ignore_list]:
        df[name] = pd.to_numeric(df[name], errors='coerce')
    df = df.dropna() #remove blank rows
    # output to excel file and return data
    df.to_excel(outfile, index=False, float_format='%.2f')
    return df

def modify_raw(bo_raw, bo_file, key_id, sheet_name='Report1', row_index=3, col_required=True, row_ignore=[]):
    # set header with proper row & delete blank rows for key_id
    df = pd.read_excel(bo_raw, sheet_name=sheet_name, header=row_index-1).dropna(subset=[key_id])
    # for data filtering, create new column with product code from contract
    if col_required:
        new_column = pd.Series(df[key_id].str[3:7], index=df.index)
        df.insert(0, 'Code', new_column)
        #exclude unnecessary rows by product codes
        df = df.loc[~df['Code'].isin(row_ignore)]
    # else remove non-digit data from key column
    else:
        df[key_id].replace(regex=True, inplace=True, to_replace=r'\D', value=r'')
    # exclude columns with blank data
    df.dropna(axis=1, how='all', inplace=True)
    df.to_excel(bo_file, sheet_name=sheet_name, float_format='%.2f', index=False, engine='openpyxl')
    return df
