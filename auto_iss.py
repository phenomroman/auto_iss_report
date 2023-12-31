#! /usr/bin/env python3
# -------------------------------------------------------------------------------
# Name:        auto_iss
# Purpose:     Automatically generate ISS report from html balance sheet and BO files
#
# Author:      phenomroman
#
# Created:     01-08-2023
# Copyright:   (c) phenomroman 2023
# Licence:     BSD
#-------------------------------------------------------------------------------
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Thread, Event, Lock
from time import sleep
from feats import loading, user_input, auto_column_width, html_to_xl, modify_raw
import pandas as pd
import os

# get last month as report period name and define reports
report_period = datetime.strftime(datetime.today().replace(day=1) - timedelta(days=1), '%B%Y')
report_options = {1: 'ISS Import Loans', 2: 'ISS Import Bills Acceptance', 3: 'ISS Export Local Bills'}

# function to calculate loan related ISS report
def iss_import_loan(br_codes, exclude_br=[]):
    # create directories if not exist
    if not os.path.exists('iss_import_loan/work_files'):
        os.makedirs('iss_import_loan/work_files')
    # create empty dataframes inside a dictionary based on given branch names to fill branch data later
    df_dic = {}
    for br_code in br_codes:
        df_dic[br_code] = {}
    # get same month adjusted data in dataframe
    same_m_adjust_df = same_m_adjustments('RAW_BO', br_codes)
    # define required general variables for branchwise calculation
    # main loan catagories
    particulars = ['Total PAD (General)', 'Total PAD (Capitalized)', 'Total PAD (EDF)', 'Total LTR/MPI',
                    'Total LIM', 'Total Loan Disbursed and Settled within this Month',
                    'Total Amount of LTR Converted to Term Loan',
                    'Total Outstanding of Term Loan Converted from Continuous, Demand and Time Loan',
                    'Total Amount of STL (Except LTR) Converted to Term loan',
                    'Total Amount of Time Loan Converted to Term loan']
    main_corp_gl_pp = [150120005, 150120041, 150120011, 150120009, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA,]
    main_sme_gl_pp = [150120006, 150120047, 150120012, 150120010, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA,]
    main_corp_gl_int = [150420005, 150420041, 150420011, 150420009, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA,]
    main_sme_gl_int = [150420006, 150420047, 150420012, 150420010, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA,]
    main_corp_gl_int_sus = [150820005, 150820039, 150820011, 150820009, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA,]
    main_sme_gl_int_sus = [150820006, 150820045, 150820012, 150820010, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA,]
    df_main = pd.DataFrame(
        {
        'Particulars': particulars + particulars + particulars + particulars + particulars + particulars,
        'GL Code': main_corp_gl_pp + main_sme_gl_pp + main_corp_gl_int + main_sme_gl_int + main_corp_gl_int_sus + main_sme_gl_int_sus
        }
    )
    # other loan catagories
    other_particulars = ['Import Loan', 'Import Loan (Capitalized)', 'Time Loan (new)',
                        'Time Loan (old)', 'Time Loan Amortized', 'Time Loan (Capitalized)',
                        'Term Loan (NEW)', 'Term Loan (OLD)', 'Term Loan (Amortized)',
                        'Term Loan (Amortized) OLD', 'LTFF',]
    other_corp_gl_pp = [150120007, 150120040, 150120025, 150120003, pd.NA, 150120045, 150130024,
                            150130001, 150130026, 150130019, 150130038,]
    other_sme_gl_pp = [150120008, 150120046, 150120026, 150120004, 150220004, 150120051,
                150130025, 150130002, 150130027, 150130020, pd.NA,]
    other_corp_gl_int = [150420007, 150420040, 150420025, 150420003, pd.NA, 150420045,
                    150430025, 150430001, 150430027, 150430019, 150430037,]
    other_sme_gl_int = [150420008, 150420046, 150420026, pd.NA, 150420004, 150420051, 150430026,
                    150430002, 150430028, 150430020, pd.NA,]
    other_corp_gl_int_sus = [150820007, 150820038, 150820024, 150820003, pd.NA, 150820043,
                        150830025, 150830001, 150830027, 150830019, pd.NA,]
    other_sme_gl_int_sus = [150820008, 150820044, 150820025, 150820004, '', 150820049,
                        pd.NA, 150830002, pd.NA, 150830020, pd.NA,]
    df_other = pd.DataFrame(
        {
        'Loan Type': other_particulars + other_particulars + other_particulars +
                    other_particulars + other_particulars + other_particulars,
        'GL Code': other_corp_gl_pp + other_sme_gl_pp + other_corp_gl_int + other_sme_gl_int +
                    other_corp_gl_int_sus + other_sme_gl_int_sus
        }
    )
    # get relevant GL html files
    files = os.listdir('BAL_SHEET')
    # convert html files into excel files and calculate from dataframes
    for br_code in br_codes:
        # find relevant html file based on name from input list
        url = [f'BAL_SHEET/{file}' for file in files if 'BALSHEETBRN' and br_code in file][0]
        # convert html to excel
        df_dic[br_code] = html_to_xl(url=url, table_range=slice(1,-1),
                            cols=['Level', 'Leaf', 'GL Code', 'GL Description',
                            'FCY Balance', 'LCY Balance', 'Total'],
                            ignore_list=['Leaf', 'GL Description'])
        # calculation of other loan catagories
        df_other_all = derive_loan_amount(df_other, df_dic[br_code], 'Loan Type')
        df_other_merged = df_other_all[0]
        df_other_sum = df_other_all[1]
        other_total = df_other_sum['Total'].sum()
        df_other_sum.loc['Total Amount'] = [other_total]
        # calculation of main loan catagories
        df_main_all = derive_loan_amount(df_main, df_dic[br_code], 'Particulars')
        df_main_merged = df_main_all[0]
        df_main_sum = df_main_all[1]
        df_main_sum.loc['Other Loans'] = [other_total]
        # get same month adjusted data for each relevant branch and add data to its main summary
        same_m_adjusted = same_m_adjust_df.loc[same_m_adjust_df['BR.'].isin([br_code]), 'LCY_AMOUNT'].sum()
        df_main_sum.loc['Total Loan Disbursed and Settled within this Month'] = [same_m_adjusted]
        # assign branch data to relevant branch and export data branchwise
        df_dic[br_code] = df_main_sum
        with pd.ExcelWriter(f'iss_import_loan/work_files/iss_{br_code}.xlsx', engine='openpyxl') as writer:
            df_main_sum.to_excel(writer, sheet_name='Main_Summary', float_format='%.2f')
            df_other_sum.to_excel(writer, sheet_name='Other_Summary', float_format='%.2f')
            df_main_merged.to_excel(writer, sheet_name='Main_Details', index=False, float_format='%.2f')
            df_other_merged.to_excel(writer, sheet_name='Other_Details', index=False, float_format='%.2f')
            sheetnames = ['Main_Summary', 'Other_Summary', 'Main_Details', 'Other_Details']
            for sheetname in sheetnames:
                sheet = writer.sheets[sheetname]
                auto_column_width(sheet, df_main_sum)
                auto_column_width(sheet, df_other_sum)
                auto_column_width(sheet, df_main_merged)
                auto_column_width(sheet, df_other_merged)
    # combine all branch data for final report
    df_final = pd.concat([df_dic[i] for i in br_codes], axis=1)
    df_final.columns = br_codes
    for br in exclude_br: #fillna blank column data for excluded branches
        df_final[br] = pd.NA
    df_final = df_final[sorted(df_final.columns)] #sort columns branchwise
    df_final['Main Operation'] = df_final.sum(axis=1, numeric_only=True)
    particulars.append('Other Loans')
    df_final.insert(0, 'Particulars', particulars)
    # export final output result in excel
    with pd.ExcelWriter(f'iss_import_loan/ISS_Import-Loan_{report_period}.xlsx', engine='openpyxl') as writer:
        df_final.to_excel(writer, float_format='%.2f', index=False)
        # beautify output
        sheet = writer.sheets['Sheet1']
        auto_column_width(sheet, df_final)
        sheet.column_dimensions['A'].width = 55
        col_list = [chr(i) for i in range(ord('B'), ord('Z'))]
        for col in col_list:
            for cell in sheet[col]:
                cell.number_format = '#,##0.00'

# function to get data for loans adjusted within the same month of creation
def same_m_adjustments(indir, br_codes):
    # get input filename from hint
    bo_files = os.listdir(indir)
    infile = [file for file in bo_files if 'same month' in file.lower()][0]
    df = pd.read_excel(f'{indir}/{infile}', sheet_name='Report1', header=2, ).dropna(subset=['PRODUCT_CODE'])
    df['BR.'] = df['RELATED_ACCOUNT'].str[:3]
    br_list = list(df['BR.'].unique())
    br_list = [br_code for br_code in br_list if br_code in br_codes]
    product_codes = ['L035', 'L041', 'L044', 'L047', 'L060', 'L061', 'L062', 'L063', 'L064', 'L072', 'L073', 'L076', 'L223', 'L226', 'L233']
    df = df.loc[df['BR.'].isin(br_list) & df['PRODUCT_CODE'].isin(product_codes)]
    return df

# function to derive loan amount from GL for each branch
def derive_loan_amount(df_cat, df_br, index):
    df_cat_merged = pd.merge(df_cat, df_br, on='GL Code', how='left').drop(['Level', 'Leaf', 'FCY Balance', 'LCY Balance'], axis=1)
    df_cat_sum = df_cat_merged.pivot_table(values='Total', index=index, aggfunc='sum', sort=False)
    return df_cat_merged, df_cat_sum

# function to calculate accepted bill related ISS report
def iss_import_bill(br_codes, exclude_br=[]):
    # create directories if not exist
    if not os.path.exists('iss_import_bill/work_files'):
        os.makedirs('iss_import_bill/work_files')
    # get relevant files
    files = os.listdir('RAW_BO')
    url = [f'RAW_BO/{file}' for file in files if 'bills' in file.lower()] [0]
    # get modified/cleaned data from 508 bills BO
    df_bill = modify_raw(url, 'iss_import_bill/work_files/bill508.xlsx', 'Cont. Ref  No.', 
                         row_ignore=['IB02', 'IB06', 'IB13', 'IB52', 'IB56', 'IB63', 'IB66'])
    # create extra columns with LC and branch codes for further calculation
    lc_code_column = pd.Series(df_bill['Contract No.'].str[6:8], index=df_bill.index)
    br_code_column = pd.Series(df_bill['Cont. Ref  No.'].str[:3], index=df_bill.index)
    df_bill.insert(1, 'LC Code', 'LC' + lc_code_column.astype(str))
    df_bill.insert(2, 'Br. Code', br_code_column)
    # export modified working file
    with pd.ExcelWriter('iss_import_bill/work_files/bill_508.xlsx', engine='openpyxl') as writer:
        df_bill.to_excel(writer, sheet_name='Report1', float_format='%.2f', index=False)
        sheet = writer.sheets['Report1']
        for cell in sheet['G']:
            cell.number_format = 'dd-mm-yyyy;@'
        for cell in sheet['W']:
            cell.number_format = 'dd-mm-yyyy;@'
        auto_column_width(sheet=sheet, dataframe=df_bill, ignore_list=['A', 'B', 'C', 'D', 'L', 'O'])
    # separate contingent liability and get total bill amount
    df_contingent_bill = df_bill.loc[~df_bill['Code'].isin(['IB16'])]
    total_amount_bo = df_contingent_bill['LCY Balance'].sum()
    # convert html file to excel and get GLs in the dataframe
    htmls = os.listdir('BAL_SHEET')
    url = [f'BAL_SHEET/{html}' for html in htmls if 'BALSHEET' in html and 'BALSHEETBRN' not in html][0]
    df_gl = html_to_xl(url=url, table_range=slice(1,-1),
            cols=['Level', 'Leaf', 'GL Code', 'GL Description', 'FCY Balance', 'LCY Balance', 'Total'],
            ignore_list=['Leaf', 'GL Description'])
    # get the relevant acceptance bill GLs to calculate total
    gl_list = [501040000, 501130000, 501140000, 501180000, 501280000, 501290000]
    total_amount_gl = df_gl.loc[df_gl['GL Code'].isin(gl_list), 'Total'].sum()
    # create empty dataframes inside a dictionary based on given branch names to fill branch data later
    df_dic = {}
    for br_code in br_codes:
        df_dic[br_code] = {}
    # define report catagories as particulars in row indices
    particulars = [
        'Accepted Bills Payable (Local)', #LC04, LC99
        'Accepted Bills Payable ( Foreign)', #LC02, LC06, LC10, LC12, LC22, LC25, LC27, (other- LC01)
        'Other Bills Payable', #LC14, LC16
        'Total Acceptance provided Against Inland Bill Related to Export LC', #LC04
        'Total Acceptance Provided Against Inland Bill not Related to Export LC', #LC99
        'Total Acceptance Provided Against Foreign Bill', #LC02, LC06, LC10, LC12, LC22, LC25, LC27, (other- LC01)
        'Total Outstanding of Acceptance Issued Against  FB/IB/AB' #local + foreign + other
    ]
    # calculate ISS for accepted bills if bill amount from BO matches with GL
    if abs(total_amount_bo - total_amount_gl) < 1:
        # define LC Codes and Bill Amount with same variable name as per report catagories
        lc_codes = {
            'local': ['LC04', 'LC99'], 'local_export': ['LC04'], 'local_other': ['LC99'], 
            'foreign': ['LC02', 'LC06', 'LC10', 'LC12', 'LC18', 'LC22', 'LC25', 'LC27'], 
            'foreign_other': ['LC01'], 'other': ['LC14', 'LC16'],
        }
        bill_amount = {
            'local': 0, 'local_export': 0, 'local_other': 0, 'foreign': 0, 'foreign_other': 0, 'other': 0,
        }
        # calculate amount as per report catagories with given branch codes
        for br_code in br_codes:
            df = df_bill.loc[df_bill['Br. Code'].isin([br_code])]
            for report_cat, lc_code in lc_codes.items():
                bill_amount[report_cat] = df.loc[df['LC Code'].isin(lc_code), 'LCY Balance'].sum()
            # create new dataframe with the derived data and set dataframe branchwise
            df_main = pd.DataFrame(
                {
                    'Particulars': particulars,
                    br_code: [bill_amount['local'], bill_amount['foreign'] + bill_amount['foreign_other'],
                              bill_amount['other'], bill_amount['local_export'], bill_amount['local_other'],
                              bill_amount['foreign'] + bill_amount['foreign_other'],  bill_amount['local'] + 
                              bill_amount['foreign'] + bill_amount['foreign_other'] + bill_amount['other']]
                }
            )
            df_dic[br_code] = df_main
    # combine all branch data horizontally, drop columns duplicated during merge, add a total column
    df_all = pd.concat([df_dic[i] for i in br_codes], axis=1)
    for br in exclude_br: #fillna blank column data for excluded branches
        df_all[br] = pd.NA
    df_final = df_all.loc[:, ~df_all.columns.duplicated()]
    key_col = df_final.pop(df_final.columns[0])
    df_final = df_final[sorted(df_final.columns)] #sort columns branchwise
    df_final.insert(0, key_col.name, key_col)
    df_final.insert(len(df_final.columns), 'Main Operation', df_final.sum(axis=1, numeric_only=True))
    # export final output result in excel
    with pd.ExcelWriter(f'iss_import_bill/ISS_Import-Bills_{report_period}.xlsx', engine='openpyxl') as writer:
        df_final.to_excel(writer, float_format='%.2f', index=False)
        # beautify output
        sheet = writer.sheets['Sheet1']
        auto_column_width(sheet, df_final)
        sheet.column_dimensions['A'].width = 55
        col_list = [chr(i) for i in range(ord('B'), ord('Z'))]
        for col in col_list:
            for cell in sheet[col]:
                cell.number_format = '#,##0.00'

# function to calculate export bill related ISS report
def iss_export_bill(br_codes, exclude_br=[]):
    # create directories if not exist
    if not os.path.exists('iss_export_bill/work_files'):
        os.makedirs('iss_export_bill/work_files')
    # define required particulars
    particulars = ['Accepted Bills Receivable (Local)', 'Total Loan Outstanding Against IBP/LDBP',
                    'Total Outstanding of Acceptance Received from Other Bank/branch Against  FBP/IBP/ABP',
                    'Total Acceptance Matured to Other Bank/branch Against  FBP/IBP/ABP',
                    'Unrealized Acceptance Receivable from Other Bank/branch Against  FBP/IBP/ABP',
                    'Total Foreign Currency in Transit', 'Total Foreign Exchange Holding',
                    'BILLS FOR COLLECTION (INLAND BILL SME+CORP)']
    # create empty dataframes inside a dictionary based on given branch names
    df_dic = {}
    for br_code in br_codes:
        df_dic[br_code] = {}
    # get BO files, GL html files and related gl headers
    bo_files = os.listdir('RAW_BO')
    bo_603 = [f"RAW_BO/{file}" for file in bo_files if '603r' in file.lower()][0]
    df_603r = modify_raw(bo_603, 'iss_export_bill/work_files/603R.xlsx', 'Contract Ref No', row_index=4, col_required=True, code=slice(0,3))
    df_603 = df_603r.dropna(subset='Accept Dt.')
    df_603_mod = df_603.loc[~df_603['OPC'].isin(['COL']) & ~df_603['Accept Dt.'].str.match('^[Dd]\w+')]
    bo_matured = [f"RAW_BO/{file}" for file in bo_files if 'mautured' in file.lower()][0]
    df_matured = modify_raw(bo_matured, 'iss_export_bill/work_files/matured.xlsx', 'USER_REF_NO', row_index=4, col_required=True, code=slice(4,7))
    balance_date = datetime.strftime(datetime.today().replace(day=1)-timedelta(days=1), '%Y-%m-%d')
    first_date = datetime.strftime(datetime.today().replace(day=1, month=1), '%Y-%m-%d')
    df_matured['MATURITY_DATE'] = pd.to_datetime(df_matured['MATURITY_DATE'])
    matured_date = (df_matured['MATURITY_DATE'] >= first_date) & (df_matured['MATURITY_DATE'] <= balance_date)
    df_matured_mod = df_matured.loc[df_matured['OPERATION'].isin(['DIS']) & matured_date]
    bo_625 = [f"RAW_BO/{file}" for file in bo_files if 'overdue local' in file.lower()][0]
    df_625 = modify_raw(bo_625, 'iss_export_bill/work_files/625A.xlsx', 'User Ref', row_index=5, col_required=True, code=slice(4,7))
    df_625_mod = df_625.loc[df_625['Opn'].isin(['DIS']) & (df_625['Maturity Date'] <= balance_date)]
    df_exrate = pd.read_excel('RAW_BO/Ex-Rate.xlsx')
    df_625_mod = df_625_mod.merge(df_exrate, how='left', on='Ccy')
    df_625_mod['LCY_AMOUNT'] = df_625_mod['Bill Amt'] * df_625_mod['Ex. Rate']
    files = os.listdir('BAL_SHEET')
    ldbp_gl = [150120019, 150120020, 150120028, 150420019, 150420020, 150420028, 150820027, 150120031, 150420031]
    local_bills_gl = [501240000, 501250000]
    # convert html files into excel format and calculate further from dataframe
    for br_code in br_codes:
        url = [f"BAL_SHEET/{file}" for file in files if 'BALSHEETBRN' and br_code in file][0]
        df_dic[br_code] = html_to_xl(url=url, table_range=slice(1,-1),
                            cols=['Level', 'Leaf', 'GL Code', 'GL Description',
                            'FCY Balance', 'LCY Balance', 'Total'],
                            ignore_list=['Leaf', 'GL Description'])
        # get particular 1 from BO 603R
        df_local_bills = df_603_mod.loc[df_603_mod['Code'].isin([br_code])]
        local_bills_outstanding = df_local_bills['Bill Outstanding LCY'].sum()
        # get particular 2,3 from GL
        ldbp_outstanding = df_dic[br_code].loc[df_dic[br_code]['GL Code'].isin(ldbp_gl), 'Total'].sum()
        # get particular 4 from BO ACCEPTANCE MATURED
        df_matured_bills = df_matured_mod.loc[df_matured_mod['Code'].isin([br_code])]
        matured_acceptance = df_matured_bills['LCY_AMOUNT'].sum()
        # get particular 5 from BO 625
        df_overdue_bills = df_625_mod.loc[df_625_mod['Code'].isin([br_code])]
        overdue_bills = df_overdue_bills['LCY_AMOUNT'].sum()
        # get particular 6,7 from BO 603R
        df_local_bills_foreign_currency = df_603r.loc[df_603r['Code'].isin([br_code]) & ~df_603r['CUR'].isin(['BDT'])]
        local_bills_foreign_currency = df_local_bills_foreign_currency['Bill Outstanding LCY'].sum()
        # get particular 8 from GL
        local_bills_collection = df_dic[br_code].loc[df_dic[br_code]['GL Code'].isin(local_bills_gl), 'Total'].sum()
        # create dataframe with respective data and set to dictionary
        df_dic[br_code] = pd.DataFrame(
            {
            'Particulars': particulars,
            br_code: [local_bills_outstanding, ldbp_outstanding, ldbp_outstanding, matured_acceptance, overdue_bills,
                        local_bills_foreign_currency, local_bills_foreign_currency, local_bills_collection]
            }
        )
        # add branchwise data to excel file as separate sheets
        if local_bills_outstanding != 0:
            with pd.ExcelWriter('iss_export_bill/work_files/603R.xlsx', engine='openpyxl', mode='a') as writer:
                df_local_bills.to_excel(writer, sheet_name=br_code, float_format='%.2f', index=False)
        if matured_acceptance != 0:
            with pd.ExcelWriter('iss_export_bill/work_files/matured.xlsx', engine='openpyxl', mode='a') as writer:
                df_matured_bills.to_excel(writer, sheet_name=br_code, float_format='%.2f', index=False)
        if overdue_bills != 0:
            with pd.ExcelWriter('iss_export_bill/work_files/625A.xlsx', engine='openpyxl', mode='a') as writer:
                df_overdue_bills.to_excel(writer, sheet_name=br_code, float_format='%.2f', index=False)
        if local_bills_foreign_currency != 0:
            with pd.ExcelWriter('iss_export_bill/work_files/603F.xlsx', engine='openpyxl') as writer:
                df_local_bills_foreign_currency.to_excel(writer, sheet_name=br_code, float_format='%.2f', index=False)

    # combine all branch data horizontally, drop columns duplicated during merge, add a total column
    df_all = pd.concat([df_dic[br_code] for br_code in br_codes], axis=1)
    # add blank data columns for excluded branches
    for br_code in exclude_br:
        df_all[br_code] = pd.NA
    # get final dataframe with data cleaned and sorted
    df_final = df_all.loc[:, ~df_all.columns.duplicated()]
    key_col = df_final.pop(df_final.columns[0])
    df_final = df_final[sorted(df_final.columns)]
    df_final.insert(0, key_col.name, key_col)
    df_final.insert(len(df_final.columns), 'Main Operation', df_final.sum(axis=1, numeric_only=True))
    # export final output result in excel
    with pd.ExcelWriter(f'iss_export_bill/ISS_Export-Local_{report_period}.xlsx', engine='openpyxl') as writer:
        df_final.to_excel(writer, float_format='%.2f', index=False)
        # beautify output
        sheet = writer.sheets['Sheet1']
        auto_column_width(sheet, df_final)
        sheet.column_dimensions['A'].width = 55
        col_list = [chr(i) for i in range(ord('B'), ord('Z'))]
        for col in col_list:
            for cell in sheet[col]:
                cell.number_format = '#,##0.00'

def main(functions, br_codes, exclude_br=[], selection=1):
    # create directories if not exist
    if not os.path.exists('BAL_SHEET'):
        os.makedirs('BAL_SHEET')
    if not os.path.exists('RAW_BO'):
        os.makedirs('RAW_BO')
    # generate reports for different parts of ISS with threading to show loader and completion
    loading_message = "Processing: "
    loading_symbols = [
        '|▷▷▷▷▷▷▷▷|', '/▶▷▷▷▷▷▷▷|', '-▶▶▷▷▷▷▷▷|', '\\▶▶▶▷▷▷▷▷|', '|▶▶▶▶▷▷▷▷|', '/▶▶▶▶▶▷▷▷|', '-▶▶▶▶▶▶▷▷|', '\\▶▶▶▶▶▶▶▷|',
        '|▶▶▶▶▶▶▶▶|', '|▶▶▶▶▶▶▶▷\\', '|▶▶▶▶▶▶▷▷-', '|▶▶▶▶▶▷▷▷/', '|▶▶▶▶▷▷▷▷|', '|▶▶▶▷▷▷▷▷\\', '|▶▶▷▷▷▷▷▷-', '|▶▷▷▷▷▷▷▷/',
    ]
    done = Event()
    loader = Thread(target=loading, args=[done, loading_message, loading_symbols])
    loader.start()
    with ThreadPoolExecutor(2) as executor:
        # function to show task completion
        def tasks_completed(future):
            global reports, report_generated, progress, selection
            with Lock():
                try:
                    result = future.result()
                    selection += 1
                    report_generated += 1
                    progress = (report_generated/reports) * 100
                    print(f"{report_options[selection]} report generated.")
                    print(f"{report_generated}/{reports} reports completed - {round(progress)}%", flush=True)
                except Exception as e:
                    print(f"!ERROR! {e}")
        global reports, report_generated, progress
        futures = []
        for f in functions:
            futures.append(executor.submit(f, br_codes, exclude_br))
        reports = len(futures)
        report_generated = 0
        for future in futures:
            future.add_done_callback(tasks_completed)
    done.set() # loader's ending condition
    loader.join() # wait for loader to finish

if __name__ == '__main__':
    # give users option to exclude any branches
    br_codes=['001', '091', '101', '102', '103', '104', '105', '106', '110', '116', '195', '200', '301', '331', '999']
    exclude_br = []
    if user_input("Do you want to exclude any branch?"):
        input_list = input("Branch codes seperated with comma: ").replace(" ", "")
        exclude_br = input_list.split(',')
        br_codes = [br_code for br_code in br_codes if br_code not in exclude_br]
    # give users option to select report catagory
    functions =[iss_import_loan, iss_import_bill, iss_export_bill]
    selection = 0
    if user_input("Do you want to generate only a part of the report?"):
        for key, value in report_options.items():
            print(f"{key}){value}", end="  ")
        selection = int(input("\nChoose a report catagory: ")) - 1
        functions = [f for i, f in enumerate(functions) if i == selection]
    # set expiry date for trial run of the app
    expiry_date = datetime.strptime('2023-12-31', '%Y-%m-%d')
    if datetime.today() < expiry_date:
        # run main function
        main(functions, br_codes, exclude_br, selection)
    else:
        # show expried message
        print(f"!TRIAL PERIOD EXPIRED! Please contact developer for renewal. @phenomroman")
    sleep(2)