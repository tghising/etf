#!/usr/bin/env python
# coding: utf-8
"""
Read the ETF Fund List file and loop through the rows
Determine the issuer to call the appropriate function
Functions return the fund's holding details
Add the holding details to the main table.
Save the complete table


For each fund:
  Get only the json from Vanguard directly.
  or find the Excel, CSV, JSON or HTML table
  Load the data to a dataframe
  Rename columns and keep the only ones that we want
  Write individual files to Excel if parameter set

Change Log
----------
2021-08-31  1.0 First working version, slow but catches most errors
2021-09-02  1.1 only writes the output once, faster

"""

from datetime import datetime
from io import StringIO
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup

# CONSTANTS Configurations
INVESTMENT_PRODUCTS_LIST = 'iShares List.xlsx'
BASE_URL = 'https://www.blackrock.com/'
OUTPUT_DIR = r'iShares'
LOGS_DIR = r'iShares'

TIME_OUT = 30  # timeout for all web requests, else may hang

OUTPUT_I_SHARES_FILE = f'{OUTPUT_DIR}\\BlackRock - iShares.xlsx'

# After renaming columns, keep only these ones in the final file
# Note that this also determines the column order in the Excel file.
COLUMN_TO_DISPLAY = ['Issuer', 'etf ticker', 'Security Ticker', 'Security Name', 'Weight %', 'Market Value',
                     'Rate', 'Maturity date', 'Country']

COLUMN_RE_MAPPING = {'Name': 'Security Name',
                     'Weight (%)': 'Weight %',
                     'Ticker': 'Security Ticker',
                     'Location': 'Country'
                     # ,'Sector':'Sector'
                     }


def create_dir(dirName):
    """ Create if it doesn't exist """
    import os
    try:
        os.makedirs(dirName)  # Create target Directory
    except FileExistsError:
        pass


def writelog(msg):
    """Writes the given string to the log file and prints it"""
    lf.write(f'{datetime.now().strftime("%H:%M:%S")}\t{msg}\n')
    print(msg)
    return


def keep_list(k, all):
    """ Given a list of columns to keep and  
    a list of columns in the df, 
    Return a list of the overlap in the order of the keep list """
    intersection = []
    for c in k:
        if c in all:
            intersection.append(c)
    return intersection


def is_get_holdings(fund, link):
    """ BlackRock iShares """
    try:
        res = requests.get(link, timeout=TIME_OUT)  # pull the containing page
    except  Exception as e:
        writelog(f'{fund}\tCould not get the containing page {link}: {e}')
        return pd.DataFrame()
    soup = BeautifulSoup(res.content, 'lxml')  # parse
    tag = soup.find('a', string='Download Holdings')  # find the tag with the link text
    if not tag:
        writelog(f'{fund}\tCould not find the spreadsheet link in\t{link}')
        return pd.DataFrame()

    file_url = BASE_URL + tag['href']  # add the relative link
    try:
        result = requests.get(file_url, timeout=TIME_OUT)  # get the file
    except  Exception as e:
        writelog(f'{fund}\tCould not get the spreadsheet {file_url}: {e}')
        return pd.DataFrame()

    # set skiprows 9 for all the holding
    first_skip_rows = 9

    df = pd.read_csv(StringIO(result.text), skiprows=first_skip_rows)
    df = df.dropna(thresh=5)  # to drop the total row and others mostly null

    ticker_header_list = df.index[df['Ticker'] == 'Ticker'].tolist()  # df.loc[df['Ticker'].isin(['Ticker'])]
    if ticker_header_list:
        # exists number of header if there exists multiple Ticker columns in the holding is one (1)
        exists_header_num = 1  # this header number must be added into new skiprows
        index = ticker_header_list[len(ticker_header_list) - 1]  # take Header from the last index
        modified_skip_rows = first_skip_rows + index + exists_header_num  # add the number of header count to new skip rows
        df = pd.read_csv(StringIO(result.text), skiprows=modified_skip_rows)
        df = df.dropna(thresh=5)  # to drop the total row and others mostly null

    # if we find any source columns in the rename dict, rename them
    for col in COLUMN_RE_MAPPING:
        if col in list(df):
            df.rename(columns={col: COLUMN_RE_MAPPING[col]}, inplace=True)

    if save_individual_files:
        save_file = f'{OUTPUT_DIR}\\{start.strftime("%Y%m%d")}_{fund}.xlsx'
        df.to_excel(save_file, sheet_name=fund, index=False, freeze_panes=(1, 0))
        # df['Issuer'] = 'BlackRock iShares'
    df['etf ticker'] = fund
    keep = keep_list(COLUMN_TO_DISPLAY, list(df))
    return df[keep]


# ============================
#      START SCRIPT FROM HERE
# ============================
print("======================================================================")
print("           BlackRock - iShares - Holdings EXTRACT: STARTED            ")
print("======================================================================")

create_dir(OUTPUT_DIR)
create_dir(LOGS_DIR)

start = datetime.now()
start_day = start.strftime("%Y%m%d")
save_individual_files = False

logfile = f'{start_day}_{os.path.basename(__file__).split(".")[0]}.log'
lf = open(f'{LOGS_DIR}\\{logfile}', 'a')
lf.write('\n' + '-' * 75 + '\n')

all_funds = pd.DataFrame()  # empty data frame to add to

fund_list = pd.read_excel(INVESTMENT_PRODUCTS_LIST, engine="openpyxl", )
fund_list.fillna('', inplace=True)  # convert NA to empty string (from float)

# ---------- Main Loop ---------- #

for i in range(len(fund_list)):
    # take care - headings are case sensitive
    fund = fund_list.loc[i, 'ASX Code']
    # etf_cat = fund_list.loc[i,'ETF Category']
    link = fund_list.loc[i, 'Link']
    issuer = fund_list.loc[i, 'Issuer']
    writelog(f'{fund}\t{issuer}\tStarting...')
    if len(link) > 4:  # skip NA
        if issuer.lower().startswith('ishares'):
            holdings = is_get_holdings(fund, link)

        else:
            writelog(f'{fund}\t{issuer}\tDid not recognise this issuer')

        # if the function returned an empty dataframe, skip
        if len(holdings) == 0:
            writelog(f'{fund}\t{issuer}\tFailed to get holdings')
        else:
            # writelog(f'{fund}\t{issuer}\tAdding {len(holdings)} rows')
            holdings['ETF Category'] = fund_list.loc[i, 'ETF Category']
            holdings['Issuer'] = fund_list.loc[i, 'Issuer']
            all_funds = all_funds.append(holdings)
            # all_funds.to_excel(OUTPUT_I_SHARES_FILE, sheet_name='ETF', index=False, freeze_panes=(1,0))
            # write out the combined DF every time in case of crash
    else:
        writelog(f'{fund}\t{issuer}\tSKIPPING, not a valid link')

# Saving all at the end - riskier
all_funds.to_excel(OUTPUT_I_SHARES_FILE, sheet_name='ETF', index=False, freeze_panes=(1, 0))
print('\n')
writelog(f'Saved the combined file {OUTPUT_I_SHARES_FILE} size {all_funds.shape}')

# ------ Print the time taken and Exit ----------
end = datetime.now()
time_taken = end - start
# writelog(f'Finished at {end.strftime("%H:%M:%S")}')
m, s = divmod(time_taken.seconds, 60)
writelog(f'This took {m} minutes, {s} seconds for {len(fund_list)} funds')

print("\n***********************************************************************")
print("                  BlackRock - iShares - Holdings EXTRACT : COMPLETED     ")
print("***********************************************************************")
