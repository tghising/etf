#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup


# In[2]:


def create_dir(dirName):
    """ Create if it doesn't exist """
    import os
    try:
        os.mkdir(dirName)  # Create target Directory
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


def ss_get_holdings(fund, link):
    """ State Street v2.0
    not using the link at all now, just the fund """
    col_rename = {'Name': 'Security Name',
                  'Weight (%)': 'Weight %',
                  'Ticker': 'Security Ticker'
                  }
    file_url = f'https://www.ssga.com/au/en_gb/individual/etfs/library-content/products/fund-data/etfs/apac/holdings-daily-au-en-{fund.lower()}.xlsx'
    try:
        r = requests.get(file_url, timeout=t_out)  # get the file
    except  Exception as e:
        writelog(f'{fund}\tCould not get the spreadsheet {file_url}: {e}')
        return pd.DataFrame()
    df = pd.read_excel(r.content, skiprows=4)
    df = df.dropna(thresh=5)  # to drop the total row and others mostly null
    # if we find any source columns in the rename dict, rename them
    for col in col_rename:
        if col in list(df):
            df.rename(columns={col: col_rename[col]}, inplace=True)
    if save_individual_files:
        save_file = f'{wkg_dir}\\{start.strftime("%Y%m%d")}_{fund}.xlsx'
        df.to_excel(save_file, sheet_name=fund, index=False, freeze_panes=(1, 0))
    df['Issuer'] = 'State Street'
    df['etf ticker'] = fund
    keep = keep_list(col_keep, list(df))
    return df[keep]


# ---------- set up constants ---------- #
wkg_dir = 'State Street Py'
create_dir(wkg_dir)
host = 'https://www3.vanguard.com.au'
path = '/personal/products/funds.json'
url = host + path
start = datetime.now()
start_day = start.strftime("%Y%m%d")
save_individual_files = False
t_out = 30  # timeout for all web requests, else may hang
log_dir = wkg_dir
logfile = f'{start_day}_{os.path.basename(__file__).split(".")[0]}.log'
lf = open(f'{log_dir}\\{logfile}', 'a')
lf.write('\n' + '-' * 75)
# After renaming columns, keep only these ones in the final file
# Note that this also determines the column order in the Excel file.
col_keep = ['Issuer', 'etf ticker', 'Security Ticker', 'Security Name', 'Weight %', 'Market Value',
            'Rate', 'Maturity date', 'Sector', 'Country', 'Number of Shares', 'Local Price']
all_funds = pd.DataFrame()  # empty data frame to add to
list_file = 'fund list test.xlsx'
list_file = 'State Street List.xlsx'
total_file = f'{wkg_dir}\\State_Street.xlsx'
fund_list = pd.read_excel(list_file, engine="openpyxl", )
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
        if issuer.lower().startswith('state street'):
            holdings = ss_get_holdings(fund, link)

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
            # all_funds.to_excel(total_file, sheet_name='ETF', index=False, freeze_panes=(1,0))
            # write out the combined DF every time in case of crash
    else:
        writelog(f'{fund}\t{issuer}\tSKIPPING, not a valid link')

# Saving all at the end - riskier
all_funds.to_excel(total_file, sheet_name='ETF', index=False, freeze_panes=(1, 0))
writelog(f'Saved the combined file {total_file} size {all_funds.shape}')

# ------ Print the time taken and Exit ----------
end = datetime.now()
time_taken = end - start
# writelog(f'Finished at {end.strftime("%H:%M:%S")}')
m, s = divmod(time_taken.seconds, 60)
writelog(f'This took {m} minutes, {s} seconds for {len(fund_list)} funds')