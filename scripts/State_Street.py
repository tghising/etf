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
import os
import requests
import pandas as pd

# CONSTANTS Configurations
INVESTMENT_PRODUCTS_LIST = 'State Street List.xlsx'
OUTPUT_DIR = r'state-street'
LOGS_DIR = r'state-street'

TIME_OUT = 30  # timeout for all web requests, else may hang

ALL_STATE_STREET_SECURITIES_FILE = f'{OUTPUT_DIR}\\All ETF Securities.xlsx'

# Note that this also determines the column order in the Excel file.
COLUMN_TO_DISPLAY = ['Issuer', 'etf ticker', 'Security Ticker', 'Country Code', 'Security Name', 'Weight %',
                     'Market Value', 'Rate', 'Maturity date', 'Sector', 'Country', 'Number of Shares', 'Local Price']

COLUMN_RE_MAPPING = {'Name': 'Security Name',
                     'Weight (%)': 'Weight %'
                     # 'Ticker': 'Security Ticker'
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


def ss_get_holdings(fund, link):
    """ State Street v2.0
    not using the link at all now, just the fund """

    file_url = f'https://www.ssga.com/au/en_gb/individual/etfs/library-content/products/fund-data/etfs/apac/holdings-daily-au-en-{fund.lower()}.xlsx'
    try:
        res = requests.get(file_url, timeout=TIME_OUT)  # get the file
    except  Exception as e:
        writelog(f'{fund}\tCould not get the spreadsheet {file_url}: {e}')
        return pd.DataFrame()

    df = pd.read_excel(res.content, skiprows=4)
    df = df.dropna(thresh=5)  # to drop the total row and others mostly null

    # Splitting Bloomberg Ticker column into "Security Ticker", "Country Code" columns
    if "Ticker" in df.columns:
        is_contains_space = df['Ticker'].str.contains("-").any()
        if is_contains_space:
            df[['Security Ticker', 'Country Code']] = df['Ticker'].str.split('-', 0, expand=True)
        else:
            df['Security Ticker'] = df['Ticker']
            df['Country Code'] = None

    # if we find any source columns in the rename dict, rename them
    for col in COLUMN_RE_MAPPING:
        if col in list(df):
            df.rename(columns={col: COLUMN_RE_MAPPING[col]}, inplace=True)
    if save_individual_files:
        save_file = f'{OUTPUT_DIR}\\{start.strftime("%Y%m%d")}_{fund}.xlsx'
        df.to_excel(save_file, sheet_name=fund, index=False, freeze_panes=(1, 0))

    df['Issuer'] = 'State Street'
    df['etf ticker'] = fund
    keep = keep_list(COLUMN_TO_DISPLAY, list(df))
    return df[keep]


# ============================
#      START SCRIPT FROM HERE
# ============================
print("======================================================================")
print("                 State Street - Holdings EXTRACT: STARTED             ")
print("======================================================================")

create_dir(OUTPUT_DIR)
create_dir(LOGS_DIR)
start = datetime.now()
start_day = start.strftime("%Y%m%d")
save_individual_files = False

logfile = f'{start_day}_{os.path.basename(__file__).split(".")[0]}.log'
lf = open(f'{LOGS_DIR}\\{logfile}', 'a')
lf.write('\n' + '-' * 75 + '\n')

all_funds = pd.DataFrame()  # empty output frame to add to

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
            # all_funds.to_excel(ALL_STATE_STREET_SECURITIES_FILE, sheet_name='ETF', index=False, freeze_panes=(1,0))
            # write out the combined DF every time in case of crash
    else:
        writelog(f'{fund}\t{issuer}\tSKIPPING, not a valid link')

# Saving all at the end - riskier
all_funds.to_excel(ALL_STATE_STREET_SECURITIES_FILE, sheet_name='ETF', index=False, freeze_panes=(1, 0))
print("\n")
writelog(f'Saved the combined file {ALL_STATE_STREET_SECURITIES_FILE} size {all_funds.shape}')

# ------ Print the time taken and Exit ----------
end = datetime.now()
time_taken = end - start
# writelog(f'Finished at {end.strftime("%H:%M:%S")}')
m, s = divmod(time_taken.seconds, 60)
writelog(f'This took {m} minutes, {s} seconds for {len(fund_list)} funds')

print("\n***********************************************************************")
print("                  State Street - Holdings EXTRACT : COMPLETED            ")
print("***********************************************************************")