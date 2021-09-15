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
  Load the output to a dataframe
  Rename columns and keep the only ones that we want
  Write individual files to Excel if parameter set

Change Log
----------
2021-08-31  1.0 First working version, slow but catches most errors
2021-09-02  1.1 only writes the output once, faster

"""
from datetime import datetime
import re
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup

INVESTMENT_PRODUCTS_OUTPUT = "ETF Securities List.xlsx"
INVESTMENT_PRODUCTS_OUTPUT = "Investment Products.xlsx"
BASE_URL = 'https://www.etfsecurities.com.au'
PRODUCT_URL = 'https://www.etfsecurities.com.au/product'
WORKING_DIR = 'ETFS'
ALL_ETF_SECURITIES_FILE = f'{WORKING_DIR}\\ALL_ETF_Securities.xlsx'


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


def get_all_products(base_url, product_url):
    try:
        response = requests.get(product_url, timeout=t_out)  # pull the containing page
    except  Exception as e:
        # writelog(f'{fund}\tCould not get the containing page {link}: {e}')
        return None

    soup = BeautifulSoup(response.content, 'lxml')  # parse

    # gdp = soup.find_all("table", attrs={"class": "wikitable"})
    gdp = soup.find_all("table")
    table = gdp[0]
    # the head will form our column names
    body = table.find_all("tr")
    # Head values (Column names) are the first items of the body list
    head = body[0]  # 0th item is the header row
    body_rows = body[1:]  # All other items becomes the rest of the rows
    # print(body_rows)
    # print(head)

    # Lets now iterate through the head HTML code and make list of clean headings

    # Declare empty list to keep Columns names
    headings = []
    for item in head.find_all("th"):  # loop through all th elements
        # convert the th elements to text and strip "\n"
        item = (item.text).rstrip("\n")
        # append the clean column name to headings
        headings.append(item)

    # Next is now to loop though the rest of the rows

    # print(body_rows[0])
    all_rows = []  # will be a list for list for all rows
    for row_num in range(len(body_rows)):  # A row at a time
        each_row = []  # this will old entries for one row
        for row_item in body_rows[row_num].find_all("td"):  # loop through all row entries
            # row_item.text removes the tags from the entries
            # the following regex is to remove \xa0 and \n and comma from row_item.text
            # xa0 encodes the flag, \n is the newline and comma separates thousands in numbers
            single_data = re.sub("(\xa0)|(\n)|,", "", row_item.text)

            # append aa to row - note one row entry is being appended
            if row_item.find('a'):
                link = row_item.find('a').get('href')
                single_data = base_url + link

            each_row.append(single_data)
        # append one row to all_rows
        all_rows.append(each_row)

    df = pd.DataFrame(data=all_rows, columns=headings)
    df["Link"] = product_url + "/" + df['Code']
    df["Link"] = df["Link"].str.lower()

    remove_cols = [x for x in headings if
                   "Sort:" in x or x == "" or x == None]  # list columns that contains Sort: in name or None or ""
    df = df.drop(columns=remove_cols)  # remove the Sort: columns
    df.to_excel(INVESTMENT_PRODUCTS_OUTPUT, sheet_name='Investment_Products', index=False, freeze_panes=(1, 0))


def etf_get_holdings(fund, link):
    """ ETF Securities """
    # ---------- constants ----------
    col_rename = {'Component Name': 'Security Name',
                  'Weight': 'Weight %',
                  'Market Value (Base CCY)': 'Market Value',
                  # 'Bloomberg Ticker':'Security Ticker'
                  }
    base_url = 'https://www.etfsecurities.com.au'
    try:
        r = requests.get(link, timeout=t_out)  # pull the containing page
    except  Exception as e:
        writelog(f'{fund}\tCould not get the containing page {link}: {e}')
        return pd.DataFrame()
    soup = BeautifulSoup(r.content, 'lxml')  # parse
    tag = soup.find(href=re.compile('\.xlsx'))  # find the tag with the link
    if not tag:
        writelog(f'{fund}\tCould not find the spreadsheet link in\t{link}')
        return pd.DataFrame()

    file_url = base_url + tag['href']  # add the relative link
    try:
        r = requests.get(file_url, timeout=t_out)  # get the file
    except  Exception as e:
        writelog(f'{fund}\tCould not get the spreadsheet: {e}')
        return pd.DataFrame()
    df = pd.read_excel(r.content, skiprows=18)
    df = df.dropna(thresh=5)  # to drop the total row and others mostly null

    # Splitting Bloomberg Ticker column into "Security Ticker", "Country Code" columns
    if "Bloomberg Ticker" in df.columns:
        df[['Security Ticker', 'Country Code', 'Security Type']] = df['Bloomberg Ticker'].str.split(' ', 0, expand=True)

    # if we find any source columns in the rename dict, rename them
    for col in col_rename:
        if col in list(df):
            df.rename(columns={col: col_rename[col]}, inplace=True)

    if save_individual_files:
        save_file = f'{wkg_dir}\\{start.strftime("%Y%m%d")}_{fund}.xlsx'
        df.to_excel(save_file, sheet_name=fund, index=False, freeze_panes=(1, 0))

    df['etf ticker'] = fund
    # keep = keep_list(COL_DISPLAY, list(df))
    # return df[keep]
    return df


# ---------- set up constants ---------- #
create_dir(WORKING_DIR)

start = datetime.now()
start_day = start.strftime("%Y%m%d")
save_individual_files = False
t_out = 30  # timeout for all web requests, else may hang
log_dir = WORKING_DIR
logfile = f'{start_day}_{os.path.basename(__file__).split(".")[0]}.log'
lf = open(f'{log_dir}\\{logfile}', 'a')
lf.write('\n' + '-' * 75)
# After renaming columns, keep only these ones in the final file
# Note that this also determines the column order in the Excel file.
COL_DISPLAY = ['Issuer', 'etf ticker', 'Security Ticker', 'Country Code', 'Security Name', 'Weight %', 'Market Value',
               'Rate', 'Maturity date', 'Sector', 'Country']
all_funds = pd.DataFrame()  # empty output frame to add to

# load all latest products
get_all_products(BASE_URL, PRODUCT_URL)

fund_list = pd.read_excel(INVESTMENT_PRODUCTS_OUTPUT, engine="openpyxl", )
fund_list.fillna('', inplace=True)  # convert NA to empty string (from float)

# ---------- Main Loop ---------- #

for i in range(len(fund_list)):
    # take care - headings are case sensitive
    # fund = fund_list.loc[i,'ASX Code']
    fund = fund_list.loc[i, 'Code']
    # etf_cat = fund_list.loc[i,'ETF Category']
    link = fund_list.loc[i, 'Link']
    # issuer = fund_list.loc[i,'Issuer']
    issuer = fund_list.loc[i, 'Product Name']
    writelog(f'{fund}\t{issuer}\tStarting...')
    if len(link) > 4:  # skip NA
        if issuer.lower().startswith('etf'):
            holdings = etf_get_holdings(fund, link)
        else:
            writelog(f'{fund}\t{issuer}\tDid not recognise this issuer')

        # if the function returned an empty dataframe, skip
        if len(holdings) == 0:
            writelog(f'{fund}\t{issuer}\tFailed to get holdings')
        else:
            # writelog(f'{fund}\t{issuer}\tAdding {len(holdings)} rows')
            # holdings['ETF Category'] = fund_list.loc[i,'ETF Category']
            # holdings['Issuer'] = fund_list.loc[i,'Issuer']
            holdings['Product Name'] = fund_list.loc[i, 'Product Name']
            all_funds = all_funds.append(holdings)
            # all_funds.to_excel(ALL_ETF_SECURITIES_FILE, sheet_name='ETF', index=False, freeze_panes=(1,0))
            # write out the combined DF every time in case of crash
    else:
        writelog(f'{fund}\t{issuer}\tSKIPPING, not a valid link')

# Saving all at the end - riskier
all_funds.to_excel(ALL_ETF_SECURITIES_FILE, sheet_name='ETFS', index=False, freeze_panes=(1, 0))
writelog(f'Saved the combined file {ALL_ETF_SECURITIES_FILE} size {all_funds.shape}')

# ------ Print the time taken and Exit ----------
end = datetime.now()
time_taken = end - start
# writelog(f'Finished at {end.strftime("%H:%M:%S")}')
m, s = divmod(time_taken.seconds, 60)
writelog(f'This took {m} minutes, {s} seconds for {len(fund_list)} funds')