from datetime import datetime
import os
import requests
import pandas as pd

# CONSTANTS Configurations
INVESTMENT_PRODUCTS_LIST = r'D:\Internship\etf\scripts\ASX Price List.xlsx'
OUTPUT_DIR = r'D:\Internship\etf\output\asx-price'
LOGS_DIR = r'D:\Internship\etf\logs\asx-price'

TIME_OUT = 30  # timeout for all web requests, else may hang

OUTPUT_FUNDS_FILE = f'{OUTPUT_DIR}\\ASX_Shares_price.xlsx'
SAVE_INDIVIDUAL_FILE = False

# Note that this also determines the column order in the Excel file.
COLUMN_TO_DISPLAY = ['etf ticker', 'close date', 'close price', 'change price', 'volume',
                     'day high price', 'day low price', 'change in percent']

COLUMN_RE_MAPPING = {
    'close_date': 'close date',
    'close_price': 'close price',
    'change_price': 'change price',
    'day_high_price': 'day high price',
    'day_low_price': 'day low price',
    'change_in_percent': 'change in percent',
}


def create_dir(dirName):
    try:
        os.makedirs(dirName)  # Create target Directory
    except FileExistsError:
        pass


def welcome_log(msg):
    lf.write(f'\t{msg}\n')
    print(msg)
    return


def generate_log(msg):
    """Writes the given string to the log file and prints it"""
    lf.write(f'{datetime.now().strftime("%H:%M:%S")}\t{msg}\n')
    print(msg)
    return


def keep_list(k, all):
    intersection = []
    for c in k:
        if c in all:
            intersection.append(c)
    return intersection


def get_shares_price(fund, link):
    # link = f'https://www.asx.com.au/asx/1/share/{fund.upper()}/prices?interval=daily&count=20'
    try:
        res = requests.get(link, timeout=TIME_OUT)  # get the file
    except  Exception as e:
        generate_log(f'{fund}\tCould not get the spreadsheet {link}: {e}')
        return pd.DataFrame()

    # Flatten data
    df = pd.json_normalize(res.json(), record_path=['data'])

    if "code" in df.columns:
        df['etf ticker'] = df['code']

    # if we find any source columns in the rename dict, rename them
    for col in COLUMN_RE_MAPPING:
        if col in list(df):
            df.rename(columns={col: COLUMN_RE_MAPPING[col]}, inplace=True)
    if SAVE_INDIVIDUAL_FILE:
        save_file = f'{OUTPUT_DIR}\\{start.strftime("%Y%m%d")}_{fund}.xlsx'
        df.to_excel(save_file, sheet_name=fund, index=False, freeze_panes=(1, 0))

    keep = keep_list(COLUMN_TO_DISPLAY, list(df))
    return df[keep]


# ============================
#      START SCRIPT FROM HERE
# ============================
create_dir(OUTPUT_DIR)
create_dir(LOGS_DIR)
start = datetime.now()

start_day = start.strftime("%Y-%m-%d")
logfile = f'{start_day}_ASX-Shares_price.log'
lf = open(f'{LOGS_DIR}\\{logfile}', 'a')

welcome_log(f'\t\t==================================================================================')
welcome_log(f'\t\t\t\t\t\t\t\t\tASX Shares Price : STARTED')
welcome_log(f'\t\t==================================================================================')

all_funds = pd.DataFrame()  # empty data frame to add to

fund_list = pd.read_excel(INVESTMENT_PRODUCTS_LIST, engine="openpyxl", )
fund_list.fillna('', inplace=True)  # convert NA to empty string (from float)

# ---------- Main Loop ---------- #

for i in range(len(fund_list)):
    # take care - headings are case sensitive
    fund = fund_list.loc[i, 'ASX Code']
    # etf_cat = fund_list.loc[i,'ETF Category']
    link = fund_list.loc[i, 'Link']
    generate_log(f'{fund}\tStarting...')
    if len(link) > 4:  # skip NA
        holdings = get_shares_price(fund, link)
        # if the function returned an empty dataframe, skip
        if len(holdings) == 0:
            generate_log(f'{fund}\tFailed to get holdings')
        else:
            all_funds = all_funds.append(holdings)
    else:
        generate_log(f'{fund}\tSKIPPING, not a valid link')

# Saving all at the end - riskier
all_funds.to_excel(OUTPUT_FUNDS_FILE, sheet_name='ASX Shares Price', index=False, freeze_panes=(1, 0))
generate_log(f'Saved the combined file {OUTPUT_FUNDS_FILE} size {all_funds.shape}')

# ------ Print the time taken and Exit ----------
end = datetime.now()
time_taken = end - start
minute, sec = divmod(time_taken.seconds, 60)

generate_log(f'')
generate_log(f'Application took {minute} minutes, {sec} seconds for execution.')
generate_log(f'***********************************************************************')
generate_log(f'\t\t\tASX Shares Price : COMPLETED')
generate_log(f'***********************************************************************\n')

lf.close()
print("Log has been generated at: " + LOGS_DIR + "\\" + logfile)
