from datetime import datetime
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup

# CONSTANTS Configurations
BASE_URL = 'https://www2.asx.com.au'
FUND_LIST_URL = "https://www2.asx.com.au/issuers/investment-products/asx-funds-statistics"
INVESTMENT_PRODUCTS_LIST = 'Russell List.xlsx'
INPUT_TEMPLATE_DIR = r'D:\Internship\etf\templates\asx\ASX List.xlsx'
OUTPUT_DIR = r'D:\Internship\etf\output\asx'
LOGS_DIR = r'D:\Internship\etf\logs\asx'

# FILTER_YEAR : the below configuration for Year based data
# i.e. ['ALL'] for all year available at FUND_LIST_URL url or
#  ['2021'] for year 2021 only or ['2021', '2020'] for year 2021 and 2020
FILTER_YEAR = ['ALL']
# FILTER_YEAR = ['2021', '2020','2018','2017']
# FILTER_YEAR = ['2021','2019']
# FILTER_YEAR = ['2017']

SHEET_TO_USE = ['ETP', 'LIC', 'REIT', 'MFSA', 'MFUND', 'INFRA']

TIME_OUT = 30  # timeout for all web requests, else may hang

OUTPUT_FUNDS_FILE = f'{OUTPUT_DIR}\\ASX Investment Products'
SAVE_INDIVIDUAL_FILE = False


def create_dir(dirName):
    try:
        os.makedirs(dirName)  # Create target Directory
    except FileExistsError:
        pass


def generate_log(msg):
    lf.write(f'{datetime.now().strftime("%H:%M:%S")}\t{msg}\n')
    print(msg)
    return


def welcome_log(msg):
    lf.write(f'\t{msg}\n')
    print(msg)
    return


def get_all_fund_list(url):
    try:
        res = requests.get(url, timeout=TIME_OUT)  # pull the containing page
    except  Exception as e:
        generate_log(f'Could not get the containing page {url}: {e}')
        return None

    soup = BeautifulSoup(res.content, 'lxml')
    soup_tabs_component = soup.find("div", attrs={"class": "tabs-component"})
    all_year_funds = soup_tabs_component.find_all("div", attrs={"class": "cmp-tabs__tabpanel"})
    all_data = []
    for each_year in all_year_funds:
        all_funds = each_year.find(id="multi-column-1").find('table').find_all('tr')
        yearly_funds = [fund.find('a') for fund in all_funds]
        for monthly_fund in yearly_funds:
            monthly_data = {}
            description = monthly_fund.get_text()
            fund_month_year = description.split("-")[-1]
            fund_year = fund_month_year.split()[-1]
            file_url = BASE_URL + monthly_fund['href']
            period_date = datetime.strptime(fund_month_year.strip(), "%B %Y")
            period_date = period_date.strftime('%d/%m/%Y')

            monthly_data['Period'] = period_date
            monthly_data['Description'] = description
            monthly_data['Exchange'] = description.split(" ")[0]
            monthly_data['Link'] = file_url
            monthly_data['Year'] = fund_year

            all_data.append(monthly_data)

    return all_data


def get_monthly_products(monthly_data):
    link = monthly_data['Link']
    fund = monthly_data['Description']
    period = monthly_data['Period']
    try:
        res = requests.get(link, timeout=TIME_OUT)  # get the file
    except  Exception as e:
        generate_log(f'{fund}\tCould not get the spreadsheet {link}: {e}')
        return pd.DataFrame()

    xl = pd.ExcelFile(res.content)
    sheets = xl.sheet_names

    processed_data = []
    # processed_data.append(sheets)
    generate_log(f'\t\t Sheets : {sheets}')

    for sheet_name in sheets:
        each_data = {}
        df = pd.DataFrame()
        exists_sheet = [col for col in SHEET_TO_USE if col in sheet_name.upper()]
        if exists_sheet:
            generate_log(f'\t\t\t {sheet_name}\treading ...')

            # read excel data of given sheet "acquired_sheet" which contains etp
            df = pd.read_excel(res.content, sheet_name=sheet_name)
            df = df.dropna(thresh=5)  # to drop the total row and others mostly null
            df.dropna(how='all', axis=1, inplace=True)
            df.columns = df.iloc[0]  # set row index 0 as column
            df.columns = df.columns.str.replace('\n', '')  # replace newline '\n' with "" from the column name

            if "ETP" in sheet_name.upper():
                asx_code_header_list = df.index[
                    df.columns[0] == 'ASX Code'].tolist()  # Check first column contains "ASX Code or Not
                if not asx_code_header_list:
                    df.columns = df.iloc[1]  # # column take from row index 1
                    df.columns = df.columns.str.replace('\n', '')  # replace newline '\n' with "" from the column name
                    df = df[2:]  # data take from row index 2
                else:
                    df = df[1:]  # data take from row index 1
            else:
                # data take from row index 1
                df = df[1:]

                if "LIC" in sheet_name.upper():
                    # rename the column "Prem/Disc % NTA (pre-tax) at NTA Date" into "Prem/Disc % NTA (pre-tax)"
                    df.columns = ['Prem/Disc % NTA (pre-tax)' if "Prem/Disc % NTA (pre-tax)" in col else col for col in df.columns]
                elif "MFUND" in sheet_name.upper():
                    # rename the column "FUM" into "FUM ($m)#"
                    df.columns = ['FUM ($m)#' if col == "FUM" in col else col for col in df.columns]
                    # remove "Historical Distribution Yield" column from the dataframe because it exists only in 2017
                    df = df.drop(['Historical Distribution Yield'], axis=1, errors='ignore')
                elif "INFRA" in sheet_name.upper():
                    # rename the column "Mkt Cap ($m)#" into "Mkt Cap ($m)"
                    df.columns = ['Mkt Cap ($m)' if 'Mkt Cap ($m)' in col else col for col in df.columns]

            # remove nan columns from the df column  for all sheets
            df = df[df.columns.dropna()]  # df = df.loc[:, df.columns.notnull()]
            df['Period'] = period  # add "Period" column

            each_data['Sheet'] = sheet_name
            each_data['Sheet_df'] = df
            processed_data.append(each_data)
        else:
            generate_log(f'\t\t\t {sheet_name} is not listed in {SHEET_TO_USE}')
            each_data['Sheet'] = sheet_name
            each_data['Sheet_df'] = df
            processed_data.append(each_data)

    return processed_data


# ============================
# MAIN PROGRAM : START SCRIPT
# ============================
if __name__ == "__main__":
    # creating the directories
    create_dir(OUTPUT_DIR)
    create_dir(LOGS_DIR)
    start = datetime.now()
    start_day = start.strftime("%Y-%m-%d")

    logfile = f'{start_day}_{os.path.basename(__file__).split(".")[0]}.log'
    lf = open(f'{LOGS_DIR}\\{logfile}', 'a')

    welcome_log(f'\t\t==================================================================================')
    welcome_log(f'\t\t\t\t\t\t\t\t\tASX FUNDS : STARTED')
    welcome_log(f'\t\t==================================================================================')
    welcome_log(f'\t\t\t\t\t\tFILTER YEARs(FILTER_YEAR) : {FILTER_YEAR}')

    # all_funds_df = pd.DataFrame()
    all_funds_df = {k: pd.DataFrame() for k in SHEET_TO_USE}  # Empty data frame initialize for each sheet

    # get all funds lists from the FUND_LIST_URL url
    fund_list = get_all_fund_list(FUND_LIST_URL)
    filtered_fund_list = []

    # filter the fund list based on user FILTER_YEAR years configuration
    if FILTER_YEAR[0].lower() == "all":
        filtered_fund_list = fund_list
    else:
        # remove "Year" from the fund list because it is not needed further
        # and filtered only for given year data FILTER_YEAR
        filtered_fund_list = [x for x in fund_list if x.pop('Year') in FILTER_YEAR]

    # Display not valid FILTER YEAR configuration if there is no filtered list of funds.
    if not filtered_fund_list:
        generate_log(f'Your FILTER_YEAR : {FILTER_YEAR} is not valid. Please re-configure FILTER_YEAR.')
    else:
        for monthly_data in filtered_fund_list:
            if monthly_data:
                desc = monthly_data['Description']
                exchange = monthly_data['Exchange']
                period = monthly_data['Period']

                generate_log(f'{desc}\tStarting .........')
                response_data = get_monthly_products(monthly_data)
                if response_data:
                    for each_sheet_df in response_data:
                        each_sheet_name = each_sheet_df['Sheet']
                        each_df = each_sheet_df['Sheet_df']
                        if not each_df.empty:
                            valid_sheet = [col for col in SHEET_TO_USE if col in each_sheet_name.upper()]
                            if valid_sheet:
                                valid_sheet_name = valid_sheet[0]
                                # add each df to corresponding sheet
                                all_funds_df[valid_sheet_name] = all_funds_df[valid_sheet_name].append(each_df)
                                if SAVE_INDIVIDUAL_FILE:
                                    save_file = f'{OUTPUT_DIR}\\{start.strftime("%Y-%m-%d")}-{valid_sheet_name}-{desc}.xlsx'
                                    each_df.to_excel(save_file, sheet_name=valid_sheet_name, index=False, freeze_panes=(1, 0))

                                generate_log(f'{desc}\t Sheet({valid_sheet_name})\t\t : completed.')

        for key in all_funds_df.keys():
            funds = all_funds_df[key]
            if not funds.empty:
                # save into .xlsx format
                save_file = OUTPUT_FUNDS_FILE + "-" + key + ".xlsx"
                funds.to_excel(save_file, sheet_name=key, index=False, freeze_panes=(1, 0))
                file_bytes_size = os.path.getsize(save_file)
                generate_log(f'')
                generate_log(f'Saved the combined file {save_file} size {round(file_bytes_size / (1024), 0)} KB')

                # save into .csv format
                save_file = OUTPUT_FUNDS_FILE + "-" + key + ".csv"
                funds.to_csv(save_file, index=False)
                file_bytes_size = os.path.getsize(save_file)
                generate_log(f'Saved the combined file {save_file} size {round(file_bytes_size / (1024), 0)} KB')

end = datetime.now()
time_taken = end - start
minute, sec = divmod(time_taken.seconds, 60)

generate_log(f'')
generate_log(f'Application took {minute} minutes, {sec} seconds for execution.')
generate_log(f'***********************************************************************')
generate_log(f'\t\t\tASX FUNDS : COMPLETED')
generate_log(f'***********************************************************************\n')

lf.close()
print("Log has been generated at: " + LOGS_DIR + "\\" + logfile)
