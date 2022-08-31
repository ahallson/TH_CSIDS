import json
import yfinance as yf
import pandas as pd

"""
This loads data for EVERY sector, industry, and sub-industry
"""

def get_req_data(start_date, end_date):
    '''
    Currently concerned only with sector
    Inputs:
    start_date = string, "YYYY-MM-DD"
    end_date = string, "YYYY-MM-DD"
    '''
    # Load tickers from json file
    sp500_tickers = json.load(open("sp500_sectors_and_industries.json", "r"))
    
    sp500_tickers_data = {}  # to store data

    for sector, industry_groups in sp500_tickers.items():  # iterate thru the sectors in the json file
        # load the data required 
        sp500_tickers_data[sector] = {  # builds a dictionary for the sector
            "sector_data": yf.download(industry_groups['sector_ticker'], start=start_date, end=end_date, progress=False)['Adj Close']
            }  # stores the data here

    return sp500_tickers_data

# Original File:

# TODO: make this a function that takes a sector, industry, or sub-industry and returns the data

# # Load tickers from json file
# sp500_tickers = json.load(open("sp500_sectors_and_industries.json", "r"))

# sp500_tickers_data = {}  # to store data

# START = input("Input start date in YYYY-MM-DD\n")  # TODO: this should be function input

# # This below needs to be traversed, looking for the input ticker, and grabbing the data for said ticker.
# # Should look like if ticker matches input, then download.

# for sector, industry_groups in sp500_tickers.items():  # iterate thru the sectors in the json file
#     print(f"Busy with {sector}")
#     sp500_tickers_data[sector] = {  # builds a dictionary for the sector
#         "sector_data": yf.download(industry_groups['sector_ticker'], start=START, progress=False)['Adj Close'],  # stores the data here
#         "industry_groups": {}}  # create a new dictionary for the industry groups, so it is associated with the sector

#     for industry_group, industries in industry_groups['industries'].items(): # iterate thru the industry groups in the json file
#         if industry_group == "sector_ticker":  # skip the first line of the json file
#             continue

#         sp500_tickers_data[sector]['industry_groups'][industry_group] = pd.DataFrame(
#             yf.download(industries['industry_ticker'], start=START, progress=False)['Adj Close'])
#         sp500_tickers_data[sector]['industry_groups'][industry_group].rename(
#             columns={'Adj Close': industry_group}, inplace=True)

#         for industry, ticker in industries['sub_industries'].items():
#             if industry == "industry_ticker":
#                 continue

#             sp500_tickers_data[sector]['industry_groups'][industry_group][industry] = yf.download(
#                 ticker, start=START, progress=False)['Adj Close']
