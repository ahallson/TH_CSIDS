import logging
from typing import Dict

import pandas as pd
import requests
from openbb_terminal.decorators import log_start_end
from datetime import datetime

logger = logging.getLogger(__name__)

import os
import json
import pandas as pd
import yfinance as yf
import numpy as np
import yahooquery as yq

SPY_SECTORS_MAP =  {
        'S&P 500 Materials (Sector)': 'basic_materials',
        'S&P 500 Industrials (Sector)': 'industrials',
        'S&P 500 Consumer Discretionary (Sector)': 'consumer_cyclical',
        'S&P 500 Consumer Staples (Sector)': 'consumer_defensive',
        'S&P 500 Health Care (Sector)': 'healthcare',
        'S&P 500 Financials (Sector)': 'financial_services',
        'S&P 500 Information Technology (Sector)': 'technology',
        'S&P 500 Telecommunication Services (Sector)': 'communication_services',
        'S&P 500 Utilities (Sector)': 'utilities',
        'S&P 500 Real Estate (Sector)': 'realestate',
        'S&P 500 Energy (Sector)': 'energy'
    }

PF_SECTORS_MAP = {
        'Basic Materials': 'S&P 500 Materials (Sector)',
        'Industrials': 'S&P 500 Industrials (Sector)',
        'Consumer Cyclical': 'S&P 500 Consumer Discretionary (Sector)',
        'Consumer Defensive': 'S&P 500 Consumer Staples (Sector)',
        'Healthcare': 'S&P 500 Health Care (Sector)',
        'Financial Services': 'S&P 500 Financials (Sector)',
        'Technology': 'S&P 500 Information Technology (Sector)',
        'Communication Services': 'S&P 500 Telecommunication Services (Sector)',
        'Utilities': 'S&P 500 Utilities (Sector)',
        'Real Estate': 'S&P 500 Real Estate (Sector)',
        'Energy': 'S&P 500 Energy (Sector)',
    }



def get_spy_sector_contributions(start_date, end_date):  # format like 2015-01-15 (YYYY-MM-DD)

    # Sector Map

    sectors_ticker = "SPY"

    # Load in info
    sp500_tickers_data = get_daily_sector_prices(start_date, end_date)
    weights = yq.Ticker(sectors_ticker).fund_sector_weightings.to_dict()

    # add the sectors + dates + adj close to the dataframe
    records = []
    for sector, data in sp500_tickers_data.items():
        for x in range(0, len(data['sector_data'])):
            record = {"sector": sector, "date": data['sector_data'].index[x], "adj_close": data["sector_data"][x],
                      "sector_weight": weights[sectors_ticker][SPY_SECTORS_MAP[sector]]}
            records.append(record)

    df = pd.DataFrame(records)

    df["pct_change"] = df.groupby("sector")["adj_close"].pct_change()

    df["contribution"] = df["pct_change"] * df["sector_weight"]

    contributions = round(df.groupby("sector").agg({"contribution": "sum"}), 2)
    contributions["contribution_as_pct"] = round((contributions["contribution"] / df["contribution"].sum()) * 100, 2)

    return contributions

def get_portfolio_sector_contributions(start_date, portfolio_trades: pd.DataFrame):

    price_data = {}
    contrib_df = pd.DataFrame()
    asset_tickers = list(portfolio_trades["Ticker"].unique())
    first_price = portfolio_trades["Date"].min()


    price_data = yf.download(asset_tickers, start=first_price, progress=False)["Adj Close"]
    price_change = price_data.pct_change()


    cumulative_positions = portfolio_trades.copy()
    cumulative_positions["Quantity"] = cumulative_positions.groupby("Ticker")["Quantity"].cumsum()

    cumulative_positions_wide = pd.pivot(cumulative_positions, index="Date",columns="Ticker",values="Quantity")

    index = pd.date_range(start=first_price, end=datetime.now(), freq="1D")
    contrib_df = cumulative_positions_wide.reindex(index).ffill(axis=0)
    contrib_df = contrib_df.div(contrib_df.sum(axis=1), axis=0)
    contrib_df = contrib_df * price_change

    # Wide to Long
    contrib_df = contrib_df.reset_index()
    contrib_df = pd.melt(contrib_df, id_vars="index")
    
    # # Get Sectors
    sector_df = portfolio_trades[["Ticker","Sector"]].groupby("Ticker").agg({"Sector":"min"}).reset_index()


    contrib_df = pd.merge(contrib_df, sector_df)
    contrib_df = contrib_df.rename(columns={"value":"contribution"})

    contrib_df = contrib_df.groupby("Sector").agg({"contribution": "sum"})
    contrib_df["contribution_as_pct"] = (contrib_df["contribution"] / contrib_df["contribution"].sum())*100

    contrib_df = contrib_df.rename(index=PF_SECTORS_MAP)
    contrib_df = contrib_df.reindex(PF_SECTORS_MAP.values())
    contrib_df = contrib_df.fillna(0)


    # For each day multiply by the holding on that day to get attribution for that asset
    return contrib_df
    # 



def get_portfolio_sector_contributions_original(start_date, portfolio_trades: pd.DataFrame):
    """
    Calculate sector attribution
    """
    pulled_tickers = {}
    stocks_added = {}
    ticker_data = {}
    portfolio_data = pd.DataFrame()
    portfolio_weighted = pd.DataFrame()
    sector_data = pd.DataFrame()

    # Pull data for each stock
    for i, trade in enumerate(portfolio_trades.iterrows()):

        if trade[1]['Ticker'] not in pulled_tickers.keys():  # only need data for every ticker once
            # Get ticker from yf
            ticker_data[trade[1]["Ticker"]] = yf.download(trade[1]['Ticker'], start=trade[1]["Date"], progress=False)

            if i == 0:  # create df on first iteration
                portfolio_data = pd.DataFrame()
                portfolio_data.index = ticker_data[trade[1]["Ticker"]].index

            # Creates wide dataframe Matrix of dates x ticker
            portfolio_data[trade[1]["Ticker"]] = ticker_data[trade[1]["Ticker"]]["Adj Close"]
            # Add to dict of tickers that have downloaded data
            pulled_tickers[trade[1]['Ticker']] = yf.Ticker(trade[1]['Ticker'])

        # Weight by number of shares in the given date range
        if i == 0:  # create df on first iteration
            portfolio_weighted = portfolio_data.copy()

        # for the first trade of a stock, create new column with weighted data
        if trade[1]["Ticker"] not in portfolio_weighted.columns:
            portfolio_weighted[trade[1]["Ticker"]] = portfolio_data[trade[1]["Ticker"]][
                                                         portfolio_data.index >= trade[1]["Date"]] * trade[1][
                                                         "Quantity"]
        else:
            # for each trade after, need to add the new shares to the existing weighted portfolio

            portfolio_weighted[trade[1]["Ticker"]][portfolio_weighted.index >= trade[1]["Date"]] += \
                portfolio_data[trade[1]["Ticker"]][portfolio_data.index >= trade[1]["Date"]] * trade[1]["Quantity"]


    for i, trade in enumerate(
            portfolio_trades.iterrows()):  # we re-iterate through the trades as we need a fully-contructed portfolio_weighted df

        # grouping by sector
        if trade[1]["Ticker"] not in stocks_added.keys():  # check data for a stock not already added

            if trade[1]["Sector"] not in sector_data.columns:  # case sector is not in df yet
                sector_data[trade[1]["Sector"]] = portfolio_weighted[trade[1]["Ticker"]]


            else:  # sector in columns, stock not added
                sector_data[trade[1]["Sector"]][portfolio_data.index >= trade[1]["Date"]] += \
                    portfolio_weighted[trade[1]["Ticker"]][portfolio_data.index >= trade[1]["Date"]]

            stocks_added[trade[1]["Ticker"]] = [trade[1]["Ticker"]]
    sectors = [
        'Basic Materials',
        'Industrials',
        'Consumer Cyclical',
        'Consumer Defensive',
        'Healthcare',
        'Financial Services',
        'Technology',
        'Communication Services',
        'Utilities',
        'Real Estate',
        'Energy'
    ]
    # fill in missing sectors
    for sector in sectors:
        if sector not in sector_data.columns:
            sector_data[sector] = 0

    sector_data.fillna(0, inplace=True)

    # calculate daily weightings
    sector_weights = sector_data.div(sector_data.sum(axis=1), axis=0)

    # reformat df to long format so that it integrates with the rest of the code
    records = []
    for i, row in enumerate(sector_weights.iterrows()):  # iterrows is not the best but it works for now

        for sector in sectors:
            record = {"sector": PF_SECTORS_MAP[sector],
                      "date": row[0],
                      "adj_close": sector_data[sector][i],
                      "sector_weight": row[1][sector]}
            records.append(record)
    df = pd.DataFrame(records)

    # filter passed off desired date here 
    df["date"] = df["date"].dt.date
    df = df[~(df["date"] < start_date)]

    # get desired output 
    df["adj_close"].fillna(0, inplace=True)
    df["sector_weight"].fillna(0, inplace=True)

    df["pct_change"] = df.groupby("sector")["adj_close"].pct_change()
    df.replace([np.inf, -np.inf], 0, inplace=True)
    df["contribution"] = round(df["pct_change"] * df["sector_weight"], 2)
    contributions = df.groupby("sector").agg({"contribution": "sum"})
    contributions["contribution_as_pct"] = round((contributions["contribution"] / df["contribution"].sum()) * 100, 2)

    # We standardize output DF from here
    # result_df = contributions.loc[:,contributions.columns != "contribution"]
    return contributions


def percentage_attrib_categorizer(bench_df, port_df):
    # rename columns
    bench_df.rename(columns={"contribution_as_pct": "S&P500 [%]"}, inplace=True)
    port_df.rename(columns={"contribution_as_pct": "Portfolio [%]"}, inplace=True)
    # append instead 
    result = bench_df.join(port_df)

    # 1. Excess Attribution

    result["Excess Attribution"] = round(result["Portfolio [%]"] - result["S&P500 [%]"], 2)

    # 2. Attribution Ratio

    result["Attribution Ratio"] = round(result["Portfolio [%]"] / result["S&P500 [%]"], 2)

    # 3. Attribution Direction

    direction = []

    for ratio in result["Attribution Ratio"]:
        if ratio >= 0:
            direction.append("Correlated (+)")
        elif ratio < 0:
            direction.append("Uncorrelated (-)")

    result["Attribution Direction [+/-]"] = direction

    # 4. Attribution Sensetivity

    sensitivity = []

    for ratio in result["Attribution Ratio"]:
        if abs(ratio) > 1.25:
            sensitivity.append("High")
        elif 0.75 <= abs(ratio) <= 1.25:
            sensitivity.append("Normal")
        elif abs(ratio) < 0.75:
            sensitivity.append("Low")

    result["Attribution Sensitivity"] = sensitivity

    return result


def raw_attrib_categorizer(bench_df, port_df):
    # rename columns
    bench_df.rename(columns={"contribution": "S&P500"}, inplace=True)
    port_df.rename(columns={"contribution": "Portfolio"}, inplace=True)
    # append instead 
    result = bench_df.join(port_df)

    # 1. Excess Attribution

    result["Excess Attribution"] = round(result["Portfolio"] - result["S&P500"], 2)

    # 2. Attribution Ratio

    result["Attribution Ratio"] = round(result["Portfolio"] / result["S&P500"], 2)

    # 3. Attribution Direction

    direction = []

    for ratio in result["Attribution Ratio"]:
        if ratio >= 0:
            direction.append("Correlated (+)")
        elif ratio < 0:
            direction.append("Uncorrelated (-)")

    result["Attribution Direction [+/-]"] = direction

    # 4. Attribution Sensetivity

    sensitivity = []

    for ratio in result["Attribution Ratio"]:
        if abs(ratio) > 1.25:
            sensitivity.append("High")
        elif 0.75 <= abs(ratio) <= 1.25:
            sensitivity.append("Normal")
        elif abs(ratio) < 0.75:
            sensitivity.append("Low")

    result["Attribution Sensitivity"] = sensitivity

    return result

# our functions here;
def get_daily_sector_prices(start_date, end_date):
    '''
    Currently concerned only with sector
    Inputs:
    start_date = string, "YYYY-MM-DD"
    end_date = string, "YYYY-MM-DD"
    '''
    # Load tickers from json file
    ticker_json_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "sp500_sectors_and_industries.json")
    sp500_tickers = json.load(open(ticker_json_path, "r"))

    sp500_tickers_data = {}  # to store data

    for sector, industry_groups in sp500_tickers.items():  # iterate thru the sectors in the json file
        # load the data required 
        sp500_tickers_data[sector] = {  # builds a dictionary for the sector
            "sector_data":
                yf.download(industry_groups['sector_ticker'], start=start_date, end=end_date, progress=False)[
                    'Adj Close']
        }  # stores the data here

    return sp500_tickers_data

