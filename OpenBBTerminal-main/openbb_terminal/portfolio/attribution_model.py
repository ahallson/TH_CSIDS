import os
import json
import pandas as pd
import yfinance as yf
import numpy as np
import yahooquery as yq


def get_spy_sector_contributions(start_date, end_date):  # format like 2015-01-15 (YYYY-MM-DD)

    # Sector Map
    sector_map = {
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
    sectors_ticker = "SPY"

    # Load in info
    sp500_tickers_data = get_daily_sector_prices(start_date, end_date)
    weights = yq.Ticker(sectors_ticker).fund_sector_weightings.to_dict()

    # add the sectors + dates + adj close to the dataframe
    records = []
    for sector, data in sp500_tickers_data.items():
        for x in range(0, len(data['sector_data'])):
            record = {"sector": sector, "date": data['sector_data'].index[x], "adj_close": data["sector_data"][x],
                      "sector_weight": weights[sectors_ticker][sector_map[sector]]}
            records.append(record)

    df = pd.DataFrame(records)

    df["pct_change"] = df.groupby("sector")["adj_close"].pct_change()

    df["contribution"] = df["pct_change"] * df["sector_weight"]

    # print(weights)
    # display(df)

    contributions = round(df.groupby("sector").agg({"contribution": "sum"}), 2)
    contributions["contribution_as_pct"] = round((contributions["contribution"] / df["contribution"].sum()) * 100, 2)

    # We standardize output DF form here
    # result_df = contributions.loc[:,contributions.columns != "contribution"]

    # result_df.rename(columns={"contribution_as_pct":"S&P 500 [%]"}, inplace=True)

    # result_df["Portfolio [%]"] =  #[INSERT PORTFOLIO ATTRIBUTIONS HERE!!!]

    # return result_df
    return contributions


def get_daily_sector_sums_from_portfolio(start_date, portfolio_trades: pd.DataFrame):
    """
    Calculate sector attribution
    """
    pulled_tickers = {}
    stocks_added = {}
    ticker_data = {}
    portfolio_data = pd.DataFrame()
    portfolio_weighted = pd.DataFrame()
    sector_data = pd.DataFrame()

    sector_map = {
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

    # Pull data for each stock
    for i, trade in enumerate(portfolio_trades.iterrows()):

        if trade[1]['Ticker'] not in pulled_tickers.keys():  # only need data for every ticker once
            # Get ticker from yf
            ticker_data[trade[1]["Ticker"]] = yf.download(trade[1]['Ticker'], start=trade[1]["Date"], progress=False)

            if i == 0:  # create df on first iteration
                portfolio_data = pd.DataFrame()
                portfolio_data.index = ticker_data[trade[1]["Ticker"]].index

            portfolio_data[trade[1]["Ticker"]] = ticker_data[trade[1]["Ticker"]]["Adj Close"]
            # Add to dict
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
            record = {"sector": sector_map[sector],
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

if __name__ == "__main__":
    df = pd.read_excel(  # TODO: Change to your own path
        "C:\\Users\\ajhal\\Projects_Code\\TH_CSIDS\\OpenBBTerminal-main\\portfolio\\holdings\\Public_Equity_Orderbook"
        ".xlsx")
    assert get_daily_sector_sums_from_portfolio(df).shape[0] != 0, "Error: Attribution model not working"
