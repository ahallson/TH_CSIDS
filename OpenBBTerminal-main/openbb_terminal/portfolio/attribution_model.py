import pandas as pd
import yfinance as yf


def get_daily_sector_sums_from_portfolio(portfolio_trades: pd.DataFrame):  # start date end date output
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

        print(i)

        if trade[1]['Ticker'] not in pulled_tickers.keys():  # only need data for every ticker once
            # Get ticker from yf
            ticker_data[trade[1]["Ticker"]] = yf.download(trade[1]['Ticker'], start=trade[1]["Date"])

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
    print(sector_weights)

    # reformat df to long format so that it integrates with the rest of the code
    records = []
    for i, row in enumerate(sector_weights.iterrows()):  # iterrows is not the best but it works for now

        for sector in sectors:
            record = {"sector": sector,
                      "date": row[0],
                      "adj_close": sector_data[sector][i],
                      "sector_weight": row[1][sector]}
            records.append(record)
    data_long = pd.DataFrame(records)

    # TODO: Debug code, remove later
    """    
    df.to_csv("C:\\Users\\ajhal\\Projects_Code\\TH_CSIDS\\OpenBBTerminal-main\\portfolio\\holdings\\long_data.csv")
    print(df)

    sector_map = {
                'Basic Materials': 'basic_materials',
                'Industrials': 'industrials',
                'Consumer Cyclical': 'consumer_cyclical',
                'Consumer Defensive': 'consumer_defensive',
                'Healthcare': 'healthcare',
                'Financial Services': 'financial_services',
                'Technology': 'technology',
                'Communication Services': 'communication_services',
                'Utilities': 'utilities',
                'Real Estate': 'realestate',
                'Energy': 'energy'
            }
    """

    return data_long


if __name__ == "__main__":
    df = pd.read_excel(  # TODO: Change to your own path
        "C:\\Users\\ajhal\\Projects_Code\\TH_CSIDS\\OpenBBTerminal-main\\portfolio\\holdings\\Public_Equity_Orderbook"
        ".xlsx")
    get_daily_sector_sums_from_portfolio(df)
