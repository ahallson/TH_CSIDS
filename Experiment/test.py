from yahooquery import Ticker

t = Ticker('SPY')

# sector weightings, returns pandas DataFrame
print(t.fund_sector_weightings)

import yfinance as yf

print(yf.download('', start='2000-01-01', end='2020-01-01'))