from yfinance.utils import get_json
import json


def main():
    """
    Pulls sector, industry, and sub-industry tickers from yahoo finance and stores them in a json file.
    :return:
    """

    sp500_tickers = {}

    # Sectors
    for sector_number in range(0, 100, 5):
        sector = get_json(f"https://finance.yahoo.com/quote/^SP500-{sector_number}")

        if sector:
            long_name_sector = sector['quoteType']['longName']
            print(f"Working on Sector {long_name_sector}")
            sp500_tickers[long_name_sector] = {'sector_ticker': f"^SP500-{sector_number}", 'industries': {}}
        else:
            continue

        # Industries
        for industry_number in range(0, 100, 5):
            industry = get_json(
                f"https://finance.yahoo.com/quote/^SP500-{sector_number}{industry_number}")

            if industry:
                long_name_industry = industry['quoteType']['longName']
                print(f"Working on Industry {long_name_industry}")
                sp500_tickers[long_name_sector]['industries'][long_name_industry] = {
                    'industry_ticker': f"^SP500-{sector_number}{industry_number}",
                    'sub_industries': {}}
            else:
                continue

            # Sub-Industries
            for sub_industry_number in range(0, 100, 5):
                sub_industry = get_json(
                    f"https://finance.yahoo.com/quote/^SP500-"
                    f"{sector_number}{industry_number}{sub_industry_number}")

                if sub_industry:
                    long_name_sub_industry = sub_industry['quoteType']['longName']
                    print(f"Working on Sub Industry {long_name_sub_industry}")
                    sp500_tickers[long_name_sector]['industries'][
                        long_name_industry]['sub_industries'][
                        long_name_sub_industry] = f"^SP500-{sector_number}{industry_number}{sub_industry_number}"
                else:
                    continue

    with open('sp500_sectors_and_industries.json', 'w') as file:
        json.dump(sp500_tickers, file, indent=2)


if __name__ == "__main__":
    main()
