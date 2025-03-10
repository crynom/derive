import requests, pandas as pd
from datetime import datetime as dt
import os

filePath = os.path.dirname(os.path.realpath(__file__))


def collectFundingRate(period: int=900, path: str=filePath) -> pd.DataFrame:

    '''Collects historical funding rate. To be joined on the timestamp.
    To note: the timestamp must be divided by 1000 to be used with datetime.from_timestamp()'''

    url = "https://api.lyra.finance/public/get_funding_rate_history"

    payload = {
        "end_timestamp": 9223372036854776000,
        "period": period,
        "start_timestamp": 0,
        "instrument_name": "ETH-PERP"
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)

    df = pd.json_normalize(response.json()['result']['funding_rate_history'])
    if 'fundingRate.xlsx' in os.listdir(path):
        history = pd.read_excel(f'{path}\\fundingRate.xlsx')
        df = pd.concat([df, history], axis=0)
        df = df.drop_duplicates(subset=['timestamp']).reset_index(drop=True)
    df.to_excel(f'{path}\\fundingRate.xlsx', index=False)
    return df


def collectCandles(tMax: int, tMin: int, path: str=filePath) -> pd.DataFrame:

    '''Collects candles, not the most precise it seems but it will have to do.
    To be joined on timestamp_bucket.'''

    url = "https://api.lyra.finance/public/get_spot_feed_history_candles"

    payload = {
        "period": 900,
        "end_timestamp": tMax / 1000,
        "start_timestamp": tMin / 1000,
        "currency": "ETH"
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)
    df = pd.json_normalize(response.json()['result']['spot_feed_history']).sort_values(by=['timestamp_bucket']).reset_index(drop=True)
    if 'candles.xlsx' in os.listdir(path):
        history = pd.read_excel('candles.xlsx')
        df = pd.concat([df, history], axis=0)
        df = df.drop_duplicates(subset=['timestamp_bucket']).reset_index(drop=True)
    df.to_excel(f'{path}\\candles.xlsx', index=False)
    return df


def collectTrades(path:str = filePath, pages: int = 7) -> pd.DataFrame:

    '''Collects trade history. Pages argument can extend the history requested,
        but this will only reduce how often the script must be run. To be joined on timestamp.
        To note: the timestamp must be divided by 1000 to be used with datetime.from_timestamp()'''
    
    url = "https://api.lyra.finance/public/get_trade_history"

    dfs = []

    for page in range(1, pages + 1):
        payload = {
            "currency": "ETH",
            "from_timestamp": 0,
            "instrument_type": "perp",
            "page": page,
            "page_size": 1000,
            "to_timestamp": 18446744073709552000,
            "trade_id": None,
            "tx_hash": None,
            "tx_status": "settled"
        }
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers)
        dfs.append(pd.json_normalize(response.json()['result']['trades']))
    df = pd.concat(dfs, axis=0)

    if 'trades.xlsx' in os.listdir(path):
        history = pd.read_excel('trades.xlsx')
        df = pd.concat([df, history], axis=0)
        df = df.drop_duplicates(subset=['trade_id']).reset_index(drop=True)
    df.to_excel(f'{path}\\trades.xlsx', index=False)
    return df


def mergeTables(funding: pd.DataFrame, candles: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    '''This function will merge all the arguments on their respective timestamps.
    Returns a table called history.'''
    pass


if __name__ == '__main__':
    funding = collectFundingRate()
    tMax, tMin = max(funding.timestamp), min(funding.timestamp)
    candles = collectCandles(tMax, tMin)
    trades = collectTrades()
