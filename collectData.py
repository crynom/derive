import requests, pandas as pd
from datetime import datetime as dt
import os

filePath = os.path.dirname(os.path.realpath(__file__))

def assignTsBucket(ts: float, allTimestamps: list[int]) -> int:
    allTs = allTimestamps + [ts]
    allTs.sort()
    i = allTs.index(ts) + 1
    return None if i > len(allTimestamps) else allTs[i]


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
        history = pd.read_excel(f'{path}\\candles.xlsx')
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
        history = pd.read_excel(f'{path}\\trades.xlsx')
        df = pd.concat([df, history], axis=0)
        df = df.drop_duplicates(subset=['trade_id']).reset_index(drop=True)
    df.to_excel(f'{path}\\trades.xlsx', index=False)
    return df


def aggregateTrades(trades: pd.DataFrame) -> pd.DataFrame:
    groups = trades.groupby('tsBucket')
    pnlGroups = trades.groupby(['tsBucket', 'direction'])
    trades['trade_price'] = trades.trade_price.astype(float)
    trades['trade_amount'] = trades.trade_amount.astype(float)
    trades['index_price'] = trades.index_price.astype(float)
    trades['realized_pnl'] = trades.realized_pnl.astype(float)

    aggTrades = groups.agg(
        maxPrice = pd.NamedAgg(column='trade_price', aggfunc='max'),
        minPrice = pd.NamedAgg(column='trade_price', aggfunc='min'),
        avgIndexPrice = pd.NamedAgg(column='index_price', aggfunc='mean')
    ).reset_index()

    minVols = []
    maxVols = []
    lastPrices = []
    lastVols = []
    pnlSells = []
    pnlBuys = []

    for row in aggTrades.itertuples():
        ts = row.tsBucket
        group = groups.get_group(ts)
        minTradeVol = sum(group[group.trade_price == min(group.trade_price)].trade_amount)
        minVols.append(minTradeVol)
        maxTradeVol = sum(group[group.trade_price == max(group.trade_price)].trade_amount)
        maxVols.append(maxTradeVol)
        lastPrice = group[group.timestamp == max(group.timestamp)].trade_price.mean()
        lastPrices.append(lastPrice)
        lastVol = sum(group[group.timestamp == max(group.timestamp)].trade_amount)
        lastVols.append(lastVol)

        try:
            sells = pnlGroups.get_group((ts, 'sell'))
            pnlSell = sum(sells.realized_pnl)
            pnlSells.append(pnlSell)
        except KeyError:
            pnlSells.append(0)

        try:
            buys = pnlGroups.get_group((ts, 'buy'))
            pnlBuy = sum(buys.realized_pnl)
            pnlBuys.append(pnlBuy)
        except KeyError:
            pnlBuys.append(0)

    aggTrades['minVol'] = minVols
    aggTrades['maxVol'] = maxVols
    aggTrades['lastPrice'] = lastPrices
    aggTrades['lastVol'] = lastVols
    aggTrades['pnlSell'] = pnlSells
    aggTrades['pnlBuy'] = pnlBuys

    return aggTrades


def mergeTables(funding: pd.DataFrame, candles: pd.DataFrame, trades: pd.DataFrame, path: str = filePath) -> pd.DataFrame:
    '''This function will merge all the arguments on their respective timestamps.
    Returns a table called history.'''
    funding['normalizedTs'] = (funding.timestamp / 1000).astype(int)
    fundingCandles = funding.merge(candles, how='left', left_on='normalizedTs', right_on='timestamp_bucket')
    allTimestamps = fundingCandles.normalizedTs.to_list()
    trades['ts'] = trades.timestamp / 1000
    trades['tsBucket'] = trades.ts.apply(assignTsBucket, args=(allTimestamps,))
    trades = aggregateTrades(trades)
    history = fundingCandles.merge(trades, how='left', left_on='normalizedTs', right_on='tsBucket')
    history['datetime'] = history.tsBucket.apply(lambda t: dt.fromtimestamp(t) if not pd.isna(t) else None)
    # trades now aggregated, need to debug output
    trades['datetime'] = trades.tsBucket.apply(lambda t: dt.fromtimestamp(t) if not pd.isna(t) else None)
    trades.to_excel(f'{path}\\aggTrades.xlsx', index=False)
    if 'history.xlsx' in os.listdir(path):
        df = pd.read_excel(f'{path}\\history.xlsx')
        history = pd.concat([history, df], axis=0)
        history.drop_duplicates(subset=['normalizedTs'])
    history.to_excel(f'{path}\\history.xlsx', index=False)
    return history

if __name__ == '__main__':

    funding = collectFundingRate()
    tMax, tMin = max(funding.timestamp), min(funding.timestamp)
    candles = collectCandles(tMax, tMin)
    trades = collectTrades()
    history = mergeTables(funding, candles, trades)