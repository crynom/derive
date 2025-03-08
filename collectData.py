import requests, pandas as pd
from datetime import datetime as dt
import os

filePath = os.path.dirname(os.path.realpath(__file__))
print(filePath)

def collectFundingRate(period: int=900, path: str=filePath):
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


if __name__ == '__main__':
    collectFundingRate()