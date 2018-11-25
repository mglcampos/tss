
import pandas as pd
import os
import chardet
import time
import datetime
#
# def import_data(path = 'histdata/stocks_usd_darwinex'):
#
#     dfs = []
#     for filename in os.listdir(path):
#         if filename.endswith(".csv") or filename.endswith(".txt"):
#             print(path + '/' + filename)
#
#             with open(path + '/' + filename, 'rb') as f:
#                 result = chardet.detect(f.readline())
#
#             df = pd.read_csv(path + '/' + filename, header=None, names = ['Symbol','Date','Seconds','Open','High','Low','Close','Volume'], sep=';', encoding=result['encoding'])
#             df.index = pd.to_datetime(df['Date'] + ' ' + df['Seconds'])
#             dfs.append(df)
#
#     print(len(dfs),dfs[0])
#
# def merge_day_time(df):
#     t0 = time.clock()
#     t1 = time.time()
#
#     date_index = []
#
#     for ir in df.itertuples():
#         date = str(ir[2]) + ' ' + str(ir[3])
#         # print date
#         date = datetime.datetime.strptime(date, "%Y.%m.%d %H:%M")
#         date_index.append(date)
#
#     df = df.set_index([date_index])
#
#     print "# merge_day_time # -", time.clock() - t0, "seconds process time"
#     print "# merge_day_time # -", time.time() - t1, "seconds wall time"
#
#
# import_data()



import tradingeconomics as te
from time import sleep

with open("links", 'r') as f:
    tmp = []
    for row in f:
        if not row.split(".com/")[1].replace("\n","") in tmp:
            tmp.append(row.split(".com/")[1].replace("\n",""))

te.login("75420451870E4F9:7C9E487641174F6")

for ticker in tmp:
    sleep(0.3)
    df = te.fetchMarkets(symbol=ticker, initDate='1991-01-01')
    # df['date'] = df.index
    print(df)
    df = df.drop('symbol', axis=1)
    df[ticker] = df['close']
    df = df.drop('close', axis=1)
    df = df.drop('low', axis=1)
    df = df.drop('high', axis=1)
    df = df.drop('open', axis=1)
    start_date = str(df.index[-1].strftime('%m/%d/%Y')).replace('/','-')
    end_date = str(df.index[0].strftime('%m/%d/%Y')).replace('/','-')
    symbol = str(ticker).replace(':','-')
    df.to_csv(symbol+"_daily_minimal.csv")
    print(symbol+"_daily_"+start_date+"_"+end_date+"_minimal.csv")
    print(df)