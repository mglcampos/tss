import bt
import pandas as pd
import tradingeconomics as te
import statsmodels.api as sm
import statsmodels.tsa.stattools as ts
import numpy as np
from bokeh.models import HoverTool, ColumnDataSource, BoxZoomTool
from bokeh.models.ranges import Range1d
from bokeh.models.axes import LinearAxis
from bokeh.layouts import gridplot
from bokeh.plotting import figure, show, output_file
from bokeh.io import output_notebook
import talib
pd.set_option("display.max_rows",1000)
from darwinex_data import DWX_Tick_Data
dwt = DWX_Tick_Data(dwx_ftp_user='mglcampos',
                 dwx_ftp_pass='2a8Ic7yydUVVKB', dwx_ftp_hostname='tickdata.darwinex.com')

## Load data

tickers = ['AUDCAD', 'AUDCHF', 'AUDJPY', 'AUDNZD', 'AUDUSD', 'AUS200',
           'CADCHF', 'CADJPY', 'CHFJPY', 'EURAUD', 'EURCAD', 'EURCHF',
           'EURGBP', 'EURJPY', 'EURMXN', 'EURNOK', 'EURNZD', 'EURSEK',
           'EURSGD', 'EURTRY', 'EURUSD', 'FCHI', 'GBPAUD', 'GBPCAD', 'GBPCHF',
           'GBPJPY', 'GBPMXN', 'GBPNOK', 'GBPNZD', 'GBPSEK', 'GBPTRY',
           'GBPUSD', 'GDAXIm', 'J225', 'LOCATION', 'NDXm', 'NZDCAD',
           'NZDCHF', 'NZDJPY', 'NZDUSD', 'SPN35', 'SPXm', 'STOXX50E', 'UK100',
           'USDCAD', 'USDCHF', 'USDHKD', 'USDJPY', 'USDMXN', 'USDNOK',
           'USDSEK', 'USDSGD', 'USDTRY', 'WS30', 'WS30m', 'XAGUSD', 'XAUUSD',
           'XBNUSD', 'XBTUSD', 'XETUSD', 'XLCUSD', 'XNGUSD', 'XPDUSD',
           'XPTUSD', 'XRPUSD', 'XTIUSD']
# hours = [str(i) for i in range(0,24)]
hours = ['12']
dates = ['2018-10-02']
data = {}
for ticker in tickers:
    df = pd.DataFrame()
    hour = hours[0]
    date = dates[0]
    try:
        df['ask'] = dwt._download_hour_(_asset=ticker, _date=date, _hour=hour,
                                        _ftp_loc_format='{}/{}_ASK_{}_{}.log.gz',
                                        _verbose=True)['ask']
    except:
        continue
    # df['bid'] = dwt._download_hour_(_asset=ticker, _date=date, _hour=hour,
    #                        _ftp_loc_format='{}/{}_BID_{}_{}.log.gz',
    #                        _verbose=True)['ask']

    data[ticker] = df.copy()

def cointegration(df1, df2):
    try:
        engle_granger = 0
        comb_index = df1.index.union(df2.index)
        df1 = df1.reindex(comb_index, method='ffill').fillna(method='ffill').fillna(method='bfill')
        df2 = df2.reindex(comb_index, method='ffill').fillna(method='ffill').fillna(method='bfill')
        #print(df1.isnull().any(), df2.isnull().any())
        #print(df1.head(), print(df2.head()))
        #print(len(df1), len(df2))
        #engle_granger = ts.coint(df1.values,df2.values)
        beta_hr = sm.OLS(df1.values[1:], df2.values[1:]).fit().params[0]
        res = df1.values - beta_hr * df2.values
        # Calculate and reports the CADF test on the residuals
        adf = ts.adfuller(res.flatten())
        return engle_granger, adf
    except:
        return 69, 69

#coint_matrix_adf = pd.DataFrame(columns=tickers, index=tickers)
#coint_matrix_engle = pd.DataFrame(columns=tickers, index=tickers)
coint_matrix_adf = {}
coint_matrix_engle = {}
cointegrated_adf = {}
cointegrated_engle = {}
for t1 in tickers:
    coint_matrix_adf[t1] = {}
    coint_matrix_engle[t1] = {}
    cointegrated_adf[t1] = {}
    cointegrated_engle[t1] = {}
    for t2 in tickers:
        if t2 == t1:
            continue
        try:
            coint_matrix_adf[t1][t2] = round(float(cointegration(data[t1]['ask'],data[t2]['ask'])[1][0]),4)
        except:
            coint_matrix_adf[t1][t2] = 0.0
        if coint_matrix_adf[t1][t2] < -2.0:
            continue
        #coint_matrix_engle[t1][t2] = round(float(cointegration(data[t1]['ask'],data[t2]['ask'])[0][0]),4)
        #if coint_matrix_engle[t1][t2] < -2.0:
         #   cointegrated_engle[t1][t2] = coint_matrix_engle[t1][t2]
        if coint_matrix_adf[t1][t2] < -2.0:
            cointegrated_adf[t1][t2] = coint_matrix_adf[t1][t2]

for ticker in tickers:
    for t in tickers:
        try:
            if coint_matrix_adf[ticker][t] < -3.4:
                print("COINT: {}".format(coint_matrix_adf[ticker][t]),ticker,t)
        except:
            continue