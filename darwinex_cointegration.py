import statsmodels.api as sm
import statsmodels.tsa.stattools as ts
from influxdb import InfluxDBClient
from requests import Session
import logging
from datetime import datetime as dt
from influxdb import InfluxDBClient
from datetime import timedelta
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='coint-darwinex.log', filemode='w')
logger = logging.getLogger()
httpsession = Session()
## Influx
user = 'admin'
pswd = 'jndm4jr5jndm4jr6'
rp = 'autogen'
precision = 'ns'
db = 'darwinex'
influx_host = 'http://localhost:8086/write?u={}&p?{}&db={}&u={}&p={}&rp={}&precision={}'.format(user, pswd, db,
                                                                                                user, pswd, rp, precision)

def cointegration(df1, df2):
    try:
        comb_index = df1.index.union(df2.index)
        df1 = df1.reindex(comb_index, method='ffill').fillna(method='ffill').fillna(method='bfill')
        df2 = df2.reindex(comb_index, method='ffill').fillna(method='ffill').fillna(method='bfill')
        #print(df1.isnull().any(), df2.isnull().any())
        #print(df1.head(), print(df2.head()))
        #print(len(df1), len(df2))
        #engle_granger = ts.coint(df1.values,df2.values)
        beta_hr = sm.OLS(df1.values, df2.values).fit().params[0]
        res = df1.values - beta_hr * df2.values
        # Calculate and reports the CADF test on the residuals
        adf = ts.adfuller(res.flatten())
        return adf
    except Exception as e:
        print(e)
        return 69

def influx_to_pandas(result):
    df = pd.DataFrame(result)
    df.index = df['time']

    return df.drop(['time'], axis=1)

def find_first_timestamp(ticker, quote, influx_client=None):
    """Finds the first point written and its timestamp."""
    if influx_client is None:
        influx_client = InfluxDBClient('104.248.41.39', 8086, 'admin', 'jndm4jr5jndm4jr6', 'darwinex')
    timestamp = list(influx_client.query("Select first(price) from {} where quote='{}'".format(ticker, quote)))[0][0]['time']
    dt_ts = dt.strptime(timestamp[:-4] + 'Z', '%Y-%m-%dT%H:%M:%S.%fZ') ## no conversion available to ns, reduce to us

    return dt_ts

def find_last_timestamp(ticker, quote, influx_client=None):
    """Finds the most recent point written and its timestamp."""
    if influx_client is None:
        influx_client = InfluxDBClient('104.248.41.39', 8086, 'admin', 'jndm4jr5jndm4jr6', 'darwinex')
    timestamp = list(influx_client.query("Select last(price) from {} where quote='{}'".format(ticker, quote)))[0][0]['time']
    dt_ts = dt.strptime(timestamp[:-4] + 'Z', '%Y-%m-%dT%H:%M:%S.%fZ') ## no conversion available to ns, reduce to us

    return dt_ts

def write_coint_to_influx(df, ticker):
    """."""

    lp_post = ''

    for row in df.itertuples():
        t = getattr(row, 'Index')
        price = getattr(row, 'ask')
        size = getattr(row, 'size')
        d = int(t.timestamp() * 1000 * 1000 * 1000)
        ## todo change this !!!!!!!!!!!
        lp_post += "{},quote={} price={},size={} {}\n".format(ticker, quote, price, size, str(d))

    res = httpsession.post(influx_host, data=lp_post)

    if res.status_code != 204:
        logger.error('ERROR, CODE {} WHEN WRITING TO INFLUXDB FOR SYMBOL {} AND QUOTE {}.'.format(str(res.status_code), ticker, quote))
    # sleep(0.1)

    logger.info('{}-{} SERIES WRITTEN TO INFLUX AT {}.'.format(ticker, 'coint', str(dt.now())))

if __name__ == '__main__':
    adf_values = []
    pvalues = []
    db = InfluxDBClient('104.248.41.39', 8086, 'admin', 'jndm4jr5jndm4jr6', 'darwinex')

    tickers = ['EURUSD', 'EURGBP']
    frequencies = ['1s', '30s', '1m', '5m', '15m']

    for ticker in tickers:
        # start = dt(2018,12,day,9,0)
        period = timedelta(hours=24)
        start = find_first_timestamp(ticker, 'ask', influx_client=db) - period
        finish = find_last_timestamp(ticker, 'ask', influx_client=db)


        for indep in tickers:
            if ticker == indep:
                continue
            for freq in frequencies:
                while start < finish:
                    try:
                        start += period + timedelta(milliseconds=1)
                        end = start + period
                        start_epoch = int(float(start.timestamp())) * 1000 * 1000 * 1000
                        end_epoch = int(end.timestamp()) * 1000 * 1000 * 1000  ## must be in ns

                        dep = ticker
                        result = list(db.query("Select last(price) from {} where time > {} and time < {} group by time({})".format(dep, str(start_epoch), str(end_epoch), freq)))[0]
                        # print(len(result))
                        # print(result[0],result[1])
                        dep_df = influx_to_pandas(result)

                        result = list(db.query("Select last(price) from {} where time > {} and time < {} group by time({})".format(indep, str(start_epoch), str(end_epoch), freq)))[0]
                        # print(len(result))
                        # print(result[0],result[1])
                        indep_df = influx_to_pandas(result)
                        print(indep_df.head())

                        adf = cointegration(dep_df['last'], indep_df['last'])
                        print(adf)
                        adf_values.append(float(adf[0]))
                        pvalues.append(float(adf[1]))
                    except Exception as e:
                        print(e)

    print("Mean adf value is {} and mean p-value is {}.".format(str(sum(adf_values)/len(adf_values)), str(sum(pvalues)/len(pvalues))))