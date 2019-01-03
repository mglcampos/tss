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
influx_host = 'http://104.248.41.39:8086/write?u={}&p?{}&db={}&u={}&p={}&rp={}&precision={}'.format(user, pswd, db,
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
    try:
        dt_ts = dt.strptime(timestamp[:-4] + 'Z', '%Y-%m-%dT%H:%M:%S.%fZ') ## no conversion available to ns, reduce to us
    except:
        dt_ts = dt.strptime(timestamp[:-4] + 'Z',
                            '%Y-%m-%dT%H:%M:%S.Z')  ## no conversion available to ns, reduce to us
    return dt_ts

def find_last_timestamp(ticker, quote, influx_client=None):
    """Finds the most recent point written and its timestamp."""
    if influx_client is None:
        influx_client = InfluxDBClient('104.248.41.39', 8086, 'admin', 'jndm4jr5jndm4jr6', 'darwinex')
    timestamp = list(influx_client.query("Select last(price) from {} where quote='{}'".format(ticker, quote)))[0][0]['time']
    try:
        dt_ts = dt.strptime(timestamp[:-4] + 'Z', '%Y-%m-%dT%H:%M:%S.%fZ') ## no conversion available to ns, reduce to us
    except:
        dt_ts = dt.strptime(timestamp[:-4] + 'Z',
                            '%Y-%m-%dT%H:%M:%S.Z')  ## no conversion available to ns, reduce to us

    return dt_ts

def write_coint_to_influx(adf, ticker, indep, freq, ts):
    """."""

    lp_post = ''
    critical_value = adf[0]
    p_value = adf[1]
    measurement = ticker + '.' + indep

    lp_post += "{},coint={},freq={] value={},p_value={} {}".format(measurement, 'adf',freq, critical_value, p_value, str(ts))
    # print(lp_post)
    res = httpsession.post(influx_host, data=lp_post)

    if res.status_code != 204:
        logger.error('ERROR, CODE {} WHEN WRITING COINT TO INFLUXDB FOR SYMBOL {}.'.format(str(res.status_code), ticker))
        raise Exception(str(res.status_code))
    # sleep(0.1)

    logger.info('{}-{} COINT SERIES WRITTEN TO INFLUX AT {}.'.format(ticker, 'coint', str(dt.now())))

if __name__ == '__main__':

    db = InfluxDBClient('104.248.41.39', 8086, 'admin', 'jndm4jr5jndm4jr6', 'darwinex')
    tickers = ['AUDCAD', 'AUDCHF', 'AUDJPY', 'AUDNZD', 'AUDUSD', 'AUS200',
               'CADCHF', 'CADJPY', 'CHFJPY', 'EURAUD', 'EURCAD', 'EURCHF',
               'EURGBP', 'EURJPY', 'EURMXN', 'EURNOK', 'EURNZD', 'EURSEK',
               'EURSGD', 'EURTRY', 'EURUSD', 'FCHI', 'GBPAUD', 'GBPCAD', 'GBPCHF',
               'GBPJPY', 'GBPMXN', 'GBPNOK', 'GBPNZD', 'GBPSEK', 'GBPTRY',
               'GBPUSD', 'GDAXIm', 'J225', 'NDXm', 'NZDCAD',
               'NZDCHF', 'NZDJPY', 'NZDUSD', 'SPN35', 'SPXm', 'STOXX50E', 'UK100',
               'USDCAD', 'USDCHF', 'USDHKD', 'USDJPY', 'USDMXN', 'USDNOK',
               'USDSEK', 'USDSGD', 'USDTRY', 'WS30', 'WS30m', 'XAGUSD', 'XAUUSD',
               'XBNUSD', 'XBTUSD', 'XETUSD', 'XLCUSD', 'XNGUSD', 'XPDUSD',
               'XPTUSD', 'XRPUSD', 'XTIUSD']
    # tickers = ['EURUSD', 'EURGBP']
    frequencies = ['5s', '15s', '30s', '1m', '5m', '15m']
    factors = [1,3,2,2,5,3]

    logger.info("TICKERS: {}".format(tickers))
    logger.info("FREQUENCIES: {}".format(frequencies))
    for ticker in tickers:
        adf_values = []
        pvalues = []
        for indep in tickers:
            if ticker == indep:
                continue
            for i, freq in enumerate(frequencies):
                # start = dt(2018,12,day,9,0)
                period = timedelta(hours=24*factors[i])
                start = find_first_timestamp(indep, 'ask', influx_client=db) - period
                finish = find_last_timestamp(indep, 'ask', influx_client=db)
                while start < finish:
                    try:
                        start += period + timedelta(milliseconds=1)
                        end = start + period
                        start_epoch = int(float(start.timestamp())) * 1000 * 1000 * 1000
                        end_epoch = int(end.timestamp()) * 1000 * 1000 * 1000  ## must be in ns
                        print('#### STARTING FOR FREQ: {}, START: {}, DEP: {} AND INDEP: {}. ####'.format(freq, start, ticker, indep))
                        logger.info('#### STARTING FOR FREQ: {}, START: {}, DEP: {} AND INDEP: {}. ####'.format(freq, start, ticker,
                                                                                                indep))
                        dep = ticker
                        result = list(db.query("Select last(price) from {} where time > {} and time < {} group by time({})".format(dep, str(start_epoch), str(end_epoch), freq)))[0]
                        # print(len(result))
                        # print(result[0],result[1])
                        dep_df = influx_to_pandas(result)

                        result = list(db.query("Select last(price) from {} where time > {} and time < {} group by time({})".format(indep, str(start_epoch), str(end_epoch), freq)))[0]
                        print(len(result))
                        logger.info("SAMPLE SIZE: {}".format(str(len(result))))
                        # print(result[0],result[1])
                        indep_df = influx_to_pandas(result)
                        # print(indep_df.head())

                        adf = cointegration(dep_df['last'], indep_df['last']) ## todo column name is last because agg function is last
                        try:
                            write_coint_to_influx(adf, ticker, indep, freq, end_epoch)
                        except Exception as e:
                            logger.error(e)
                            logger.error('COULDNT WRITE COINT TO INFLUX FOR FREQ: {}, START: {}, DEP: {} AND INDEP: {}.'.format(freq, start, ticker, indep))

                        print(adf)
                        logger.info("ADF: {}".format(str(adf)))

                        adf_values.append(float(adf[0]))
                        pvalues.append(float(adf[1]))
                    except Exception as e:
                        print(e)
                        logger.error(e)

        print("Mean adf value for {} and {} is {} and mean p-value is {}.".format(ticker, indep, str(sum(adf_values)/len(adf_values)), str(sum(pvalues)/len(pvalues))))
        logger.info("\nMean adf value for {} and {} is {} and mean p-value is {}.\n".format(ticker, indep, str(sum(adf_values)/len(adf_values)), str(sum(pvalues)/len(pvalues))))