
import statsmodels.api as sm
import statsmodels.tsa.stattools as ts
from influxdb import InfluxDBClient
from datetime import datetime as dt
from datetime import timedelta
import pandas as pd

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
        beta_hr = sm.OLS(df1.values, df2.values).fit().params[0]
        res = df1.values - beta_hr * df2.values
        # Calculate and reports the CADF test on the residuals
        adf = ts.adfuller(res.flatten())
        return engle_granger, adf
    except Exception as e:
        print(e)
        return 69, 69

def influx_to_pandas(result):
    df = pd.DataFrame(result)
    df.index = df['time']

    return df.drop(['time'], axis=1)

if __name__ == '__main__':
    adf_values = []
    pvalues = []
    for day in range(1,28):
        try:
            start = dt(2018,12,day,9,0)
            period = timedelta(hours=24)
            end = start + period
            start_epoch = int(float(start.timestamp())) * 1000 * 1000 * 1000
            end_epoch = int(end.timestamp()) * 1000 * 1000 * 1000 ## must be in ns

            db = InfluxDBClient('104.248.41.39', 8086, 'admin', 'jndm4jr5jndm4jr6', 'darwinex')

            dep = 'EURNOK'
            result = list(db.query("Select last(price) from {} where time > {} and time < {} group by time(1s)".format(dep, str(start_epoch), str(end_epoch))))[0]
            # print(len(result))
            # print(result[0],result[1])
            dep_df = influx_to_pandas(result)

            indep = 'EURSEK'
            result = list(db.query("Select last(price) from {} where time > {} and time < {} group by time(1s)".format(indep, str(start_epoch), str(end_epoch))))[0]
            # print(len(result))
            # print(result[0],result[1])
            indep_df = influx_to_pandas(result)
            # print(indep_df.head())

            engle, adf = cointegration(dep_df['last'], indep_df['last'])
            print(adf)
            adf_values.append(float(adf[0]))
            pvalues.append(float(adf[1]))
        except Exception as e:
            print(e)

    print("Mean adf value is {} and mean p-value is {}.".format(str(sum(adf_values)/len(adf_values)), str(sum(pvalues)/len(pvalues))))