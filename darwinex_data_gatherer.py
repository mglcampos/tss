
from darwinex_data import DWX_Tick_Data
from requests import Session
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='opentsdb-influxdb.txt', filemode='w')
logger = logging.getLogger()
httpsession = Session()
## Influx
user = 'root'
pswd = 'root'
rp = 'autogen'
precision = 'ns'
db = 'darwinex'
influx_host = 'http://localhost:8086/write?u={}&p?{}&db={}&u={}&p={}&rp={}&precision={}'.format(user, pswd, db,
                                                                                                user, pswd, rp, precision)

def write_tick_to_influx(df, quote, ticker):
    """."""

    lp = ''
    source = 'IB'
    s = 'IBUS30-2'

    for t in df.index:
        d = (t - dt(1970,1,1)).total_seconds()
        if len(str(timestamp)) == 10:
            d = d * 1000 + 1
        p = ask[timestamp]
        ask_data_string1 += "{},source={},quote={},symbol={} value={} {}\n".format('markets', source, q, s, str(float(p)), str(d))
        ask_data_string2 += "{},source={},symbol={} value={} {}\n".format(q, source, s, str(float(p)), str(d))
        ask_data_string3 += "{},source={},quote={} value={} {}\n".format(s, source, q, str(float(p)), str(d))

    res = httpsession.post(url_string, data=ask_data_string1)

    if res.status_code != 204:
        ##todo change quote to be dynamic
        logger.error('ERROR WRITING TO INFLUXDB FOR SYMBOL {}, SOURCE {} AND QUOTE {}.'.format(s, source, 'bid'))
    # sleep(0.1)

    logger.info('BID SERIES WRITTEN TO INFLUX AT {}.'.format(dt.now()))

if __name__ == '__main__':

    dwt = DWX_Tick_Data(dwx_ftp_user='mglcampos',
                     dwx_ftp_pass='2a8Ic7yydUVVKB', dwx_ftp_hostname='tickdata.darwinex.com')

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

    files = dwt.walk_dir()
    for ticker in tickers:
        print(files[ticker])
        for file in files[ticker]:
            print(file)
            df = dwt._download_hour_(_asset=ticker,
                                _ftp_loc_format=ticker+'/'+file,
                                _verbose=True)
            print(df.head())
        break



    # file = 'AUDCAD_ASK_2017-10-01_22.log.gz'
    # ticker = 'AUDCAD'
    # date = '2017-10-05'
    # hour='22'
    # df = dwt._download_hour_(_asset=ticker, _date=date, _hour=hour,
    #                                     _ftp_loc_format='{}/{}_ASK_{}_{}.log.gz',
    #                                     _verbose=True)
    # print(len(df))
    # print(df.head())