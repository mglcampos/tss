
from darwinex_data import DWX_Tick_Data
from requests import Session
import logging
from datetime import datetime as dt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='ftp-influxdb.log', filemode='w')
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

    lp_post = ''

    for row in df.itertuples():
        t = getattr(row, 'Index')
        price = getattr(row, 'ask')
        size = getattr(row, 'size')
        d = int(t.timestamp() * 1000 * 1000 * 1000)

        lp_post += "{},quote={} price={},size={} {}\n".format(ticker, quote, price, size, str(d))

    res = httpsession.post(influx_host, data=lp_post)

    if res.status_code != 204:
        logger.error('ERROR, CODE {} WHEN WRITING TO INFLUXDB FOR SYMBOL {} AND QUOTE {}.'.format(str(res.status_code), ticker, quote))
    # sleep(0.1)

    logger.info('{}-{} SERIES WRITTEN TO INFLUX AT {}.'.format(ticker, quote, str(dt.now())))

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
            if 'ASK' in file:
                quote = 'ask'
            else:
                quote = 'bid'
            logger.info('Downloading {}......'.format(file))
            print(file)
            df = dwt._download_hour_(_asset=ticker,
                                _ftp_loc_format=ticker+'/'+file,
                                _verbose=True)
            # print(df.head())
            try:
                write_tick_to_influx(df, quote, ticker)
            except Exception as e:
                logger.error(str(e))
                logger.error('COULDNT WRITE FILE {} TO INFLUX.'.format(file))
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