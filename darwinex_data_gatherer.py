
from darwinex_data import DWX_Tick_Data


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

    # files = dwt.walk_dir()
    # for ticker in tickers:
    #     print(files[ticker])
    #     for file in files[ticker]:
    #         df = dwt._download_hour_(_asset=ticker,
    #                             _ftp_loc_format=ticker+'/'+file,
    #                             _verbose=True)
    #         print(df.head())

    file = 'AUDCAD_ASK_2017-10-01_22.log.gz'
    ticker = 'AUDCAD'
    date = '2017-10-05'
    hour='22'
    df = dwt._download_hour_(_asset=ticker, _date=date, _hour=hour,
                                        _ftp_loc_format='{}/{}_ASK_{}_{}.log.gz',
                                        _verbose=True)
    print(len(df))
    print(df.head())