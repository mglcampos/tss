
import bt
import pandas as pd
from algos import WeighTarget
class Backtest():

    def __init__(self, tickers=None):

        if tickers == None:
            self.ticker_list = self._retrieve_tickers()
        else:
            self.ticker_list = tickers

        self.data = self._load_data(self.ticker_list)

    def _retrieve_tickers(self):
        with open("links", 'r') as f:
            tmp = []
            for row in f:
                if not row.split(".com/")[1].replace("\n", "") in tmp:
                    tmp.append(row.split(".com/")[1].replace("\n", ""))
        return tmp

    def _load_data(self, tickers):

        data = pd.DataFrame()
        comb_index = None
        for ticker in tickers:
            df = pd.read_csv('histdata/stocks_psi_geral/minimal/{}_daily_minimal.csv'.format(ticker.replace(':','-')),
                           parse_dates=True, index_col=0)
            df = df[df.index > '2017-01-01']
            df.index.name = 'Date'
            #df.columns = [ticker.split(':')[0]]
            df = df.iloc[::-1]
            if comb_index is None:
                comb_index = df.index
                data = df
            else:
                comb_index.union(df.index)

            data[ticker] = df
            data = data.reindex(index=comb_index, method='pad')
            data.fillna(inplace=True, method='ffill')
        # print(data.head())
        # print(comb_index)

        # data.plot()

        return data

    def above_sma(self, tickers, sma_per=50, start='2010-01-01', name='above_sma'):
        """
        Long securities that are above their n period
        Simple Moving Averages with equal weights.
        """
        # download data
        # Guarantee all data iterates over the same index.


        # calc sma
        sma9 = self.data.rolling(9).mean()
        sma21 = self.data.rolling(21).mean()
        sma100 = self.data.rolling(100).mean()
        from algos import SelectWhere
        # create strategy
        s = bt.Strategy(name, [SelectWhere((self.data > sma100)),
                               bt.algos.WeighEqually(),
                               bt.algos.Rebalance()])

        # now we create the backtest
        return bt.Backtest(s, self.data)

    def ma_cross(self, ticker, start='2010-01-01',
                 short_ma=50, long_ma=200, name='ma_cross'):
        # these are all the same steps as above
        #
        print(self.data.head())
        print(self.data[ticker].head())
        short_sma = self.data[ticker].rolling(short_ma).mean()
        long_sma = self.data[ticker].rolling(long_ma).mean()
        # self.data[ticker].columns = [ticker]
        # print(self.data[ticker].columns)
        # target weights
        tw = long_sma.copy()
        tw[short_sma > long_sma] = 1.0
        tw[short_sma <= long_sma] = -1.0
        tw[long_sma.isnull()] = 0.0

        # here we specify the children (3rd) arguemnt to make sure the strategy
        # has the proper universe. This is necessary in strategies of strategies
        s = bt.Strategy(name, [WeighTarget(tw), bt.algos.Rebalance()], [ticker])
        # tmp = pd.DataFrame()
        # tmp = self.data[ticker]
        # print(ticker)
        #
        # tmp.columns = ['Date', ticker]
        # print(tmp.columns)
        # print(tmp.head())
        return bt.Backtest(s, self.data)

    def run(self, selected_tickers, start=None, end=None):

        t1 = self.ma_cross('galp:pl', name='galp_ma_cross')
        t2 = self.ma_cross('bcp:pl', name='bcp_ma_cross')

        # let's run these strategies now
        res = bt.run(t1, t2)
        print(res.display())
        print(res.get_transactions())

if __name__ == "__main__":
    test = Backtest(tickers=['galp:pl','bcp:pl'])

    res = test.run(['galp:pl'])
    print(res.display())
    res.plot()