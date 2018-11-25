
import bt
import pandas as pd

class Backtest():

    def __init__(self):
        self.ticker_list = self._retrieve_tickers()
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
            df.columns = [ticker.split(':')[0]]
            df = df.iloc[::-1]
            if comb_index is None:
                comb_index = df.index
                data = df
            else:
                comb_index.union(df.index)

            data[ticker] = df
            data = data.reindex(index=comb_index, method='pad')
            data.fillna(inplace=True, method='ffill')
        print(data.head())
        # print(comb_index)

        data.plot()
        print(data)
        return data

    def above_sma(self, tickers, sma_per=50, start='2010-01-01', name='above_sma'):
        """
        Long securities that are above their n period
        Simple Moving Averages with equal weights.
        """
        # download data
        # Guarantee all data iterates over the same index.


        # calc sma
        sma = self.data.rolling(sma_per).mean()
        from algos import SelectWhere
        # create strategy
        s = bt.Strategy(name, [SelectWhere(self.data > sma),
                               bt.algos.WeighEqually(),
                               bt.algos.Rebalance()])

        # now we create the backtest
        return bt.Backtest(s, self.data)

    def run(self, selected_tickers, start=None, end=None):


        sma10 = self.above_sma(selected_tickers, sma_per=10, name='sma10')
        # sma20 = above_sma(tickers, sma_per=20, name='sma20')
        # sma40 = above_sma(tickers, sma_per=40, name='sma40')
        # benchmark = long_only_ew('spy', name='spy')

        # run all the backtests!
        res = bt.run(sma10)
        print(res.display())
        print(res.get_transactions())


if __name__ == "__main__":
    test = Backtest()

    test.run('galp')