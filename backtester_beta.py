
from datetime import datetime
import backtrader as bt
import backtrader.feeds as btfeed
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo

class TECSVData(btfeed.GenericCSVData):
    params = (
        ('nullvalue', float('NaN')),
        ('dtformat', '%Y-%m-%d'),
        ('tmformat', '%H:%M:%S'),

        ('datetime', 4),
        ('time', -1),
        ('open', 0),
        ('high', 1),
        ('low', 2),
        ('close', 3),
        ('volume', -1),
        ('openinterest', -1),
    )

class SmaCross(bt.SignalStrategy):
    def __init__(self):
        sma1, sma2 = bt.ind.SMA(period=10), bt.ind.SMA(period=100)
        crossover = bt.ind.CrossOver(sma1, sma2)
        self.signal_add(bt.SIGNAL_LONG, crossover)

cerebro = bt.Cerebro()
cerebro.addstrategy(SmaCross)

# data0 = bt.feeds.YahooFinanceData(dataname='MSFT', fromdate=datetime(2011, 1, 1),
#                                   todate=datetime(2012, 12, 31))

data0 = TECSVData(dataname='C:\\Users\\utilizador\\PycharmProjects\\tss\\histdata\\stocks_psi_geral\\full\\edp-pl_daily_06-20-1997_11-02-2018.csv')

cerebro.adddata(data0)

cerebro.run()
# cerebro.plot(volume=False)
b = Bokeh(style='bar', plot_mode='single', scheme=Tradimo())
cerebro.plot(b, volume=False)