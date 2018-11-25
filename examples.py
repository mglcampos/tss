##zipline --start 2014-1-1 --end 2018-1-1 -o dma.pickle
# bundle.asset_finder.retrieve_all(bundle.asset_finder.sids)
from zipline.data import bundles
import pandas as pd
import pytz
from zipline import TradingAlgorithm
# bundle = bundles.load('quantopian-quandl')
# print(bundle.asset_finder.retrieve_all(bundle.asset_finder.sids))

from zipline.api import order_target, record, symbol
import matplotlib.pyplot as plt

def initialize(context):
    context.i = 0
    context.asset = symbol('EDP')


def handle_data(context, data):
    # Skip first 300 days to get full windows
    context.i += 1
    if context.i < 300:
        return

    # Compute averages
    # data.history() has to be called with the same params
    # from above and returns a pandas dataframe.
    short_mavg = data.history(context.asset, 'price', bar_count=100, frequency="1d").mean()
    long_mavg = data.history(context.asset, 'price', bar_count=300, frequency="1d").mean()

    # Trading logic
    if short_mavg > long_mavg:
        # order_target orders as many shares as needed to
        # achieve the desired number of shares.
        order_target(context.asset, 100)
    elif short_mavg < long_mavg:
        order_target(context.asset, 0)

    # Save values for later inspection
    record(AAPL=data.current(context.asset, 'price'),
           short_mavg=short_mavg,
           long_mavg=long_mavg)


def analyze(context, perf):
    fig = plt.figure()
    ax1 = fig.add_subplot(211)
    perf.portfolio_value.plot(ax=ax1)
    ax1.set_ylabel('portfolio value in $')

    ax2 = fig.add_subplot(212)
    perf['AAPL'].plot(ax=ax2)
    perf[['short_mavg', 'long_mavg']].plot(ax=ax2)

    perf_trans = perf.ix[[t != [] for t in perf.transactions]]
    buys = perf_trans.ix[[t[0]['amount'] > 0 for t in perf_trans.transactions]]
    sells = perf_trans.ix[
        [t[0]['amount'] < 0 for t in perf_trans.transactions]]
    ax2.plot(buys.index, perf.short_mavg.ix[buys.index],
             '^', markersize=10, color='m')
    ax2.plot(sells.index, perf.short_mavg.ix[sells.index],
             'v', markersize=10, color='k')
    ax2.set_ylabel('price in $')
    plt.legend(loc=0)
    plt.show()



data = pd.read_csv('C:\\Users\\utilizador\PycharmProjects\\tss\histdata\stocks_psi_geral\edp-pl_daily_06-20-1997_11-02-2018.csv')
# panel = pd.Panel(data)
# panel.minor_axis = ['Open', 'High', 'Low', 'Close']
# panel.major_axis = panel.major_axis.tz_localize(pytz.utc)

#initializing trading enviroment
algo_obj = TradingAlgorithm(initialize=initialize, handle_data=handle_data, capital_base = 100000.0)
#run algo
perf_manual = algo_obj.run(data)

print("total pnl : " + str(float(perf_manual[["PnL"]].iloc[-1])))
buy_trade = perf_manual[["status"]].loc[perf_manual["status"] == "buy"].count()
sell_trade = perf_manual[["status"]].loc[perf_manual["status"] == "sell"].count()
total_trade = buy_trade + sell_trade
print("buy trade : " + str(int(buy_trade)) + " sell trade : " + str(int(sell_trade)) + " total trade : " + str(int(total_trade)))