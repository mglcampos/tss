
import pytz
from datetime import datetime
from zipline.algorithm import TradingAlgorithm
from zipline.api import order, record, symbol, order_target
import matplotlib.pyplot as plt
import pandas as pd

# Load data manually csv
#Date,Open,High,Low,Close,Volume,Adj Close
#1984-09-07,26.5,26.87,26.25,26.5,2981600,3.02
#...
parse = lambda x: pytz.utc.localize(datetime.strptime(x, '%Y-%m-%d'))
# data=pd.read_csv('histdata/crypto/ETCUSD_2018_M1M2.csv', parse_dates=['Date'], index_col=0,date_parser=parse)
#data = pd.read_csv('histdata/crypto/ETCUSD_2018_M1M2.csv', names = ["Datetime", "High", "Low", "Open", "Close", "Volume", "QuoteVolume", "WeightedAverage"], parse_dates=['Datetime'], index_col=0,date_parser=parse)
data = pd.read_csv('histdata/stocks_psi_geral/son-pl_daily_01-03-2000_11-02-2018.csv', names = ["open","high","low","close","date"], parse_dates=['date'], index_col=4, date_parser=parse, skiprows=1)
##data.index = data['date']
## todo fix benchmark problem, provide psi20 ?? no benchmark??
print(data.head())

# Define algorithm
def initialize(context):
    pass

def handle_data(context, data):
    # order('Close', 10)
    # record(ETCUSD=data['Close'])

    # Skip first 300 days to get full windows
    context.i += 1
    if context.i < 300:
        return

    # Compute averages
    # data.history() has to be called with the same params
    # from above and returns a pandas dataframe.
    short_mavg = data['close'].mean()
    long_mavg = data['close'].mean()

    # Trading logic
    if short_mavg > long_mavg:
        # order_target orders as many shares as needed to
        # achieve the desired number of shares.
        order_target(context.asset, 100)
    elif short_mavg < long_mavg:
        order_target(context.asset, 0)

    # Save values for later inspection
    record(EDP=data['close'],
           short_mavg=short_mavg,
           long_mavg=long_mavg)

def analyze(context, perf):
    fig = plt.figure()
    ax1 = fig.add_subplot(211)
    perf.portfolio_value.plot(ax=ax1)
    ax1.set_ylabel('portfolio value in $')

    ax2 = fig.add_subplot(212)
    perf['ETCUSD'].plot(ax=ax2)
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


# Create algorithm object passing in initialize and
# handle_data functions
algo_obj = TradingAlgorithm(initialize=initialize,
                            handle_data=handle_data, data_frequency='minute')

# Run algorithm
perf_manual = algo_obj.run(data)


# Print
perf_manual.to_csv('output.csv')

print("total pnl : " + str(float(perf_manual[["PnL"]].iloc[-1])))
buy_trade = perf_manual[["status"]].loc[perf_manual["status"] == "buy"].count()
sell_trade = perf_manual[["status"]].loc[perf_manual["status"] == "sell"].count()
total_trade = buy_trade + sell_trade
print("buy trade : " + str(int(buy_trade)) + " sell trade : " + str(int(sell_trade)) + " total trade : " + str(int(total_trade)))

