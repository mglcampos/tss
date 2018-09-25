
## alpha
# In short, an alpha of 1.0 means the mutual fund or investment outperformed its benchmark index by 1 percent.
# Conversely, an alpha of -1.0 means the mutual fund or investment underperformed its benchmark index by 1%.
## beta
# A beta of less than 1 means that the security will be less volatile than the market.
# A beta of greater than 1 indicates that the security's price will be more volatile than the market. If a stock's beta is 1.5, it's determined to be 50% more volatile than the overall market.


import pandas as pd

perf = pd.read_pickle('dma.pickle') # read in perf DataFrame
print(perf['AAPL'].sample(20))
print(perf.head())