""" Check recent crypto trades, store coin info in df, move to database on completion """
import ccxt

# Use ccxt library to get market data from diff exchanges, start with coinbase and binance
print(ccxt.exchanges)

