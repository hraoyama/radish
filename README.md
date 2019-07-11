# radish
Backtest platform

## API:

    register_timer(handler, frequency)
register a function called in a frequency
    
    get_ohlcv(source, symbol, frequency, fields, end, limit)
fetch ohlcv 

    get_current_frame_timestamp()
get current timestamp
    
    get_markets(source)
fetch symbols in current source

    get_orderbook(source, symbol, depth, end):
fetch orderbook

    get_tickers(source)
fetch all tickers in current source

    get_portfolio(source, account_type)
get current portfolio. Default account type is spot.

    place_order(source, order, account_type)
place a order to execute. Default account type is spot.

    get_my_trades(source, symbol, since, account_type)
fetch past trades

    order_target_percents(source, percents, order_type, account_type)
place orders for multi assets with percents to whole portfolio
percents are list of assets to their percents. 
Default order type is market order, and account type is spot.
    
    order_target_percent(source, asset, percent, order_type,account_type)
place order for one asset with percent to whole portfolio
Default order type is market order, and account type is spot.

    get_open_orders(source, symbol, account_type)
Fetch open orders in current source. Default account type is spot,
This will be useful in limit orders. 

    cancel_order(source, order, account_type)
Cancel open orders in current source. Default account type is spot,
This will be useful in limit orders. 
   




 