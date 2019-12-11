from paprika.data.data_channel import DataChannel
from paprika.data.data_type import DataType


def test_data_channel_fetch():
    DataChannel.clear_all_feeds()

    """
    So there will be two API in the DataChannel for fetch data. 
    DataChannel.fetch and DataChannel.fetch_price.
    
    Tips:
    1. symbols could be exact symbol or pattern list.

    def fetch(symbols: List[str],
            data_type: Optional[DataType] = DataType.CANDLE,
            frequency: Optional[str] = '1D',
            timestamp: Optional[Union[int, datetime]] = None,
            start: Optional[Union[int, datetime]] = None,
            end: Optional[Union[int, datetime]] = None,
            time_span: Optional[int] = 1,
            fields: Optional[List[str]] = [],
            arctic_sources: Optional[Tuple[str]] = (PERMANENT_ARCTIC_SOURCE_NAME,),
            arctic_host: Optional[str] = DEFAULT_ARCTIC_HOST
            ):
      
    def fetch_price(symbols: List[str],
                    timestamp: Union[int, datetime],
                    data_type: Optional[DataType] = DataType.CANDLE,
                    frequency: Optional[str] = '1D',
                    time_span: Optional[int] = 1,
                    arctic_sources: Optional[Tuple[str]] = (PERMANENT_ARCTIC_SOURCE_NAME,),
                    arctic_host: Optional[str] = DEFAULT_ARCTIC_HOST
                    ):
    """

    symbol_patterns = ['SP500.A.*']
    symbols = DataChannel.check_register(symbol_patterns)
    df = DataChannel.fetch(symbol_patterns, data_type=DataType.CANDLE, frequency='1D')
    print(df.head())
    print(df.tail())
    df2 = DataChannel.fetch(symbol_patterns, data_type=DataType.CANDLE, frequency='1D', timestamp=df.index[-100][1])
    print(df2)
    df3 = DataChannel.fetch_price(symbol_patterns, timestamp=df.index[-100][1], frequency='1D')
    print(df3)

    symbol_patterns = ['ETF.XS.*']
    symbols = DataChannel.check_register(symbol_patterns)
    df = DataChannel.fetch(symbol_patterns, data_type=DataType.TRADES)
    print(df.head())
    print(df.tail())
    df2 = DataChannel.fetch(symbol_patterns, timestamp=df.index[-100][1], data_type=DataType.TRADES)
    print(df2)
    df3 = DataChannel.fetch_price(symbol_patterns, timestamp=df.index[-100][1])
    print(df3)

    symbol_patterns = ['EUX.FB.*']
    symbols = DataChannel.check_register(symbol_patterns)
    df = DataChannel.fetch(symbol_patterns, data_type=DataType.ORDERBOOK)
    print(df.head())
    print(df.tail())
    df2 = DataChannel.fetch(symbol_patterns, data_type=DataType.ORDERBOOK, timestamp=df.index[-100][1])
    print(df2)
    df3 = DataChannel.fetch_price(symbol_patterns, timestamp=df.index[-100][1])
    print(df3)

    print('ok')




