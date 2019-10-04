from paprika.exchange.data_processor import DataProcessor
from paprika.data.data_channel import DataChannel


def get_return_series(ticker, data_type, between_times=None, time_freq=None,
                      extract_returns_args=("Price", "LOG_RETURN", "LogReturn_Px")):
    data_name = DataChannel.name_to_data_type(ticker, data_type)
    process_series = None
    if data_name in DataChannel.table_names(arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME):
        process_series = DataChannel.download(data_name,
                                              arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME,
                                              use_redis=True)
        if process_series is not None:
            process_series = DataProcessor(process_series)
        else:
            raise ValueError(
                f'Unable to retrieve data for {data_name} from Redis or from {DataChannel.PERMANENT_ARCTIC_SOURCE_NAME}')
    
    if between_times is not None:
        process_series = process_series.between_time(between_times[0], between_times[1])
    
    if time_freq is not None:
        process_series = process_series.time_freq(time_freq)
    
    process_series = process_series.positive_price()
    if extract_returns_args is not None:
        process_series = process_series.extract_returns(extract_returns_args[0], extract_returns_args[1],
                                                        extract_returns_args[2])
    
    return process_series.data
