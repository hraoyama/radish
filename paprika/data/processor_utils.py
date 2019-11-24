from paprika.data.data_processor import DataProcessor
from paprika.data.data_channel import DataChannel

import pandas as pd


def merge_data_frames_in_dict_values(input_dict):
    merged_data = None
    for index, val in enumerate(input_dict.items()):
        if index == 0:
            merged_data = val[1]
        else:
            merged_data = pd.merge(merged_data, val[1], how='outer', left_index=True, right_index=True)
    
    return merged_data


def get_return_series(ticker, data_type,
                      between_times=None, time_freq=None,
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
    
    # process_series = process_series.positive_price()
    if extract_returns_args is not None:
        if len(extract_returns_args) == 2:
            extract_returns_args.append(extract_returns_args[0])
        process_series = process_series.extract_returns(extract_returns_args[0], extract_returns_args[1],
                                                        extract_returns_args[2])
        return process_series.data[[extract_returns_args[2]]]
    else:
        raise ValueError(f'extract_returns_args must be specified for get_return_series to return the correct column')
