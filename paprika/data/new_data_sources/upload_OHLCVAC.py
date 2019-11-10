from paprika.data.data_channel import DataChannel
from paprika.data.data_utils import OHLCVAC_upload
from multiprocessing import Process

# not in a separate function because of the multiprocessing module on Windows
if __name__ == "__main__":
    
    tickers = ['GLD', 'GDX', 'IGE', 'KO', 'OIH', 'PEP', 'RKH', 'RTH', 'SPY', 'GLD2', 'USO']
    existing_table_names = DataChannel.check_register([x + ".OHLCVAC_PRICE" for x in tickers], feeds_db=False)
    for table_to_remove in existing_table_names:
        DataChannel.delete_table(table_to_remove, arctic_source_name=DataChannel.PERMANENT_ARCTIC_SOURCE_NAME)
        assert table_to_remove not in DataChannel.check_register([table_to_remove], feeds_db=False)
    
    resource_path = r'../../../resources/data/'
    ps = [Process(target=OHLCVAC_upload, args=(ticker, resource_path)) for ticker in tickers]
    for p in ps:
        p.start()
    for p in ps:
        p.join()
