from paprika.data.data_utils import OHLCVAC_upload
from multiprocessing import Process

# not in a separate function because of the multiprocessing module on Windows

if __name__ == "__main__":
    tckrs = ['GLD', 'GDX', 'IGE', 'KO', 'OIH', 'PEP', 'RKH', 'RTH', 'SPY']
    resource_path = r'../../../resources/data/'
    ps = [Process(target=OHLCVAC_upload, args=(ticker, resource_path)) for ticker in tckrs]
    for p in ps:
        p.start()
    for p in ps:
        p.join()
