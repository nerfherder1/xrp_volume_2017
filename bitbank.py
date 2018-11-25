import pandas as pd
import re
pd.core.common.is_list_like = pd.api.types.is_list_like
import datetime
from pprint import pprint
import inspect
import time



#ce5ae250-38ea-481d-8867-7d3e375a8176

def appendDFToCSV_void(df, csvFilePath, sep=","):
    import os
    if not os.path.isfile(csvFilePath):
        df.to_csv(csvFilePath, mode='a', index=False, sep=sep)
    elif len(df.columns) != len(pd.read_csv(csvFilePath, nrows=1, sep=sep, ).columns):
        raise Exception(
            "Columns do not match!! Dataframe has " + str(len(df.columns)) + " columns. CSV file has " + str(
                len(pd.read_csv(csvFilePath, nrows=1, sep=sep).columns)) + " columns.")
    elif not (df.columns == pd.read_csv(csvFilePath, nrows=1, sep=sep).columns).all():
        raise Exception("Columns and column order of dataframe and csv file do not match!!")
    else:
        df.to_csv(csvFilePath, mode='a', index=False, sep=sep, header=False)
        print("Appended entries... ")


def poloniex_data(symbol='XRPJPY', timeframe="1day"):  # Poloneix  -  Params:  int frequency = 300,900,1800,7200,14400,86400 (Mins = 5,15,30 Hrs = 2,4,24)
    from pandas.io.json import json_normalize
    # EPOCH = 0 : INDICATES FROM BEGINNING OF TIME.
    print("\n")
    print("STARTING POLONIEX")
    print("\n")
    pulled = False  # Defaults as false - Forces loop until successful pull from Bittrex
    while not pulled:
        try:
            import ccxt
            bitbank = ccxt.bitbank()
            markets = bitbank.load_markets()
            #pprint(markets)
            bitbank.verbose = True
            #url = bitbank.fetch_ohlcv(symbol='XRP/JPY',timeframe='1d',limit=None,)
            #pprint(url)
            url = 'https://public.bitbank.cc/xrp_jpy/candlestick/1day/2018'
            print("Pulled Data Successfully \n")
            #{'type': '1day', 'ohlcv':
            ohlcv = pd.read_json(url)['data']['candlestick'][0]['ohlcv']
            #pprint(ohlcv)
            columns = ['Open', 'high', 'low', 'close', 'volume','UnixTime']
            ohlcv = pd.DataFrame(ohlcv,columns=columns)
            ohlcv['UnixTime'] = pd.to_datetime(ohlcv['UnixTime'], unit='ms')
            pprint(ohlcv)
            print("Data Pulled: " + url)
            #print(ohlcv.tail(n=1))
            ########### END - PULL HISTORICAL DATA FROM EXCHANGE API ##############################
            pulled = True  # Exit while loop
        except Exception:  # If Pull fails it will loop back to top
            print("PULL ERROR | {} : Could not pull {} dataframe from {} at {}".format(inspect.stack()[0][3],
                                                                                       symbol,
                                                                                       inspect.stack()[0][3],
                                                                                       timeframe))
            print("Will try again ... ")
            time.sleep(3)
            pass
    ########### APPEND DATAFRAME ENTRIES TO CSV ###################################
    AppendNewData = ohlcv
    symbol = re.sub('/', '', symbol)  # REMOVE ILLEGAL CHARACTERS FROM FILENAME
    print("preparing to save file to : " + str(symbol) + "_" + str(timeframe) + "_BITBANK_2018.csv")
    appendDFToCSV_void(AppendNewData, str(symbol) + "_" + str(timeframe) + "_BITBANK_2018.csv"
                            ,",")  # XRPUSD_INTERVAL_KRAKEN.CSV
    # AFTER APPENDING THEN YOU CAN MAKE DATE AS INDEX.
    AppendNewData.set_index('UnixTime', inplace=True)
    #print(AppendNewData.head(n=1))
    ########### END - APPEND DATAFRAME ENTRIES TO CSV ###################################
    return AppendNewData

df = poloniex_data()
