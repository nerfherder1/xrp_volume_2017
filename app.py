import pandas as pd
import re
pd.core.common.is_list_like = pd.api.types.is_list_like
import datetime
from pprint import pprint
import inspect
import time


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


def poloniex_data(symbol='USDT_XRP', timeframe=300, epochend=1515081600
                  ,epochstart=1512864000):  # Poloneix  -  Params:  int frequency = 300,900,1800,7200,14400,86400 (Mins = 5,15,30 Hrs = 2,4,24)
    # EPOCH = 0 : INDICATES FROM BEGINNING OF TIME.
    print("\n")
    print("STARTING POLONIEX")
    print("\n")
    pulled = False  # Defaults as false - Forces loop until successful pull from Bittrex
    while not pulled:
        try:
            url = 'https://poloniex.com/public?command=returnChartData&currencyPair=' + symbol + '&end='+str(epochend)+'&period=' + str \
                (timeframe) + '&start=' + str(epochstart)
            print("Pulled Data Successfully \n")
            ohlcv = pd.read_json(url)
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
    print("preparing to save file to : " + str(symbol) + "_" + str(timeframe) + "_POLONIEX.csv")
    appendDFToCSV_void(AppendNewData, str(symbol) + "_" + str(timeframe) + "_POLONIEX.csv"
                            ,",")  # XRPUSD_INTERVAL_KRAKEN.CSV
    # AFTER APPENDING THEN YOU CAN MAKE DATE AS INDEX.
    AppendNewData.set_index('date', inplace=True)
    #print(AppendNewData.head(n=1))
    ########### END - APPEND DATAFRAME ENTRIES TO CSV ###################################
    return AppendNewData

df = poloniex_data()
date = df.index
high = df['high']
vol = df['volume']
df['dollars'] = high*vol
max_high = round(max(high),2)
intervals = df.shape[0]

print("\n\n5 MIN INTERVALS FROM BASE TO PEAK OF RUN : ",intervals)

sum_dollars = df['dollars'].sum()
print("##########################################################")
print("##########################################################")
print("    Total amount of USDT to hit {} :".format(max_high), '${:,.2f}'.format(sum_dollars))
print("    1.87 BILLION DOLLARS")
print("##########################################################")
print("##########################################################")