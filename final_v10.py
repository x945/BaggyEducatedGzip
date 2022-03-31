import fxcmpy
import numpy as np
import pandas as pd
import pandas_ta as ta
import time

conn = fxcmpy.fxcmpy(config_file='fxcm.cfg')
accountID = conn.get_default_account()
conn.set_default_account(accountID)

def handler(data, dataframe):
     ###### 1 Mintue Timeframe
     ## Ask prices
    askm1df = pd.DataFrame()
    askm1df = dataframe['Ask'].resample('1Min').ohlc()
    askm1df['Rsi'] = ta.rsi(askm1df['close'], length=2)
    askm1df['RsiMA'] = ta.sma(askm1df['Rsi'], length=2)
    askm1df['Gamma'] = askm1df['Rsi'] - askm1df['RsiMA']
    askm1df['Gamma_Long'] = np.where(askm1df['Gamma']>0, 1, 0)
    askm1df['Gamma_Short'] = np.where(askm1df['Gamma']<0, 1, 0)
    askm1df['Rsi_Diff'] = askm1df['Gamma_Long'] - askm1df['Gamma_Short']
    askm1df['Rsi_Crossover'] = askm1df['Rsi_Diff'].diff().fillna(0)
    if len(askm1df['Rsi_Crossover']):
        if askm1df['Rsi_Crossover'].iloc[-1] == 2:
            print('Crossover is equal to 2')
    askm1df['Short_SMA'] = ta.sma(askm1df['close'], length=2)
    askm1df['Long_SMA'] = ta.sma(askm1df['close'], length=5)
    askm1df['Bullish_SMA'] = np.where(askm1df['Long_SMA']>askm1df['Short_SMA'], 1, 0)
    askm1df['Bearish_SMA'] = np.where(askm1df['Long_SMA']<askm1df['Short_SMA'], 1, 0)
    askm1df['SMA_Diff'] = askm1df['Bullish_SMA'] - askm1df['Bearish_SMA']
    askm1df['SMA_Crossover'] = askm1df['SMA_Diff'].diff().fillna(0)
    ## Bid prices
    bidm1df = pd.DataFrame()
    bidm1df = dataframe['Bid'].resample('1Min').ohlc()
    bidm1df['Rsi'] = ta.rsi(bidm1df['close'], length=2)
    bidm1df['RsiMA'] = ta.sma(bidm1df['Rsi'], length=2)
    bidm1df['Gamma'] = bidm1df['Rsi'] - bidm1df['RsiMA']
    bidm1df['Gamma_Long'] = np.where(bidm1df['Gamma']>0, 1, 0)
    bidm1df['Gamma_Short'] = np.where(bidm1df['Gamma']<0, 1,  0)
    bidm1df['Rsi_Diff'] = bidm1df['Gamma_Long'] - bidm1df['Gamma_Short']
    bidm1df['Rsi_Crossover'] = bidm1df['Rsi_Diff'].diff().fillna(0)
    if len(bidm1df['Rsi_Crossover']):
        if bidm1df['Rsi_Crossover'].iloc[-1] == -2:
            print('Crossover is equal to -2')
    bidm1df['Short_SMA'] = ta.sma(bidm1df['close'], length=2)
    bidm1df['Long_SMA'] = ta.sma(bidm1df['close'], length=5)
    bidm1df['Bullish_SMA'] = np.where(bidm1df['Long_SMA']>bidm1df['Short_SMA'], 1, 0)
    bidm1df['Bearish_SMA'] = np.where(bidm1df['Long_SMA']<bidm1df['Short_SMA'], 1, 0)
    bidm1df['SMA_Diff'] = bidm1df['Bullish_SMA'] - bidm1df['Bearish_SMA']
    bidm1df['SMA_Crossover'] = bidm1df['SMA_Diff'].diff().fillna(0)
    ## Write the output
    m1df = pd.concat([askm1df, bidm1df], axis=1,  keys=['Ask', 'Bid'])
    m1df.to_csv('data/m1_output.csv', index_label='Date')
    ###### 5 Mintue Timeframe
     ## Ask prices
    askm5df = pd.DataFrame()
    askm5df = dataframe['Ask'].resample('15Min').ohlc()
    askm5df['Rsi'] = ta.rsi(askm5df['close'], length=2)
    askm5df['RsiMA'] = ta.sma(askm5df['Rsi'], length=2)
    askm5df['Gamma'] = askm5df['Rsi'] - askm5df['RsiMA']
    askm5df['Gamma_Long'] = np.where(askm5df['Gamma']>0, 1, 0)
    askm5df['Gamma_Short'] = np.where(askm5df['Gamma']<0, 1, 0)
    askm5df['Rsi_Diff'] = askm5df['Gamma_Long'] - askm5df['Gamma_Short']
    askm5df['Rsi_Crossover'] = askm5df['Rsi_Diff'].diff().fillna(0)
    ## Bid prices
    bidm5df = pd.DataFrame()
    bidm5df = dataframe['Bid'].resample('5Min').ohlc()
    bidm5df['Rsi'] = ta.rsi(bidm5df['close'], length=2)
    bidm5df['RsiMA'] = ta.sma(bidm5df['Rsi'], length=2)
    bidm5df['Gamma'] = bidm5df['Rsi'] - bidm5df['RsiMA']
    bidm5df['Gamma_Long'] = np.where(bidm5df['Gamma']>0, 1, 0)
    bidm5df['Gamma_Short'] = np.where(bidm5df['Gamma']<0, 1,  0)
    bidm5df['Rsi_Diff'] = bidm5df['Gamma_Long'] - bidm5df['Gamma_Short']
    bidm5df['Rsi_Crossover'] = bidm5df['Rsi_Diff'].diff().fillna(0)
    ## Write the output
    m5df = pd.concat([askm5df, bidm5df], axis=1,  keys=['Ask', 'Bid'])           
    m5df.to_csv('data/m5_output.csv', index_label='Date')
    ####### Orders Excution
    if len(askm1df['Rsi_Crossover']):
        if askm1df['Rsi_Crossover'].iloc[-1] == 2:
            print('Crossover is equal to 2 and a buy position is triggered')
            conn.create_entry_order(symbol='BRKB.us', is_buy=True, amount=300, limit=0, is_in_pips = True, time_in_force='GTC', rate=500, stop=500, trailing_step=None, trailing_stop_step=None)
    if len(bidm1df['Rsi_Crossover']):
        if bidm1df['Rsi_Crossover'].iloc[-1] == -2:
            print('Crossover is equal to -2 and a sell position is triggered')
            conn.create_entry_order(symbol='BRKB.us', is_buy=False, amount=300, limit=0, is_in_pips = True, time_in_force='GTC', rate=500, stop=500, trailing_step=None, trailing_stop_step=None)
    ## Write closed position
    positiondf = pd.DataFrame()
    positiondf = conn.get_closed_positions()
    if len(positiondf):
        positiondf.to_csv('data/closed_positions.csv')

conn.subscribe_market_data('SPX500', (handler,))
time.sleep(1000)
conn.unsubscribe_market_data('SPX500')
