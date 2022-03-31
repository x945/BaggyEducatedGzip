import fxcmpy
import numpy as np
import pandas as pd
import pandas_ta as ta
import time
import datetime
from datetime import timedelta
from pytz import timezone


conn = fxcmpy.fxcmpy(config_file='fxcm.cfg')
accountID = conn.get_default_account()
conn.set_default_account(accountID)

tz = timezone('EST')

############ Defnitions 

askm5df = pd.DataFrame()
bidm5df = pd.DataFrame()

askm5df['close'], bidm5df['close'] = np.nan, np.nan

activateRSI_askm5, shortSMA_askm5, longSMA_askm5, activateGAMMA_askm5, BuyDirection_askm5 = False, False, False, False, False

activateRSI_bidm5, shortSMA_bidm5, longSMA_bidm5, activateGAMMA_bidm5, SellDirection_bidm5 = False, False, False, False, False

def process_askm5(dt, data):
    global askm5df
    askm5df.loc[len(dt.index), 'liveClose'] = data
    askm5df['close'] = askm5df['liveClose'].rolling(window=2, min_periods=1).apply(lambda x:x.iloc[0])

def process_bidm5(dt, data):
    global bidm5df
    bidm5df.loc[len(dt.index), 'liveClose'] = data
    bidm5df['close'] = bidm5df['liveClose'].rolling(window=2, min_periods=1).apply(lambda x:x.iloc[0])


def handler(data, dataframe):
    ###### 5 Mintue Timeframe
    ## Ask prices
    askm5df = pd.DataFrame()
    askm5df = dataframe['Ask'].resample('5Min').last()
    process_askm5(askm5df, askm5df[-1])
    ## Bid prices
    bidm5df = pd.DataFrame()
    bidm5df = dataframe['Bid'].resample('5Min').last()
    process_bidm5(bidm5df, bidm5df[-1])

def time_to_open(current_time):
    if current_time.weekday() <= 4:
        d = (current_time + timedelta(days=1)).date()
    else:
        days_to_mon = 0 - current_time.weekday() + 7
        d = (current_time + timedelta(days=days_to_mon)).date()
    next_day = datetime.datetime.combine(d, datetime.time(9, 30, tzinfo=tz))
    seconds = (next_day - current_time).total_seconds()
    return seconds

while True:
    conn.subscribe_market_data('SPX500', (handler,))
    time.sleep(1)
    ############## 5Min Ask Prices
    if len(askm5df['close']) > 12:
        askm5df['Short_SMA'] = ta.sma(askm5df['close'], length=12)
        shortSMA_askm5 = True
    if len(askm5df['close']) > 26:
        askm5df['Long_SMA'] = ta.sma(askm5df['close'], length=26)
        longSMA = True
    if shortSMA_askm5 & longSMA_askm5:
        askm5df['Bullish_SMA'] = np.where(askm5df['Long_SMA']>askm5df['Short_SMA'], 1, 0)
        askm5df['Bearish_SMA'] = np.where(askm5df['Long_SMA']<askm5df['Short_SMA'], 1, 0)
        askm5df['SMA_Diff'] = askm5df['Bullish_SMA'] - askm5df['Bearish_SMA']
        askm5df['SMA_Crossover'] = askm5df['SMA_Diff'].diff().fillna(0)
        BuyDirection_askm5 = True
    askm5df.to_csv('data/askm5_results.csv', index_label='index')
    ############## 5Min Bid Prices
    if len(bidm5df['close']) > 12:
        bidm5df['Short_SMA'] = ta.sma(bidm5df['close'], length=12)
        shortSMA_bidm5 = True
    if len(bidm5df['close']) > 26:
        bidm5df['Long_SMA'] = ta.sma(bidm5df['close'], length=26)
        longSMA_bidm5 = True
    if shortSMA_bidm5 & longSMA_bidm5:
        bidm5df['Bullish_SMA'] = np.where(bidm5df['Long_SMA']>bidm5df['Short_SMA'], 1, 0)
        bidm5df['Bearish_SMA'] = np.where(bidm5df['Long_SMA']<bidm5df['Short_SMA'], 1, 0)
        bidm5df['SMA_Diff'] = bidm5df['Bullish_SMA'] - bidm5df['Bearish_SMA']
        bidm5df['SMA_Crossover'] = bidm5df['SMA_Diff'].diff().fillna(0)
        SellDirection_bidm5 = True
    bidm5df.to_csv('data/bidm5_results.csv', index_label='index')
    ############### Execute Orders
    activateTrade = False
    activateLong = True
    activateShort = False
    ## Check it is a weekday Mon-Fri and market is open 9:30-3:30
    if datetime.datetime.now(tz).weekday() >= 0 and datetime.datetime.now(tz).weekday() <= 4:
        if datetime.datetime.now(tz).time() > datetime.time(00, 00) and datetime.datetime.now(tz).time() <= datetime.time(23, 59):
            m5Buy = pd.read_csv('data/askm5_results.csv')
            if BuyDirection_askm5:
               if len(m5Buy['SMA_Crossover']):
                   if m5Buy['SMA_Crossover'].iloc[-1] == 2:
                      Bullish_SMA = True
                   if m5Buy['SMA_Crossover'].iloc[-1] != 2:
                      Bullish_SMA = False
               if activateLong & Bullish_SMA:
                   conn.create_entry_order(symbol='SPX500', is_buy=True, is_in_pips=True, amount='100', time_in_force='GTC' , order_type='Entry', limit=50, stop=-50)
                   if len(conn.get_open_trade_ids()) >= 1:
                       activateTrade = True
                       tradeID = conn.get_open_trade_ids()[-1]
                       print('×'*50)
                       print('A position has been opened with trade ID:{}'.format(tradeID))
                       print('×'*50)  
            m5Sell = pd.read_csv('data/bidm5_results.csv')
            if SellDirection_bidm5:
               if len(m5Sell['SMA_Crossover']):
                   if m5Sell['SMA_Crossover'].iloc[-1] == -2:
                        Bearish_SMA = True
                   if m5Sell['SMA_Crossover'].iloc[-1] != -2:
                        Bearish_SMA = False
               if activateShort & Bearish_SMA:
                    conn.create_entry_order(symbol='SPX500', is_buy=False, is_in_pips=True, amount='100', time_in_force='GTC', order_type='Entry', limit=50, stop=-50)
                    if len(conn.get_open_trade_ids()) >= 1:
                        activateTrade = True
                        tradeID = conn.get_open_trade_ids()[-1]
                        print('×'*50)
                        print('A position has been opened with trade ID:{}'.format(tradeID))
                        print('×'*50)
            if activateTrade:
                if conn.get_closed_position(tradeID) == True:
                    activateTrade = False
                    trade = conn.get_closed_position(tradeID)
                    loss = trade.get_grossPL()
                    if np.sign(loss) == -1:
                        print('='*50)
                        print('The trade [{}] has lost {}. The session terminated. A review of price action is required'.format(tradeID, loss))
                        print('='*50)
                        break
        # Get time amount until open, sleep that amount
        else: 
            print('Market closed ({})'.format(datetime.datetime.now(tz)))
            print('Sleeping', round(time_to_open(datetime.datetime.now(tz))/60/60, 2), 'hours')
            time.sleep(time_to_open(datetime.datetime.now(tz)))
     # If not trading day, find out how much until open, sleep that amount
    else:
       print('Market closed ({})'.format(datetime.datetime.now(tz)))
       print('Sleeping', round(time_to_open(datetime.datetime.now(tz))/60/60, 2), 'hours')
       time.sleep(time_to_open(datetime.datetime.now(tz)))
