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

def handler(data, dataframe):
     ###### 1 Mintue Timeframe
    ## Ask prices
    #askm1df = pd.DataFrame()
    agg_dict = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}
    askm1Data = dataframe['Ask'].resample('1Min').agg(agg_dict)
    askm1df = pd.DataFrame(askm1Data, columns=[ 'Open', 'High', 'Low', 'Close'])
    askm1df.index.name = 'Date/time'
    askm1df.to_csv('data/askm1_prices.csv')
    ## Bid prices
    #bidm1df = pd.DataFrame()
    bidm1Data = dataframe['Bid'].resample('1Min').agg(agg_dict)
    bidm1df = pd.DataFrame(bidm1Data, columns=[ 'Open', 'High', 'Low', 'Close'])
    bidm1df.index.name = 'Date/time'
    bidm1df.to_csv('data/bidm1_prices.csv')
    ###### 1 Mintue Timeframe
    ## Ask prices
    #askm15df = pd.DataFrame()
    askm15Data = dataframe['Ask'].resample('15Min').agg(agg_dict)
    askm15df = pd.DataFrame(askm15Data, columns=[ 'Open', 'High', 'Low', 'Close'])
    askm15df.index.name = 'Date/time'
    askm15df.to_csv('data/askm15_prices.csv')
    ## Bid prices
    #bidm15df = pd.DataFrame()
    bidm15Data = dataframe['Bid'].resample('15Min').agg(agg_dict)
    bidm15df = pd.DataFrame(bidm15Data, columns=[ 'Open', 'High', 'Low', 'Close'])
    bidm15df.index.name = 'Date/time'
    bidm15df.to_csv('data/bidm15_prices.csv')

conn.subscribe_market_data('SPX500', (handler,))

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
    time.sleep(60)
    ############## 1Min Ask Prices
    df = pd.read_csv('data/askm1_prices.csv', on_bad_lines='warn')
    askdata = pd.DataFrame()
    min_value = 3
    activateRSI_askm1, shortSMA_askm1, longSMA_askm1, activateGAMMA_askm1, BuyDirection_askm1 = False, False, False, False, False
    askdata['close'] = 0
    if len(df.index) > 3:
        for i in range(min_value, len(df.index)):
            min_value = min_value + 1
            askdata.loc[i, 'close'] = df.loc[i-2, 'Close']
    if len(askdata['close']) > 14:
        askdata['Rsi'] = ta.rsi(askdata['close'], length=14)
        askdata['RsiMA'] = ta.sma(askdata['Rsi'], length=14)
        activateRSI_askm1 = True
    if len(askdata['close']) > 12:
        askdata['Short_SMA'] = ta.sma(askdata['close'], length=12)
        shortSMA_askm1 = True
    if len(askdata['close']) > 26:
        askdata['Long_SMA'] = ta.sma(askdata['close'], length=26)
        longSMA_askm1 = True
    if activateRSI_askm1:
        askdata['Gamma'] = askdata['Rsi'] - askdata['RsiMA']
        activateGAMMA_askm1 =True
    if activateGAMMA_askm1:
        askdata['Gamma_Long'] = np.where(askdata['Gamma']>0, 1, 0)
        askdata['Gamma_Short'] = np.where(askdata['Gamma']<0, 1, 0)
        askdata['Rsi_Diff'] = askdata['Gamma_Long'] - askdata['Gamma_Short']
        askdata['Rsi_Crossover'] = askdata['Rsi_Diff'].diff().fillna(0)
    if shortSMA_askm1 & longSMA_askm1:
        askdata['Bullish_SMA'] = np.where(askdata['Long_SMA']>askdata['Short_SMA'], 1, 0)
        askdata['Bearish_SMA'] = np.where(askdata['Long_SMA']<askdata['Short_SMA'], 1, 0)
        askdata['SMA_Diff'] = askdata['Bullish_SMA'] - askdata['Bearish_SMA']
        askdata['SMA_Crossover'] = askdata['SMA_Diff'].diff().fillna(0)
        BuyDirection_askm1 = True
    askdata.to_csv('data/askm1_results.csv', index_label='index')
    ############## 1Min Bid Prices
    dx = pd.read_csv('data/bidm1_prices.csv', on_bad_lines='warn')
    biddata = pd.DataFrame()
    min_value = 3
    activateRSI_bidm1, shortSMA_bidm1, longSMA_bidm1, activateGAMMA_bidm1, SellDirection_bidm1 = False, False, False, False, False
    biddata['close'] = 0
    if len(dx.index) > 3:
        for i in range(min_value, len(dx.index)):
            min_value = min_value + 1
            biddata.loc[i, 'close'] = dx.loc[i-2, 'Close']
    if len(biddata['close']) > 14:
        biddata['Rsi'] = ta.rsi(biddata['close'], length=14)
        biddata['RsiMA'] = ta.sma(biddata['Rsi'], length=14)
        activateRSI_bidm1 = True
    if len(biddata['close']) > 12:
        biddata['Short_SMA'] = ta.sma(biddata['close'], length=12)
        shortSMA_bidm1 = True
    if len(biddata['close']) > 26:
        biddata['Long_SMA'] = ta.sma(biddata['close'], length=26)
        longSMA_bidm1 = True
    if activateRSI_bidm1:
        biddata['Gamma'] = biddata['Rsi'] - biddata['RsiMA']
        activateGAMMA_bidm1 =True
    if activateGAMMA_bidm1:
        biddata['Gamma_Long'] = np.where(biddata['Gamma']>0, 1, 0)
        biddata['Gamma_Short'] = np.where(biddata['Gamma']<0, 1, 0)
        biddata['Rsi_Diff'] = biddata['Gamma_Long'] - biddata['Gamma_Short']
        biddata['Rsi_Crossover'] = biddata['Rsi_Diff'].diff().fillna(0)
    if shortSMA_bidm1 & longSMA_bidm1:
        biddata['Bullish_SMA'] = np.where(biddata['Long_SMA']>biddata['Short_SMA'], 1, 0)
        biddata['Bearish_SMA'] = np.where(biddata['Long_SMA']<biddata['Short_SMA'], 1, 0)
        biddata['SMA_Diff'] = biddata['Bullish_SMA'] - biddata['Bearish_SMA']
        biddata['SMA_Crossover'] = biddata['SMA_Diff'].diff().fillna(0)
        SellDirection_bidm1 = True
    biddata.to_csv('data/bidm1_results.csv', index_label='index')
############## 15Min Ask Prices
    dz = pd.read_csv('data/askm15_prices.csv', on_bad_lines='warn')
    ask15data = pd.DataFrame()
    min_value = 3
    activateRSI_askm15, shortSMA_askm15, longSMA_askm15, activateGAMMA_askm15, BuyDirection_askm15 = False, False, False, False, False
    ask15data['close'] = 0
    if len(dz.index) > 3:
        for i in range(min_value, len(dz.index)):
            min_value = min_value + 1
            ask15data.loc[i, 'close'] = dz.loc[i-2, 'Close']
    if len(ask15data['close']) > 14:
        ask15data['Rsi'] = ta.rsi(ask15data['close'], length=14)
        ask15data['RsiMA'] = ta.sma(ask15data['Rsi'], length=14)
        activateRSI_askm15 = True
    if len(ask15data['close']) > 12:
        ask15data['Short_SMA'] = ta.sma(ask15data['close'], length=12)
        shortSMA_askm15 = True
    if len(ask15data['close']) > 26:
        ask15data['Long_SMA'] = ta.sma(ask15data['close'], length=26)
        longSMA = True
    if activateRSI_askm15:
        ask15data['Gamma'] = ask15data['Rsi'] - ask15data['RsiMA']
        activateGAMMA_askm15 =True
    if activateGAMMA_askm15:
        ask15data['Gamma_Long'] = np.where(ask15data['Gamma']>0, 1, 0)
        ask15data['Gamma_Short'] = np.where(ask15data['Gamma']<0, 1, 0)
        ask15data['Rsi_Diff'] = ask15data['Gamma_Long'] - askdata['Gamma_Short']
        ask15data['Rsi_Crossover'] = ask15data['Rsi_Diff'].diff().fillna(0)
    if shortSMA_askm15 & longSMA_askm15:
        ask15data['Bullish_SMA'] = np.where(ask15data['Long_SMA']>ask15data['Short_SMA'], 1, 0)
        ask15data['Bearish_SMA'] = np.where(ask15data['Long_SMA']<ask15data['Short_SMA'], 1, 0)
        ask15data['SMA_Diff'] = ask15data['Bullish_SMA'] - ask15data['Bearish_SMA']
        ask15data['SMA_Crossover'] = ask15data['SMA_Diff'].diff().fillna(0)
        BuyDirection_askm15 = True
    ask15data.to_csv('data/askm15_results.csv', index_label='index')
    ############## 15Min Bid Prices
    dc = pd.read_csv('data/bidm15_prices.csv', on_bad_lines='warn')
    bid15data = pd.DataFrame()
    min_value = 3
    activateRSI_bidm15, shortSMA_bidm15, longSMA_bidm15, activateGAMMA_bidm15, SellDirection_bidm15 = False, False, False, False, False
    bid15data['close'] = 0
    if len(dc.index) > 3:
        for i in range(min_value, len(dc.index)):
            min_value = min_value + 1
            bid15data.loc[i, 'close'] = dc.loc[i-2, 'Close']
    if len(bid15data['close']) > 14:
        bid15data['Rsi'] = ta.rsi(bid15data['close'], length=14)
        bid15data['RsiMA'] = ta.sma(bid15data['Rsi'], length=14)
        activateRSI_bidm15 = True
    if len(bid15data['close']) > 12:
        bid15data['Short_SMA'] = ta.sma(bid15data['close'], length=12)
        shortSMA_bidm15 = True
    if len(bid15data['close']) > 26:
        bid15data['Long_SMA'] = ta.sma(bid15data['close'], length=26)
        longSMA_bidm15 = True
    if activateRSI_bidm15:
        bid15data['Gamma'] = bid15data['Rsi'] - bid15data['RsiMA']
        activateGAMMA_bidm15 =True
    if activateGAMMA_bidm15:
        bid15data['Gamma_Long'] = np.where(bid15data['Gamma']>0, 1, 0)
        bid15data['Gamma_Short'] = np.where(bid15data['Gamma']<0, 1, 0)
        bid15data['Rsi_Diff'] = bid15data['Gamma_Long'] - bid15data['Gamma_Short']
        bid15data['Rsi_Crossover'] = bid15data['Rsi_Diff'].diff().fillna(0)
    if shortSMA_bidm15 & longSMA_bidm15:
        bid15data['Bullish_SMA'] = np.where(bid15data['Long_SMA']>bid15data['Short_SMA'], 1, 0)
        bid15data['Bearish_SMA'] = np.where(bid15data['Long_SMA']<bid15data['Short_SMA'], 1, 0)
        bid15data['SMA_Diff'] = bid15data['Bullish_SMA'] - bid15data['Bearish_SMA']
        bid15data['SMA_Crossover'] = bid15data['SMA_Diff'].diff().fillna(0)
        SellDirection_bidm15 = True
    bid15data.to_csv('data/bidm15_results.csv', index_label='index')
    ############### Execute Orders
    activateTrade = False
    ## Check it is a weekday Mon-Fri and market is open 9:30-3:30
    if datetime.datetime.now(tz).weekday() >= 0 and datetime.datetime.now(tz).weekday() <= 4:
        if datetime.datetime.now(tz).time() > datetime.time(00, 00) and datetime.datetime.now(tz).time() <= datetime.time(23, 59):
            m1Buyorder = pd.read_csv('data/askm1_results.csv')
            m15Buyorder = pd.read_csv('data/askm15_results.csv')
            if BuyDirection_askm15:
               if len(m15Buyorder['Gamma_Long']):
                   if m15Buyorder['Gamma_Long'].iloc[-1] == 1:
                       Gamma15_Long = True
                       if len(m1Buyorder['SMA_Diff']):
                          if m1Buyorder['SMA_Diff'].iloc[-1] == 1:
                              Bullish_SMA = True
                              if len(m1Buyorder['Rsi_Crossover']):
                                 if Gamma15_Long & Bullish_SMA & (m1Buyorder['Rsi_Crossover'].iloc[-1] == 2):
                                     conn.open_trade(symbol='SPX500', is_buy=True, is_in_pips=True, amount='100', time_in_force='IOC', order_type='AtMarket', limit=50, stop=-50)
                                     if len(conn.get_open_trade_ids()) >= 1:
                                         activateTrade = True
                                         tradeID = conn.get_open_trade_ids()[-1]
                                         print('×'*50)
                                         print('A position has been opened with trade ID:{}'.format(tradeID))
                                         print('×'*50)
                          if m1Buyorder['SMA_Diff'].iloc[-1] != 1:
                              Bullish_SMA = False
                   if m15Buyorder['Gamma_long'].iloc[-1]  != 1:
                       Gamma15_Long = False
            m1Sell = pd.read_csv('data/bidm1_results.csv')
            m15Sell = pd.read_csv('data/bidm15_results.csv')
            if SellDirection_bidm15:
               if len(m15Sell['Gamma_Short']):
                   if m15Sell['Gamma_Short'].iloc[-1] == 1:
                       Gamma15_Short = True
                       if len(m1Sell['SMA_Diff']):
                           if m1Sell['SMA_Diff'].iloc[-1] == -1:
                               Bearish_SMA = True
                               if len(m1Sell['Rsi_Crossover']):
                                   if Gamma15_Short & Bearish_SMA & (m1Sell['Rsi_Crossover'].iloc[-1] == -2):
                                       conn.open_trade(symbol='SPX500', is_buy=False, is_in_pips=True, amount='100', time_in_force='IOC', order_type='AtMarket', limit=50, stop=-50)
                                       if len(conn.get_open_trade_ids()) >= 1:
                                           activateTrade = True
                                           tradeID = conn.get_open_trade_ids()[-1]
                                           print('×'*50)
                                           print('A position has been opened with trade ID:{}'.format(tradeID))
                                           print('×'*50)
                           if m1Sell['SMA_Diff'].iloc[-1] != -1:
                               Bearish_SMA = False
                   if m15Sell['Gamma_long'].iloc[-1]  != 1:
                       Gamma15_Short = False
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
