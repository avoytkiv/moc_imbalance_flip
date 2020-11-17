import pymysql
import pandas as pd
import numpy as np
import os
import glob
import re
from datetime import timedelta

from tools.credentials import get_login, get_pass
from tools.tools import init_logging, next_date, get_prices


# Initiate logging
logger = init_logging(log_file='imb.log', append=False)
logger.info('Backtest started')

cwd = os.getcwd()
user = get_login()
password = get_pass()

# Connection to db
con = pymysql.connect(host='10.12.1.25', port=3306, database='UsEquitiesL1', user=user, password=password)

# Backtest config
bt_config = {'hold': 60000, 'minVolume': 2000000, 'maxSpread': 0.2, 'absDeltaImbPct': 1}
bp = 50000

query_stock = "SELECT * " \
              "FROM stock.Stock s " \
              "WHERE Symbol = %(symbol)s " \
              "AND `Timestamp` = %(date)s"

query_close_price = "SELECT * " \
                    "FROM stock.Stock " \
                    "WHERE symbol = %(symbol)s " \
                    "AND Timestamp = %(date)s " \
                    "AND Updated<'16:00' " \
                    "GROUP BY Timestamp "


path = cwd + '/data/imbalances/*.csv'
data = []
files = sorted(glob.glob(path))
for f in files:
    # f = cwd + '/data/imbalances/2020-02-07.csv'
    date = re.search('imbalances/(.*).csv', f).group(1)
    logger.info('Date: {}'.format(date))
    df = pd.read_csv(f, index_col=0)
    symbols = df['Symbol'].unique()
    logger.info('Symbols in universe: {}'.format(len(symbols)))

    for s in symbols:
        # s='BX'
        moc_date = date
        logger.info('Symbol:{}'.format(s))
        stock = pd.read_sql_query(query_stock, con, params={'symbol': s, 'date': date})
        if stock.empty:
            logger.info('Volume is empty')
            continue
        volume = stock['Shares'].iloc[-1]

        if volume < bt_config['minVolume']:
            logger.info('Volume filter: {}'.format(volume))
            continue

        current_symbol = df[df['Symbol'] == s].copy()
        current_symbol['reverse_count'] = np.arange(start=1, stop=len(current_symbol)+1)
        current_symbol['imbBeforeReversePct'] = current_symbol['PreviShares'] * 100 / volume
        current_symbol['imbAfterReversePct'] = current_symbol['iShares'] * 100 / volume
        current_symbol['deltaImbPct'] = current_symbol['imbAfterReversePct'] - current_symbol['imbBeforeReversePct']

        current_symbol = current_symbol[current_symbol['deltaImbPct'].abs() > bt_config['absDeltaImbPct']]
        if current_symbol.empty:
            logger.info('Delta imbalance filter')
            continue

        current_symbol['direction'] = np.where(current_symbol['deltaImbPct'] > 0, 'Long', 'Short')
        current_symbol['open_price'] = np.where(current_symbol['direction'] == 'Long',
                                                current_symbol['Ask_P'], current_symbol['Bid_P'])
        current_symbol['spread_at_open'] = current_symbol['Ask_P'] - current_symbol['Bid_P']

        current_symbol = current_symbol[current_symbol['spread_at_open'] < bt_config['maxSpread']]
        if current_symbol[current_symbol['spread_at_open'] < bt_config['maxSpread']].empty:
            logger.info('Spread filter')
            continue


        # Trade only first reversal
        current_symbol = current_symbol[current_symbol['reverse_count'] == current_symbol['reverse_count'].min()]

        # Set time index
        current_symbol.loc[:, 'TIME'] = current_symbol.loc[:, 'TIME'].map(lambda x: x.lstrip('0 days '))
        current_symbol['start'] = current_symbol.loc[:, ['Timestamp', 'TIME']].apply(lambda x: ' '.join(x), axis=1)
        current_symbol.index = pd.to_datetime(current_symbol['start'])
        # Add exit time and status
        current_symbol['stop'] = current_symbol.index + timedelta(milliseconds=bt_config['hold'])
        current_symbol.loc[current_symbol['stop'] > pd.to_datetime(date + ' ' + '16:00:00'), 'close_status'] = 'moc'
        current_symbol['close_status'] = current_symbol['close_status'].fillna('market')
        current_symbol.loc[current_symbol['stop'] > pd.to_datetime(date + ' ' + '16:00:00'), 'stop'] = pd.to_datetime(
            date + ' ' + '16:00:00')
        # Filter if start is after market close
        current_symbol = current_symbol.loc[current_symbol.index < pd.to_datetime(date + ' ' + '15:59:59')]

        if current_symbol.empty:
            logger.info('Time entry filter')
            continue

        # Slice price/market data needed for returns calculation
        datetime_start = current_symbol['start'].iloc[0].split('.')[0]
        datetime_stop = str(current_symbol['stop'].iloc[0]).split('.')[0]

        logger.info('Time range from {} to {}'.format(datetime_start, datetime_stop))

        # Slice prices
        current_prices = get_prices(s, date, datetime_start, datetime_stop)

        if current_prices.empty:
            logger.info('No price data for this reversal')
            continue


        direction = current_symbol['direction'].iloc[0]
        open_price = current_symbol['open_price'].iloc[0]
        close_status = current_symbol['close_status'].iloc[0]
        spread_at_open = current_symbol['spread_at_open'].iloc[0]


        if close_status == 'moc':
            logger.info('Close status moc')
            # Get moc price for the date. Use next available date
            df_moc_close_price = pd.read_sql_query(query_close_price, con,
                                                   params={'symbol': s, 'date': next_date(date, 1)})

            # If no moc price because of weekend day
            # TODO: how to be sure that we got correct moc price and the data is not missing for long period
            # TODO: in current flow will get price anyway
            df_status = 'data_yes'
            if df_moc_close_price.empty:
                df_status = 'data_no'
                logger.info('datafrmae status: {}'.format(df_status))
            # while df_moc_close_price.empty:
            while df_status=='data_no':

                logger.info('No moc price for {} on this date {}. Try next date'.format(s, date))
                new_date = next_date(date=moc_date, i=+1)
                logger.info('New date: {}'.format(new_date))
                df_moc_close_price = pd.read_sql_query(query_close_price, con,
                                        params={'symbol': s, 'date': new_date})
                if df_moc_close_price.empty:
                    df_status = 'data_no'
                else:
                    df_status = 'data_yes'

                moc_date = new_date

            moc_close_price = df_moc_close_price['Price'].iloc[0] / 10000
            logger.info('Moc price {}, moc date {} for symbol {}'.format(moc_close_price, moc_date, s))
            close_price = moc_close_price
            spread_at_close = 0
            logger.info('Close position with moc order')
        else:
            if direction == 'Long':
                close_price = current_prices['Bid_P'].iloc[-1]
            elif direction == 'Short':
                close_price = current_prices['Ask_P'].iloc[-1]

            spread_at_close = current_prices['Ask_P'].iloc[-1] - current_prices['Bid_P'].iloc[-1]


        position_size = current_symbol['Ask_S'].iloc[0] if direction == 'Long' else current_symbol['Bid_S'].iloc[0]
        delta_move = close_price - open_price if direction == 'Long' else open_price - close_price
        position_pnl = delta_move * position_size
        delta_move_pct = delta_move * 100 / open_price

        position_size_bp = min(bp / open_price, position_size)
        position_pnl_bp = delta_move * position_size_bp

        data.append({'date': date,
                     'symbol': s,
                     'volume': volume,
                     'start': datetime_start,
                     'stop': datetime_stop,
                     'direction': direction,
                     'open_price': open_price,
                     'spread_at_open': spread_at_open,
                     'close_price': close_price,
                     'close_status': close_status,
                     'spread_at_close': spread_at_close,
                     'position_size': position_size,
                     'position_size_bp': position_size_bp,
                     'reverse_count':current_symbol['reverse_count'].iloc[0],
                     'imbBeforeReversePct': current_symbol['imbBeforeReversePct'].iloc[0],
                     'imbAfterReversePct': current_symbol['imbAfterReversePct'].iloc[0],
                     'deltaImbPct': current_symbol['deltaImbPct'].iloc[0],
                     'delta_move': delta_move,
                     'delta_move_pct': delta_move_pct,
                     'position_pnl_bp': position_pnl_bp,
                     'position_pnl': position_pnl})

    stat = pd.DataFrame(data)
    stat.to_csv(cwd + '/data/positions/hold_{}_volume_{}_spread_{}_deltaimb_{}_date_{}.csv'.format(bt_config['hold'],
                                                                                                   bt_config['minVolume'],
                                                                                                   bt_config['maxSpread'],
                                                                                                   bt_config['absDeltaImbPct'],
                                                                                                   date))
logger.info('Positions saved')
logger.info('Backtest finished')

