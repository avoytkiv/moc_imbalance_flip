import pymysql
import pandas as pd
import numpy as np
import logging
import os
import glob
import re
from datetime import datetime, timedelta
from clickhouse_driver.pandasConnector import pandasConnector as clickConn
from tools.credentials import get_login, get_pass


def init_logging(log_file=None, append=False, console_loglevel=logging.INFO):
    """Set up logging to file and console."""
    if log_file is not None:
        if append:
            filemode_val = 'a'
        else:
            # overwrite
            filemode_val = 'w'
        logging.basicConfig(level=logging.DEBUG,
                            format="%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s",
                            # datefmt='%m-%d %H:%M',
                            filename=log_file,
                            filemode=filemode_val)
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(console_loglevel)
    # set a format which is simpler for console use
    formatter = logging.Formatter("%(message)s")
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    global logger
    logger = logging.getLogger(__name__)


def get_data(query, params):
    try:
        data = pd.read_sql_query(query, con, params)
    except:
        data = pd.DataFrame(data=[])

    return data


def next_date(date, i=1):
    return datetime.strftime(datetime.strptime(date, '%Y-%m-%d') + timedelta(days=i), '%Y-%m-%d')
init_logging(log_file='imb.log', append=False)


def get_prices(symbol, date, datetime_start, datetime_stop):
    con = clickConn(host="10.12.1.60", db="tick", user='quant', password='quant')
    prices = con.read_sql_query("SELECT toString(XTime) as Time, XTimeMicro as TimeMicro, MsgCnt, Symbol, Bid_P, Ask_P "
                             "FROM tick.Equities "
                             "WHERE Symbol = '%s' "
                             "AND TradeDate='%s' "
                             "AND toDateTime(XTime)>toDateTime('%s') "
                             "AND toDateTime(XTime)<toDateTime('%s') "
                             "ORDER BY XTime, MsgCnt ASC" % (symbol, date, datetime_start, datetime_stop),
                             tableName='Equities')

    df_prices = pd.DataFrame(prices)

    if df_prices.empty:
        logger.info('No data for this time range from {} to {}'.format(datetime_start, datetime_stop))

        return df_prices

    else:
        df_prices['TimeMicro'] = df_prices['TimeMicro'].astype(str).str.zfill(6)
        df_prices['Time'] = df_prices['Time'].astype(str) + '.' + df_prices['TimeMicro'].astype(str)
        df_prices.index = pd.to_datetime(df_prices['Time'])

        return df_prices


cwd = os.getcwd()
user = get_login()
password = get_pass()
con = pymysql.connect(host='10.12.1.25', port=3306, database='UsEquitiesL1', user=user, password=password)

bt_config = {'hold': 60000, 'minVolume': 2000000, 'maxSpread': 0.2, 'absDeltaImbPct': 1}

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
for f in glob.glob(path):
    date = re.search('imbalances/(.*).csv', f).group(1)
    logger.info('Date: {}'.format(date))
    df = pd.read_csv(f, index_col=0)

    symbols = df['Symbol'].unique()

    logger.info('Symbols in universe: {}'.format(symbols))

    hold_period = 60000

    data = []
    for s in symbols:
        stock = pd.read_sql_query(query_stock, con, params={'symbol': s, 'date': date})
        volume = stock['Shares'].iloc[-1]

        if volume < bt_config['minVolume']:
            logger.info('Volume filter: {} is less than {}'.format(volume, bt_config['minVolume']))
            continue

        # # Get moc price for the date. Use next available date
        # df_moc_close_price = pd.read_sql_query(query_close_price, con,
        #                                        params={'symbol': s, 'date': next_date(date, 1)})
        #
        # # If no moc price because of weekend day
        # # TODO: how to be sure that we got correct moc price and the data is not missing for long period
        # # TODO: in current flow will get price anyway
        # while df_moc_close_price.empty:
        #     logger.info('No moc price for {} for this date {}. Try next date'.format(s, date))
        #     new_date = next_date(date=date, i=+1)
        #     logger.info('New date: {}'.format(new_date))
        #     df_moc_close_price = pd.read_sql_query(query_close_price, con,
        #                             params={'symbol': s, 'date': new_date})
        #
        #     date = new_date
        #
        # moc_close_price = df_moc_close_price['Price'].iloc[0] / 10000
        # logger.info('Current symbol {} with volume {} and moc price {}'.format(s, volume, moc_close_price))

        current_symbol = df[df['Symbol'] == s].copy()
        current_symbol['reverse_count'] = np.arange(start=1, stop=len(current_symbol)+1)
        current_symbol['imbBeforeReversePct'] = current_symbol['iShares'] * 100 / volume
        current_symbol['imbAfterReversePct'] = current_symbol['NextiShares'] * 100 / volume
        current_symbol['deltaImbPct'] = current_symbol['imbAfterReversePct'] - current_symbol['imbBeforeReversePct']

        if current_symbol[current_symbol['deltaImbPct'].abs() > bt_config['absDeltaImbPct']].empty:
            logger.info('Delta imbalance filter: {} is less than {}'.format(volume, bt_config))
            continue

        # Set time index
        current_symbol.loc[:, 'TIME'] = current_symbol.loc[:, 'TIME'].map(lambda x: x.lstrip('0 days '))
        current_symbol['timeindex'] = current_symbol.loc[:, ['Timestamp', 'TIME']].apply(lambda x: ' '.join(x), axis=1)
        current_symbol.index = pd.to_datetime(current_symbol['timeindex'])
        current_symbol = current_symbol.loc[current_symbol.index < pd.to_datetime(date + ' ' + '15:59:59')]

        current_symbol['stop'] = current_symbol.index + timedelta(milliseconds=hold_period)
        current_symbol.loc[current_symbol['stop'] > pd.to_datetime(date + ' ' + '15:59:59'), 'stop'] = pd.to_datetime(date + ' ' + '16:00:00')

        # Slice price data needed for returns calculation
        datetime_start = current_symbol['timeindex'].iloc[0].split('.')[0]
        datetime_stop = str(current_symbol['stop'].iloc[-1]).split('.')[0]

        logger.info('Time range from {} to {}'.format(datetime_start, datetime_stop))

        current_prices = get_prices(s, date, datetime_start, datetime_stop)

        if current_prices.empty:
            logger.info('No price data for this reversal')
            continue

        for index, row in current_symbol.iterrows():
            if index > pd.to_datetime(date + ' ' + '15:59:59'):
                logger.info('Continue because start datetime {} is later than 15:59:59'.format(index))
                continue
            logger.info('Reverse count: {}'.format(row['reverse_count']))
            direction = 'Long' if np.sign(row['deltaImbPct']) > 0 else 'Short'
            open_price = row['Ask_P'] if 'Long' else row['Bid_P']
            spread_at_open = row['Ask_P'] - row['Bid_P']

            exit_time = index + timedelta(milliseconds=bt_config['hold'])
            current_prices_slice = current_prices[(current_prices.index > index) & (current_prices.index < exit_time)]

            if current_prices_slice.empty:
                logger.info('No data for {} time hold'.format(bt_config['hold']))
                continue


            if exit_time > pd.to_datetime(date + ' ' + '16:00:00'):
                exit_time = pd.to_datetime(date + ' ' + '16:00:00')
                close_price = moc_close_price
                spread_at_close = 0
                close_status = 'moc'
                logger.info('Close position with moc order')
            else:
                if direction == 'Long':
                    close_price = current_prices_slice['Bid_P'].iloc[-1]
                elif direction == 'Short':
                    close_price = current_prices_slice['Ask_P'].iloc[-1]

                close_status = 'market'
                spread_at_close = current_prices_slice['Ask_P'].iloc[-1] - current_prices_slice['Bid_P'].iloc[-1]


            delta_move = close_price - open_price if direction == 'Long' else open_price - close_price
            delta_move_pct = delta_move * 100 / open_price

            data.append({'symbol': s,
                         'start': index,
                         'stop': exit_time,
                         'open_price': open_price,
                         'close_status': close_status,
                         'volume': volume,
                         'spread_at_open': spread_at_open,
                         'spread_at_close': spread_at_close,
                         'reverse_count':row['reverse_count'],
                         'imbBeforeReversePct': row['imbBeforeReversePct'],
                         'imbAfterReversePct': row['imbAfterReversePct'],
                         'deltaImbPct': row['deltaImbPct'],
                         'direction': direction,
                         'delta_move': delta_move,
                         'delta_move_pct': delta_move_pct,
                         'holding_time_2': hold_period})

stat = pd.DataFrame(data)
stat.to_csv(cwd + '/data/atto/stat.csv')


