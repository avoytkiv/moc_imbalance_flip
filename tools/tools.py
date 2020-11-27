import logging
import pandas as pd
import pymysql
from datetime import datetime, timedelta
from tools.credentials import get_login, get_pass
from clickhouse_driver.pandasConnector import pandasConnector as clickConn


con = pymysql.connect(host='10.12.1.25', port=3306, database='UsEquitiesL1', user=get_login(), password=get_pass())

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
    # logger = logging.getLogger(__name__)
    return logging.getLogger(__name__)


def get_data(query, params):
    try:
        data = pd.read_sql_query(query, con, params)
    except:
        data = pd.DataFrame(data=[])

    return data


def next_date(date, i=1):
    return datetime.strftime(datetime.strptime(date, '%Y-%m-%d') + timedelta(days=i), '%Y-%m-%d')


def get_prices(symbol, date, datetime_start, datetime_stop):
    con = clickConn(host="10.12.1.60", db="tick", user='quant', password='quant')
    prices = con.read_sql_query("SELECT toString(XTime) as Time, XTimeMicro as TimeMicro, MsgCnt, Symbol, Bid_P, Ask_P "
                             "FROM tick.Equities "
                             "WHERE Symbol = '%s' "
                             "AND TradeDate='%s' "
                             "AND toDateTime(XTime)>=toDateTime('%s') "
                             "AND toDateTime(XTime)<toDateTime('%s') "
                             "ORDER BY XTime, MsgCnt ASC" % (symbol, date, datetime_start, datetime_stop),
                             tableName='Equities')

    df_prices = pd.DataFrame(prices)

    if df_prices.empty:

        return df_prices

    else:
        df_prices['TimeMicro'] = df_prices['TimeMicro'].astype(str).str.zfill(6)
        df_prices['Time'] = df_prices['Time'].astype(str) + '.' + df_prices['TimeMicro'].astype(str)
        df_prices.index = pd.to_datetime(df_prices['Time'])

        return df_prices
