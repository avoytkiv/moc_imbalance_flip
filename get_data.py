import pymysql
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import time


user = 'avoitkiv'
password = 'Pooboad2Fe3d'
con = pymysql.connect(host='10.12.1.25', port=3306, database='UsEquitiesL1', user=user, password=password)


start = time.time()
from datetime import date
d1 = date(2020, 2, 3)
d2 = date(2020, 11, 12)
delta = d2 - d1

dates = []
for i in range(delta.days + 1):
    current_date = d1 + timedelta(days=i)
    dates.append(current_date)

for d in dates:
    d = datetime.strftime(d, '%Y-%m-%d')
    query_imb = "SELECT * " \
                "FROM " \
                    "(SELECT Symbol, Timestamp, TIME, iPaired, Ask_P, Bid_P, iShares, " \
                        "LEAD(iShares,1) OVER ( ORDER BY Symbol, msgCnt ) AS NextiShares, " \
                        "LEAD(Bid_S,1) OVER ( ORDER BY Symbol, msgCnt ) AS NextBid_S, " \
                        "LEAD(Ask_S,1) OVER ( ORDER BY Symbol, msgCnt ) AS NextAsk_S, " \
                        "LEAD(Ask_P,1) OVER ( ORDER BY Symbol, msgCnt ) AS NextAsk_P, " \
                        "LEAD(Bid_P,1) OVER ( ORDER BY Symbol, msgCnt ) AS NextBid_P " \
                    "FROM `%s` AS t " \
                    "WHERE Reason='Imbalance' " \
                        "AND TIME>'15:50:00' " \
                        "AND MsgSource='NYSE') T " \
                "WHERE (((T.iShares > 0) AND (T.NextiShares < 0)) " \
                "OR ((T.iShares < 0) AND (T.NextiShares > 0)))" % d

    try:
        df_date = pd.read_sql_query(query_imb, con)
    except:
        df_date = pd.DataFrame(data=[])


    if df_date.empty:
        print('No data for this date: {}'.format(d))
        continue
    df_date.to_csv('/Users/Berg/Documents/Strategies/trend_signals_docker/data/atto/imbalances/' + d + '.csv')
    print('Date {} saved'.format(d))

end = time.time()
print(end - start)