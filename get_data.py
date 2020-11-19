import pymysql
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from tools.credentials import get_login, get_pass


cwd = os.getcwd()
user = get_login()
password = get_pass()
con = pymysql.connect(host='10.12.1.25', port=3306, database='UsEquitiesL1', user=user, password=password)


start = time.time()
from datetime import date
d1 = date(2020, 2, 3)
d2 = date(2020, 11, 19)
delta = d2 - d1

dates = []
for i in range(delta.days + 1):
    current_date = d1 + timedelta(days=i)
    dates.append(current_date)

for d in dates:
    start_d = time.time()
    d = datetime.strftime(d, '%Y-%m-%d')
    query_imb = "SELECT * FROM " \
                "(SELECT Symbol, Timestamp, TIME, iPaired, Ask_P, Bid_P, Ask_S, Bid_S, iShares, " \
                "LAG(iShares,1) OVER ( PARTITION BY Symbol ORDER BY Symbol, msgCnt ) AS PreviShares " \
                "FROM UsEquitiesL1.`%s` AS t " \
                "WHERE Reason='Imbalance' " \
                "AND Ask_P > Bid_P " \
                "AND TIME>'15:50:00' " \
                "AND MsgSource='NYSE' " \
                "AND Symbol IN (SELECT DISTINCT Symbol FROM stock.Stock WHERE `Timestamp` = '%s' AND DailyShares > 2000000 AND EXCHANGE = 'N')) AS T " \
                "WHERE (((T.iShares > 0) AND (T.PreviShares < 0)) " \
                "OR ((T.iShares < 0) AND (T.PreviShares > 0)))" % ((d,)*2)

    try:
        df_date = pd.read_sql_query(query_imb, con)
    except:
        df_date = pd.DataFrame(data=[])


    if df_date.empty:
        print('No data for this date: {}'.format(d))
        continue
    df_date.to_csv(cwd + '/data/imbalances/' + d + '.csv')
    stop_d = time.time()
    print('Date {} saved. Time: {} seconds'.format(d, stop_d - start_d))

end = time.time()
print(end - start)