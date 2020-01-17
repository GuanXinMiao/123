import pymysql
import pandas as pd 
from datetime import date
import datetime
import matplotlib.pyplot as plt

connect = pymysql.connect(host = "13.231.106.254", port = 3301, user = 'funnow_go', passwd = 'y3n42hVf4yUJUy6g', db = 'FunNow_V2')
cursor = connect.cursor()

today = date.today()
how_many_days_ago = 90

start_search_date = today - datetime.timedelta(days = how_many_days_ago)
query = """
SELECT Category, COUNT(IF(OrderAmount = 1, 1, NULL))1Order, 
COUNT(IF(OrderAmount = 2, 1, NULL))2Orders, COUNT(IF(OrderAmount = 3, 1, NULL))3Orders,
COUNT(IF(OrderAmount = 4, 1, NULL))4Orders, COUNT(IF(ORderAmount = 5, 1, NULL))5Orders,
COUNT(IF(OrderAmount > 5, 1, NULL))MoreOrders
FROM(
SELECT UID, Category, COUNT(*)OrderAmount
FROM PersonalBehavior
WHERE (Region = '大台北' OR Region = '台中' OR Region = '桃園' OR Region = '台南｜高雄')
AND OrderDate > '2019' 
GROUP BY UID, Category) T1
GROUP BY Category
""".format(start_search_date, today)


cursor.execute(query)

df = pd.DataFrame(cursor.fetchall(), columns = ['Category', '1Order', '2Orders', '3Orders', '4Orders', '5Orders', 'MoreOrders'])
df.to_csv('OrderAmountByCategory.csv')

query = """
SELECT recency, AVG(OrderAmount)
FROM
(
SELECT T1.UID, T1.Category, T1.GMV, T1.OrderAmount, T2.recency
FROM 
(
SELECT UID, Category, SUM(GMV)GMV , SUM(OrderAmount)OrderAmount
FROM PersonalBehavior
WHERE (Region = '大台北' OR Region = '台中' OR Region = '桃園' OR Region = '台南｜高雄') AND OrderDate > '2019/10/07'
GROUP BY UID, Category
)T1
LEFT JOIN(
SELECT UID, Category, MIN(DateD) as recency
FROM(
SELECT UID, Category, DATEDIFF(DATE_FORMAT(CURDATE(), '%Y/%m/%d'), DATE_FORMAT(OrderDate,'%Y/%m/%d') ) as DateD
FROM PersonalBehavior
WHERE (Region = '大台北' OR Region = '台中' OR Region = '桃園' OR Region = '台南｜高雄')
AND (OrderDate > '2019/10/07' )
)T2
GROUP By UID, Category)T2 ON T1.UID = T2.UID AND T1.Category = T2.Category
WHERE T1.Category IS NOT NULL
) T3
GROUP BY recency
"""
cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns = [ 'recency', 'AVGOrderAmount'])
#df = pd.DataFrame(cursor.fetchall(), columns = ['OrderAmount', 'CountCol'])
print(df)
print(df.describe())
plt.ylabel("Average Order Amount")
plt.xlabel("Recency")
plt.bar(df['recency'], df['AVGOrderAmount'])
plt.show()