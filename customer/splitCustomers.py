import pymysql
import pandas as pd 
from datetime import date
import datetime
def split_people(OrderAmount, recency):
	"""
	1為新顧客
	2為一次性消費顧客
	3為常貴客
	4為先前客
	"""
	if OrderAmount < 2:
		if recency < 11:
			return 1
		else:
			return 2
	else:
		if recency < 11:
			return 3
		else:
			return 4

connect = pymysql.connect(host = "13.231.106.254", port = 3301, user = 'funnow_go', passwd = 'y3n42hVf4yUJUy6g', db = 'FunNow_V2')
cursor = connect.cursor()

today = date.today()
how_many_days_ago = 90
start_search_date = today - datetime.timedelta(days = how_many_days_ago)
start_date = str(start_search_date).replace("-", "/")

query = """
SELECT UID, Email, Category, MIN(DateD) as recency, COUNT(*)OrderAmount
FROM(
SELECT UID, Category, Email, DATEDIFF(DATE_FORMAT(CURDATE(), '%Y/%m/%d'), DATE_FORMAT(OrderDate,'%Y/%m/%d') ) as DateD
FROM PersonalBehavior
WHERE (Region = '大台北' OR Region = '台中' OR Region = '桃園' OR Region = '台南｜高雄')
AND (OrderDate >= '{}') AND Category in ('住宿', '休息', '按摩', '餐廳','酒吧', '女性', '美髮')
)T2
GROUP By UID, Category, Email
""".format(start_date)

cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns = [ 'UID', 'Email', 'Category', 'recency', 'OrderAmount'])
ndfl = []
for index, row in df.iterrows():
	Category = ''
	if row['Category'] == '女性' or row['Category'] == '美髮':Category = '(女性美髮)'
	elif row['Category'] == '餐廳' or row['Category'] == '酒吧': Category = '(餐廳酒吧)'
	else:Category = "(%s)"%row['Category']
	ndfl.append({'UID': row['UID'], 'Email':row['Email'], 'Category':Category, 'recency':row['recency'], 'OrderAmount':row['OrderAmount']})
new_df = pd.DataFrame(ndfl)
new_df.to_csv('PersonalBehavior.csv', index = False)

new_customer = []
one_order_customer = []
importent_customer = []
previously_customer = []
for index, row in df.iterrows():
    customer_type = split_people(row['OrderAmount'],row['recency'])
    if customer_type == 1:
    	new_customer.append({'UID':row['UID'], 'Email':row['Email']})
    if customer_type == 2:
    	one_order_customer.append({'UID':row['UID'], 'Email':row['Email']})
    if customer_type == 3:
    	importent_customer.append({'UID':row['UID'], 'Email':row['Email']})
    if customer_type == 4:
    	previously_customer.append({'UID':row['UID'], 'Email':row['Email']})
new_customerDF = pd.DataFrame(new_customer)
one_order_customerDF = pd.DataFrame(one_order_customer)
importent_customerDF = pd.DataFrame(importent_customer)
previously_customerDF = pd.DataFrame(previously_customer)
new_customerDF.to_csv('new_customer.csv')
one_order_customerDF.to_csv('one_order_customer.csv')
importent_customerDF.to_csv('importent_customer.csv')
previously_customerDF.to_csv('previously_customer.csv')
print(new_customerDF)
print(one_order_customerDF)
print(importent_customerDF)
print(previously_customerDF)
