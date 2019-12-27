# FunNow
funnow programs
"""
Author : Guan Xin Miao
Date: 26/12/2019
Purpose: Store data for personal behavior analysis
"""
import pandas as pd
import datetime
from datetime import date
import pymysql
from sqlalchemy import create_engine

today = date.today()
yesterday = today - datetime.timedelta(days = 1)

def get_dataFrame(cursor):
	global today, yesterday
	query = """
	SELECT Member.UID, Orders.Email, Region.Name, COUNT(*) as TotalOrderAmount, DATE_FORMAT(CONVERT_TZ(Orders.CreatedAt , '+00:00','+08:00'), '%Y/%m/%d') AS OrderMonth,
	SUM(Orders.TotalAmount) SumTotalAmount, SUM(Orders.TotalAmount)/COUNT(*) Average, Tag.tagName
	FROM Member
	LEFT JOIN Orders ON Member.UID = Orders.UID
	LEFT JOIN Product ON Product.ID = Orders.ProductID
	LEFT JOIN Branch ON Product.BID = Branch.BID
	LEFT JOIN BranchToRegion ON Branch.BID = BranchToRegion.BID
	LEFT JOIN Region ON BranchToRegion.RegionID = Region.ID
	LEFT JOIN (SELECT Product.ID AS ProductID, ProductTag.Name tagName
	           FROM Product
	           LEFT JOIN ProductToTag ON ProductToTag.ProductID = Product.ID
	           LEFT JOIN ProductTag ON ProductTag.ID = ProductToTag.TagID
	           LEFT JOIN ProductTagGroup ON ProductTagGroup.ID = ProductTag.GroupID
	           WHERE ProductTagGroup.ID = 2)Tag ON Orders.ProductID = Tag.ProductID
	WHERE Orders.Status = 2 AND Orders.CreatedAt BEWTEEN '{}' AND '{}'
	GROUP BY Member.UID, Region.Name, Tag.tagName, DATE_FORMAT(CONVERT_TZ(Orders.CreatedAt , '+00:00','+08:00'), '%Y/%m') 
	""".format(yesterday, today)
	cursor.execute(query)
	print("finished")
	df = pd.DataFrame(cursor.fetchall(), columns = ['UID', 'Email', 'Region', 'OrderAmount','OrderDate', 'GMV', 'AVG', 'Category'])
	return df

def store_data(connect, df, name):
	df.to_sql(name = name, con = connect, if_exists = 'append', index = False)
	print('finished stored')

def main():
	#connect to database for collecting 
	connect = pymysql.connect(host='funnow-prod-go-20190514-for-metabase.cqajv42imxem.ap-northeast-1.rds.amazonaws.com', port=3306, user='funnow', passwd='*******', db='FunNow_V2')
	cursor = connect.cursor()
	#connect to database for store
	engine = create_engine("mysql+pymysql://funnow_go:***********@13.231.106.254:3301/FunNow_V2?charset=utf8mb4")
	con = engine.connect()
	#df = get_dataFrame(cursor)

	table_name = 'PersonalBehavior'
	#store_data(con, df, table_name)

if __name__ == '__main__':
	main()
