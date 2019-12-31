# FunNow
funnow programs
import pandas as pd
import datetime
from datetime import date
import pymysql
from sqlalchemy import create_engine

def store_ProductTag_data():
	engine = create_engine('mysql+pymysql://funnow_go:***********@13.231.106.254:3301/FunNow_V2?charset=utf8mb4')

	conn = pymysql.connect(host='funnow-prod-go-20190514-for-metabase.cqajv42imxem.ap-northeast-1.rds.amazonaws.com', port=3306, user='funnow', passwd='*********', db='FunNow_V2')
	cursor = conn.cursor()

	cursor.execute("""SELECT Product.ID PID, Product.Name, TagName.Tag, Region.Name Region, Branch.BID BID, Branch.Name BranchName, PPrice.Weekday, PPrice.minimumPrice
	FROM Product
	LEFT JOIN Branch ON Product.BID = Branch.BID
	LEFT JOIN (
	SELECT Product.ID AS Pid, ProductTag.Name Tag
	           FROM Product
	           LEFT JOIN ProductToTag ON ProductToTag.ProductID = Product.ID
	           LEFT JOIN ProductTag ON ProductTag.ID = ProductToTag.TagID
	           LEFT JOIN ProductTagGroup ON ProductTagGroup.ID = ProductTag.GroupID
	           WHERE ProductTagGroup.ID = 2
	)TagName ON Product.ID = TagName.Pid
	LEFT JOIN BranchToRegion ON Branch.BID = BranchToRegion.BID
	LEFT JOIN Region ON BranchToRegion.RegionID = Region.ID
	LEFT JOIN BranchToIndType ON Branch.BID = BranchToIndType.BID
	LEFT JOIN (
	SELECT Product.ID PID, ProductPriceWeekday.Weekday, MIN(ProductPrice.Price) minimumPrice
    FROM Product
    LEFT JOIN ProductPriceWeekday ON Product.ID = ProductPriceWeekday.ProductID
    LEFT JOIN ProductPrice ON ProductPriceWeekday.SetID = ProductPrice.SetID
    WHERE ProductPriceWeekday.Weekday = 5 or ProductPriceWeekday.Weekday = 6
    GROUP BY Product.ID, ProductPriceWeekday.Weekday
	)PPrice ON Product.ID = PPrice.PID
	WHERE BranchToIndType.IndTypeID <> 0 AND Weekday IS NOT NULL""")
	df = pd.DataFrame(cursor.fetchall(), columns = ['PID', 'ProductName', 'Tag','Region', 'BID', 'BranchName', 'Weekday', 'MinimumPrice'])

	df.to_sql(name = 'ProductWeekdayPrice', con = engine, if_exists = 'append', index = False)

def get_lodging_Data():

	conn = pymysql.connect(host = '13.231.106.254', port = 3301, user = 'funnow_go', passwd = '*********', db = 'FunNow_V2')
	cursor = conn.cursor()
	query = """
	SELECT Friday.PID, Friday.ProductName, Friday.ViewAmount, Friday.Tag, Friday.Region, Friday.BID, Friday.BranchName, 
	Friday.MinimumPrice as FridayMiniPrice, 
	Saturday.MinimumPrice as SaturdayMiniPrice
	FROM(
		SELECT ProductRank.PID, ViewAmount, ProductName, Tag, Region, BID, BranchName, MinimumPrice
		FROM(
			SELECT PID, COUNT(*) ViewAmount
			FROM ProductDetailViewed
			WHERE ProductDetailViewed.CreatedAt > '2019-12-02'
			GROUP BY PID)ProductRank
		LEFT JOIN ProductWeekdayPrice ON ProductRank.PID = ProductWeekdayPrice.PID
		WHERE Weekday = 5 AND Tag = '住宿'
	)Friday
	LEFT JOIN (
	SELECT ProductRank.PID, ViewAmount, ProductName, Tag, Region, BID, BranchName, MinimumPrice
	FROM(
		SELECT PID, COUNT(*) ViewAmount
		FROM ProductDetailViewed
		WHERE ProductDetailViewed.CreatedAt > '2019-12-02'
		GROUP BY PID)ProductRank
		LEFT JOIN ProductWeekdayPrice ON ProductRank.PID = ProductWeekdayPrice.PID
		WHERE Weekday = 6 AND Tag = '住宿'
	)Saturday ON Saturday.PID = Friday.PID
	ORDER BY Friday.ViewAmount DESC 
	LIMIT 50
	"""
	cursor.execute(query)

	df = pd.DataFrame(cursor.fetchall(), columns = ['PID', 'ProductName', 'ViewAmount', 'Tag', 'Region', 'BID', 'BranchName', 'FridayMiniPrice', 'SaturdayMiniPrice'])
	df.to_csv('Top50.csv', index = True, encoding = 'utf-8')
