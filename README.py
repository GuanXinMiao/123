# FunNow
funnow programs
"""
Author: GuanXin Miao
Date: 25/12/2019
Purpose:
Create New 'FirstBuy' Table to FunNow analysis connection
"""

import pymysql
import pandas as pd
from datetime import date
import datetime
from sqlalchemy import create_engine
import sqlalchemy
import os

# database connection for reading
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
conn1 = pymysql.connect(host = '13.231.106.254', port = 3301, user = 'funnow_go', passwd = 'y3n42hVf4yUJUy6g', db = 'FunNow_V2')
cursor1 = conn1.cursor()

conn2 = pymysql.connect(host='funnow-prod-go-20190514-for-metabase.cqajv42imxem.ap-northeast-1.rds.amazonaws.com', port=3306, user='funnow', passwd='b5D_C4-7f', db='FunNow_V2')
cursor2 = conn2.cursor()

today = date.today()
yesterday = today - datetime.timedelta(days = 1)

query = """
SELECT OrdersProduct.ID AS OrdersProduct_ID, Txn.ExOrderID HashOrderID, Orders.ID Orders_ID, 
       Orders.UID AS User_ID, 
       IF(OrdersProduct.ID = Orders.OrdersProductID, '主商品', '加購') Product_Type, 
       CASE Orders.Status                      
                              WHEN 2 THEN '交易完成'
                              WHEN 6 THEN '銷單'
                              WHEN 7 THEN '退訂'
                              WHEN 8 THEN '已轉單'
                              WHEN 9 THEN 'APP銷單'
                              WHEN 10 THEN 'APP退訂' END AS Order_Status,
        OrdersProduct.ProductID Product_ID, 
        Product.Name AS Product_Name, 分類標籤.商品分類 Product_Category, 活動標籤.商品分類 Campaign_Tag,
        OrdersProduct.Price Product_Price, 
        OrdersProduct.Quantity Product_Quantity, 
        IF(OrdersProduct.ID = Orders.OrdersProductID, OrdersUpgrade.UpgradeName, NULL)  Update_Name, 
        IF(OrdersProduct.ID = Orders.OrdersProductID, OrdersUpgrade.Price, NULL) Upgrade_Price, 
        IF(OrdersProduct.ID = Orders.OrdersProductID, OrdersUpgrade.Quantity, NULL)  Update_Quantity, 
        IF(OrdersProduct.ID = Orders.OrdersProductID, Orders.TotalAmount, NULL) Order_Amount,
        IF(OrdersProduct.ID = Orders.OrdersProductID, Orders.Bonus, NULL)  Bonus,
        IF(OrdersProduct.ID = Orders.OrdersProductID, Orders.Discount, NULL) Discount,
        IF(OrdersProduct.ID = Orders.OrdersProductID, IF(Orders.Status = 7, Txn.Amount*0.3, IF(Orders.Status = 6, Txn.Amount*0, Txn.Amount)), NULL) Txn_Amount,  #Txn.Amount 實際交易金額
        CASE Txn.CurrencyID
        WHEN 0 THEN 'TWD'
        WHEN 1 THEN 'HKD'
        WHEN 2 THEN 'JPY'
        WHEN 3 THEN 'MYR' END AS Txn_Currency,
        CASE Region.ID
        WHEN 1 THEN 'TWD'
        WHEN 3 THEN 'TWD'
        WHEN 4 THEN 'HKD'
        WHEN 5 THEN 'JPY' 
        WHEN 6 THEN 'TWD'
        WHEN 8 THEN 'JPY' 
        WHEN 11 THEN 'MYR' END AS Order_Currency,
        Orders.BID AS Branch_ID, Branch.Name AS Branch_Name,IndustryType.Name AS Branch_Type,
       Region.ID AS Region_ID, Region.Name AS Region_Name,
       DATE_FORMAT(CONVERT_TZ(Orders.BookingTime , '+00:00','+08:00'), '%Y/%m/%d %T') AS Appointment_Time, 
       DATE_FORMAT(CONVERT_TZ(Orders.CreatedAt , '+00:00','+08:00'), '%Y/%m/%d %T') AS Order_Createtime,
       OrdersVIP.VIPID AS VIP_ID, Product.Duration 方案時間, ProductUpgrade.Duration 升級時間,
       PromoCode.ID PromoCodeID, PromoCode.PromoTitle, PromoCode.Code PromoCode  
FROM OrdersProduct
LEFT JOIN Orders ON Orders.ID = OrdersProduct.OrdersID
LEFT JOIN OrdersVIP ON Orders.ID = OrdersVIP.OrdersID
LEFT JOIN OrdersUpgrade ON Orders.ID = OrdersUpgrade.OrdersID
LEFT JOIN Product ON OrdersProduct.ProductID = Product.ID
LEFT JOIN (SELECT Product.ID AS 商品id, ProductTag.Name 商品分類
           FROM Product
           LEFT JOIN ProductToTag ON ProductToTag.ProductID = Product.ID
           LEFT JOIN ProductTag ON ProductTag.ID = ProductToTag.TagID
           LEFT JOIN ProductTagGroup ON ProductTagGroup.ID = ProductTag.GroupID
           WHERE ProductTagGroup.ID = 2) 分類標籤 ON 分類標籤.商品id = Product.ID
LEFT JOIN (SELECT Product.ID AS 商品id, ProductTag.Name 商品分類
           FROM Product
           LEFT JOIN ProductToTag ON ProductToTag.ProductID = Product.ID
           LEFT JOIN ProductTag ON ProductTag.ID = ProductToTag.TagID
           LEFT JOIN ProductTagGroup ON ProductTagGroup.ID = ProductTag.GroupID
           WHERE ProductTagGroup.ID = 32) 活動標籤 ON 活動標籤.商品id = Product.ID
LEFT JOIN Branch ON Branch.BID = Orders.BID
LEFT JOIN BranchToIndType ON Branch.BID = BranchToIndType.BID
LEFT JOIN IndustryType ON BranchToIndType.IndTypeID = IndustryType.ID
LEFT JOIN Region ON Region.ID = Orders.RegionID
LEFT JOIN Member ON Member.UID = Orders.UID
LEFT JOIN Txn ON Txn.OrdersID = Orders.ID
LEFT JOIN ProductUpgrade ON ProductUpgrade.ID = OrdersUpgrade.UpgradeID
LEFT JOIN PromoDiscount ON Orders.ID = PromoDiscount.OrderID
Left JOIN PromoCode ON PromoDiscount.PromoCodeID = PromoCode.ID
WHERE Orders.Status = 2 AND Orders.CreatedAt between CONVERT_TZ( '{}' , '+00:00', '-08:00') AND CONVERT_TZ( '{}' , '+00:00', '-08:00')
AND Orders.CreatedAt = (SELECT MIN(o2.CreatedAt)
                              FROM Orders o2
                              WHERE o2.UID = Orders.UID AND o2.status = 2)
""".format(yesterday, today)

cursor2.execute(query)
df = pd.DataFrame(cursor2.fetchall(), columns = ['OrdersProductID', 'HashOrderID', 'OrderID', 'UID', 'ProductType', 'OrderStatus', 'ProductID', 'ProductName',\
'ProductCategory', 'CampaignTag', 'ProductPrice', 'ProductQuantity', 'UpdateName', 'UpgradePrice', 'UpdateQuantity', 'OrderAmount', 'Bonus', 'Discount','TxnAmount',\
 'TxnCurrency', 'OrderCurrency','BranchID','BranchName', 'BranchType', 'RegionID', 'RegionName', 'AppointmentTime', 'OrderCreateTime', 'VIPID', 'caseTime',\
  'updateTime', 'PromoCodeID', 'PromoTitle', 'promoCode'])

#store data
engine = create_engine("mysql+pymysql://funnow_go:y3n42hVf4yUJUy6g@13.231.106.254:3301/FunNow_V2?charset=utf8mb4")
con = engine.connect()
df.to_sql(name = "FirstBuy", con = con, if_exists = 'append', index = False)

