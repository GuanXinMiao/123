# FunNow
funnow programs
import pandas as pd
import pymysql
from datetime import date
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g

def get_dataFrame_for_Register_sheet(search_start_date, search_end_date, cursor):
    query_string = """
        SELECT DATE_FORMAT(CONVERT_TZ(Member.CreatedAt , '+00:00','+08:00'), '%Y/%m/%d') 註冊日期,
        COUNT(Member.UID) Global_Total註冊,
        COUNT(CASE Member.RegType WHEN 0 THEN 1 END) AS Global_Email註冊,
        COUNT(CASE Member.RegType WHEN 2 THEN 1 END) AS Global_Facebook註冊,
        COUNT(CASE Member.RegType WHEN 4 THEN 1 END) AS Global_LINE註冊,
        COUNT(CASE Member.RegType WHEN 5 THEN 1 END) AS Global_Cathay註冊,
        COUNT(CASE Member.RegType WHEN 3 THEN 1 END) AS Global_GUEST註冊,
        COUNT(CASE WHEN Member.RegionID IN (1,3,6) THEN 1 END) AS TW_Total註冊,
        COUNT(CASE WHEN (Member.RegType = 0 AND Member.RegionID IN (1,3,6)) THEN 1 END) AS TW_Email註冊,
        COUNT(CASE WHEN (Member.RegType = 2 AND Member.RegionID IN (1,3,6)) THEN 1 END) AS TW_Facebook註冊,
        COUNT(CASE WHEN (Member.RegType = 4 AND Member.RegionID IN (1,3,6)) THEN 1 END) AS TW_LINE註冊,
        COUNT(CASE WHEN (Member.RegType = 5 AND Member.RegionID IN (1,3,6)) THEN 1 END) AS TW_Cathay註冊,
        COUNT(CASE WHEN (Member.RegType = 3 AND Member.RegionID IN (1,3,6)) THEN 1 END) AS TW_GUEST註冊,
        COUNT(CASE WHEN Member.RegionID = 4 THEN 1 END) AS HK_Total註冊,
        COUNT(CASE WHEN (Member.RegType = 0 AND Member.RegionID = 4) THEN 1 END) AS HK_Email註冊,
        COUNT(CASE WHEN (Member.RegType = 2 AND Member.RegionID = 4) THEN 1 END) AS HK_Facebook註冊,
        COUNT(CASE WHEN (Member.RegType = 4 AND Member.RegionID = 4) THEN 1 END) AS HK_LINE註冊,
        COUNT(CASE WHEN (Member.RegType = 5 AND Member.RegionID = 4) THEN 1 END) AS HK_Cathay註冊,
        COUNT(CASE WHEN (Member.RegType = 3 AND Member.RegionID = 4) THEN 1 END) AS HK_GUEST註冊,
        COUNT(CASE WHEN Member.RegionID = 11 THEN 1 END) AS KL_Total註冊,
        COUNT(CASE WHEN (Member.RegType = 0 AND Member.RegionID = 11) THEN 1 END) AS KL_Email註冊,
        COUNT(CASE WHEN (Member.RegType = 2 AND Member.RegionID = 11) THEN 1 END) AS KL_Facebook註冊,
        COUNT(CASE WHEN (Member.RegType = 4 AND Member.RegionID = 11) THEN 1 END) AS KL_LINE註冊,
        COUNT(CASE WHEN (Member.RegType = 5 AND Member.RegionID = 11) THEN 1 END) AS KL_Cathay註冊,
        COUNT(CASE WHEN (Member.RegType = 3 AND Member.RegionID = 11) THEN 1 END) AS KL_GUEST註冊,
        COUNT(CASE WHEN Member.RegionID IN (5,8) THEN 1 END) AS JP_Total註冊,
        COUNT(CASE WHEN (Member.RegType = 0 AND Member.RegionID IN (5,8)) THEN 1 END) AS JP_Email註冊,
        COUNT(CASE WHEN (Member.RegType = 2 AND Member.RegionID IN (5,8)) THEN 1 END) AS JP_Facebook註冊,
        COUNT(CASE WHEN (Member.RegType = 4 AND Member.RegionID IN (5,8)) THEN 1 END) AS JP_LINE註冊,
        COUNT(CASE WHEN (Member.RegType = 5 AND Member.RegionID IN (5,8)) THEN 1 END) AS JP_Cathay註冊,
        COUNT(CASE WHEN (Member.RegType = 3 AND Member.RegionID IN (5,8)) THEN 1 END) AS JP_GUEST註冊

        FROM Member
        LEFT JOIN Region ON Member.RegionID = Region.ID
        WHERE Member.CreatedAt BETWEEN CONVERT_TZ({}, '+00:00', '-08:00') AND CONVERT_TZ({}, '-08:00', '+08:00')
        GROUP BY DATE_FORMAT(CONVERT_TZ(Member.CreatedAt , '+00:00','+08:00'), '%Y/%m/%d')
    """.format(search_start_date, search_end_date)
    
    cursor.execute(query_string)
    df = pd.DataFrame(cursor.fetchall(), columns = ['註冊日期', 'Global_Total註冊', 'Global_Email註冊', 'Global_Facebook註冊', 'Global_LINE註冊',\
     'Global_Cathay註冊', 'Global_GUEST註冊', 'TW_Total註冊', 'TW_Email註冊', 'TW_Facebook註冊', 'TW_LINE註冊', 'TW_Cathay註冊', 'TW_GUEST註冊', \
     'HK_Total註冊', 'HK_Email註冊', 'HK_Facebook註冊', 'HK_LINE註冊', 'HK_Cathay註冊', 'HK_GUEST註冊', 'KL_Total註冊', 'KL_Email註冊', 'KL_Facebook註冊', \
     'KL_LINE註冊', 'KL_Cathay註冊', 'KL_GUEST註冊', 'JP_Total註冊', 'JP_Email註冊', 'JP_Facebook註冊', 'JP_LINE註冊', 'JP_Cathay註冊', 'JP_GUEST註冊'])

    return df
def get_dataFrame_for_DAU_sheet(search_start_date, search_end_date, cursor):

    query_string = """
        SELECT DATE_FORMAT(CONVERT_TZ(MPointAuth.CreatedAt , '+00:00','+08:00'), '%Y/%m/%d') 註冊日期,
        COUNT(Member.UID) Global_Total_AU,
        COUNT(CASE Member.RegType WHEN 0 THEN 1 END) AS Global_Email_AU,
        COUNT(CASE Member.RegType WHEN 2 THEN 1 END) AS Global_Facebook_AU,
        COUNT(CASE Member.RegType WHEN 3 THEN 1 END) AS Global_GUEST_AU,
        COUNT(CASE WHEN Member.RegionID IN (1,3,6) THEN 1 END) AS TW_Total_AU,
        COUNT(CASE WHEN (Member.RegType = 0 AND Member.RegionID IN (1,3,6)) THEN 1 END) AS TW_Email_AU,
        COUNT(CASE WHEN (Member.RegType = 2 AND Member.RegionID IN (1,3,6)) THEN 1 END) AS TW_Facebook_AU,
        COUNT(CASE WHEN (Member.RegType = 3 AND Member.RegionID IN (1,3,6)) THEN 1 END) AS TW_GUEST_AU,
        COUNT(CASE WHEN Member.RegionID = 4 THEN 1 END) AS HK_Total_AU,
        COUNT(CASE WHEN (Member.RegType = 0 AND Member.RegionID = 4) THEN 1 END) AS HK_Email_AU,
        COUNT(CASE WHEN (Member.RegType = 2 AND Member.RegionID = 4) THEN 1 END) AS HK_Facebook_AU,
        COUNT(CASE WHEN (Member.RegType = 3 AND Member.RegionID = 4) THEN 1 END) AS HK_GUEST_AU,
        COUNT(CASE WHEN Member.RegionID = 11 THEN 1 END) AS KL_Total_AU,
        COUNT(CASE WHEN (Member.RegType = 0 AND Member.RegionID = 11) THEN 1 END) AS KL_Email_AU,
        COUNT(CASE WHEN (Member.RegType = 2 AND Member.RegionID = 11) THEN 1 END) AS KL_Facebook_AU,
        COUNT(CASE WHEN (Member.RegType = 3 AND Member.RegionID = 11) THEN 1 END) AS KL_GUEST_AU,
        COUNT(CASE WHEN Member.RegionID IN (5,8) THEN 1 END) AS JP_Total_AU,
        COUNT(CASE WHEN (Member.RegType = 0 AND Member.RegionID IN (5,8)) THEN 1 END) AS JP_Email_AU,
        COUNT(CASE WHEN (Member.RegType = 2 AND Member.RegionID IN (5,8)) THEN 1 END) AS JP_Facebook_AU,
        COUNT(CASE WHEN (Member.RegType = 3 AND Member.RegionID IN (5,8)) THEN 1 END) AS JP_GUEST_AU
        FROM MPointAuth
        LEFT JOIN Member ON MPointAuth.UID = Member.UID
        LEFT JOIN Region ON Member.RegionID = Region.ID
        WHERE MPointAuth.CreatedAt BETWEEN CONVERT_TZ({}, '+00:00', '-08:00') AND CONVERT_TZ({}, '-08:00', '+08:00')
        GROUP BY DATE_FORMAT(CONVERT_TZ(MPointAuth.CreatedAt , '+00:00','+08:00'), '%Y/%m/%d')
    """.format(search_start_date, search_end_date)

    cursor.execute(query_string)
    df = pd.DataFrame(cursor.fetchall())

    return df
def get_dataFrame_for_WAU_sheet(search_start_date, search_end_date, cursor):
    
    query_string = """
        SELECT DATE_FORMAT(CONVERT_TZ(MPointAuth.CreatedAt , '+00:00','+08:00'), '%Y/%u') 註冊日期,
        COUNT(DISTINCT(Member.UID)) Global_Total_AU,
        COUNT(DISTINCT(CASE Member.RegType WHEN 0 THEN MPointAuth.UID END)) AS Global_Email_AU,
        COUNT(DISTINCT(CASE Member.RegType WHEN 2 THEN MPointAuth.UID END)) AS Global_Facebook_AU,
        COUNT(DISTINCT(CASE Member.RegType WHEN 3 THEN MPointAuth.UID END)) AS Global_GUEST_AU,
        COUNT(DISTINCT(CASE WHEN Member.RegionID IN (1,3,6) THEN MPointAuth.UID END)) AS TW_Total_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 0 AND Member.RegionID IN (1,3,6)) THEN MPointAuth.UID END)) AS TW_Email_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 2 AND Member.RegionID IN (1,3,6)) THEN MPointAuth.UID END)) AS TW_Facebook_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 3 AND Member.RegionID IN (1,3,6)) THEN MPointAuth.UID END)) AS TW_GUEST_AU,
        COUNT(DISTINCT(CASE WHEN Member.RegionID = 4 THEN MPointAuth.UID END)) AS HK_Total_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 0 AND Member.RegionID = 4) THEN MPointAuth.UID END)) AS HK_Email_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 2 AND Member.RegionID = 4) THEN MPointAuth.UID END)) AS HK_Facebook_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 3 AND Member.RegionID = 4) THEN MPointAuth.UID END)) AS HK_GUEST_AU,
        COUNT(DISTINCT(CASE WHEN Member.RegionID = 11 THEN MPointAuth.UID END)) AS KL_Total_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 0 AND Member.RegionID = 11) THEN MPointAuth.UID END)) AS KL_Email_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 2 AND Member.RegionID = 11) THEN MPointAuth.UID END)) AS KL_Facebook_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 3 AND Member.RegionID = 11) THEN MPointAuth.UID END)) AS KL_GUEST_AU,
        COUNT(DISTINCT(CASE WHEN Member.RegionID IN (5,8) THEN MPointAuth.UID END)) AS JP_Total_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 0 AND Member.RegionID IN (5,8)) THEN MPointAuth.UID END)) AS JP_Email_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 2 AND Member.RegionID IN (5,8)) THEN MPointAuth.UID END)) AS JP_Facebook_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 3 AND Member.RegionID IN (5,8)) THEN MPointAuth.UID END)) AS JP_GUEST_AU
        FROM MPointAuth
        LEFT JOIN Member ON MPointAuth.UID = Member.UID
        LEFT JOIN Region ON Member.RegionID = Region.ID
        WHERE MPointAuth.CreatedAt BETWEEN CONVERT_TZ({}, '+00:00', '-08:00') AND CONVERT_TZ({}, '-08:00', '+08:00')
        GROUP BY DATE_FORMAT(CONVERT_TZ(MPointAuth.CreatedAt , '+00:00','+08:00'), '%Y/%u')
    """.format(search_start_date, search_end_date)

    cursor.execute(query_string)
    df = pd.DataFrame(cursor.fetchall())

    return df

def get_dataFrame_for_MAU_sheet(search_start_date, search_end_date, cursor):

    query_string = """  
        SELECT DATE_FORMAT(CONVERT_TZ(MPointAuth.CreatedAt , '+00:00','+08:00'), '%Y/%m') 月份,
        COUNT(DISTINCT(Member.UID)) Global_Total_AU,
        COUNT(DISTINCT(CASE Member.RegType WHEN 0 THEN MPointAuth.UID END)) AS Global_Email_AU,
        COUNT(DISTINCT(CASE Member.RegType WHEN 2 THEN MPointAuth.UID END)) AS Global_Facebook_AU,
        COUNT(DISTINCT(CASE Member.RegType WHEN 3 THEN MPointAuth.UID END)) AS Global_GUEST_AU,
        COUNT(DISTINCT(CASE WHEN Member.RegionID IN (1,3,6) THEN MPointAuth.UID END)) AS TW_Total_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 0 AND Member.RegionID IN (1,3,6)) THEN MPointAuth.UID END)) AS TW_Email_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 2 AND Member.RegionID IN (1,3,6)) THEN MPointAuth.UID END)) AS TW_Facebook_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 3 AND Member.RegionID IN (1,3,6)) THEN MPointAuth.UID END)) AS TW_GUEST_AU,
        COUNT(DISTINCT(CASE WHEN Member.RegionID = 4 THEN MPointAuth.UID END)) AS HK_Total_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 0 AND Member.RegionID = 4) THEN MPointAuth.UID END)) AS HK_Email_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 2 AND Member.RegionID = 4) THEN MPointAuth.UID END)) AS HK_Facebook_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 3 AND Member.RegionID = 4) THEN MPointAuth.UID END)) AS HK_GUEST_AU,
        COUNT(DISTINCT(CASE WHEN Member.RegionID = 11 THEN MPointAuth.UID END)) AS KL_Total_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 0 AND Member.RegionID = 11) THEN MPointAuth.UID END)) AS KL_Email_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 2 AND Member.RegionID = 11) THEN MPointAuth.UID END)) AS KL_Facebook_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 3 AND Member.RegionID = 11) THEN MPointAuth.UID END)) AS KL_GUEST_AU,
        COUNT(DISTINCT(CASE WHEN Member.RegionID IN (5,8) THEN MPointAuth.UID END)) AS JP_Total_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 0 AND Member.RegionID IN (5,8)) THEN MPointAuth.UID END)) AS JP_Email_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 2 AND Member.RegionID IN (5,8)) THEN MPointAuth.UID END)) AS JP_Facebook_AU,
        COUNT(DISTINCT(CASE WHEN (Member.RegType = 3 AND Member.RegionID IN (5,8)) THEN MPointAuth.UID END)) AS JP_GUEST_AU
        FROM MPointAuth
        LEFT JOIN Member ON MPointAuth.UID = Member.UID
        LEFT JOIN Region ON Member.RegionID = Region.ID
        WHERE MPointAuth.CreatedAt BETWEEN CONVERT_TZ({}, '+00:00', '-08:00') AND CONVERT_TZ({}, '-08:00', '+08:00')
        GROUP BY DATE_FORMAT(CONVERT_TZ(MPointAuth.CreatedAt , '+00:00','+08:00'), '%Y/%m')
    """.format(search_start_date, search_end_date)

    cursor.execute(query_string)
    df = pd.DataFrame(cursor.fetchall())

    return df

def get_dataFrame_for_FirstBuy_sheet(search_start_date, search_end_date, cursor):
    query_string = """
        SELECT DATE_FORMAT(CONVERT_TZ(Orders.CreatedAt , '+00:00','+08:00'), '%Y/%u') WEEK, 
        COUNT(DISTINCT(Member.UID)) Global_Members_FirstBuy,
        COUNT(DISTINCT(CASE WHEN Member.RegionID IN (1,3,6) THEN Orders.UID END)) AS TW_Members_FirstBuy,
        COUNT(DISTINCT(CASE WHEN Member.RegionID = 4 THEN Orders.UID END)) AS HK_Members_FirstBuy,
        COUNT(DISTINCT(CASE WHEN Member.RegionID = 11 THEN Orders.UID END)) AS KL_Members_FirstBuy,
        COUNT(DISTINCT(CASE WHEN Member.RegionID IN (5,8) THEN Orders.UID END)) AS JP_Members_FirstBuy
        FROM Orders
        LEFT JOIN Member ON Member.UID = Orders.UID
        LEFT JOIN Region ON Member.RegionID = Region.ID
        LEFT JOIN Txn ON Txn.OrdersID = Orders.ID
        WHERE Member.CreatedAt BETWEEN CONVERT_TZ({}, '+00:00', '-08:00') AND CONVERT_TZ({}, '-08:00', '+08:00') #本週註冊
        AND Orders.CreatedAt BETWEEN CONVERT_TZ({}, '+00:00', '-08:00') AND CONVERT_TZ({}, '-08:00', '+08:00') ##本週交易
        AND DATE_FORMAT(CONVERT_TZ(Orders.CreatedAt , '+00:00','+08:00'), '%Y/%u') = DATE_FORMAT(CONVERT_TZ(Member.CreatedAt , '+00:00','+08:00'), '%Y/%u') #註冊週與交易週相等
        AND Orders.status = 2 AND Txn.Amount > 0
        GROUP BY DATE_FORMAT(CONVERT_TZ(Orders.CreatedAt , '+00:00','+08:00'), '%Y/%u')
    """.format(search_start_date, search_end_date,search_start_date, search_end_date)


    cursor.execute(query_string)
    df = pd.DataFrame(cursor.fetchall())
    df[6] = df[1] - df[2] - df[3] - df[4] - df[5]

    return df

connect = pymysql.connect(host='funnow-prod-go-20190514-for-metabase.cqajv42imxem.ap-northeast-1.rds.amazonaws.com', port=3306, user='funnow', passwd='b5D_C4-7f', db='FunNow_V2')
cursor = connect.cursor()
df = get_dataFrame_for_FirstBuy_sheet('"2019-01-01"', '"2019-12-17"',cursor)
sheet_name = "FirstBuy"

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("trySheets-708d0bd4d3dc.json", scope)
client = gspread.authorize(creds)
print(df)
d2g.upload(df = df, gfile = '1skrKAOLZLPoul59kgjbY6jQwE60VxVCOcIwG2fZJCXo', start_cell = 'A2', wks_name = sheet_name , col_names = False, row_names = False, clean = False, credentials = creds, df_size = False)
