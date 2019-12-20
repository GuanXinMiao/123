"""
Author: Guan Xin Miao
Date: 17/12/2019
OutPut: https://docs.google.com/spreadsheets/d/1skrKAOLZLPoul59kgjbY6jQwE60VxVCOcIwG2fZJCXo/edit?folder=0AJ9hIR97Msg6Uk9PVA#gid=412779160
Purpose: 
		1.Manage and store data from funNow database to google sheet
		2.Store data from Itunes(ios download), Google play(Android download) and CleverTap(Uninstall) using web crawler

"""

import pandas as pd
import pymysql
from datetime import date
import datetime
import os
import smtplib
import gspread
import math
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

#----------------------------------- dataFrame from sql functions --------------------------------------#

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

#-------------------------------------------------------------------------------------------------------------------------------------------------------#

def send_mail(send_from, send_to, subject, text, filepath, password):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))
    filename = os.path.basename(filepath)
    attachment = open(filepath, "rb")
    part = MIMEBase('application', "octet-stream")
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename= {}'.format(filename))
    msg.attach(part)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(send_from, password)
    text = msg.as_string()
    server.sendmail(send_from, send_to, text)
    server.quit()
#-------------------------------------------- store data functions --------------------------------------------------------------------------------------#
def store_data_register(dataframe, creds, date_diff):

	sheet_name = "Register"
	start_row = str(date_diff - 11)
	d2g.upload(df = dataframe, gfile = '1skrKAOLZLPoul59kgjbY6jQwE60VxVCOcIwG2fZJCXo', start_cell = "A" + start_row, wks_name = sheet_name , col_names = False, row_names = False, clean = False, credentials = creds, df_size = False)
	print(sheet_name + " finished!")

def store_data_DAU(dataframe, creds, date_diff):

	sheet_name = 'DAU'
	start_row = str(date_diff - 11)
	d2g.upload(df = dataframe, gfile = '1skrKAOLZLPoul59kgjbY6jQwE60VxVCOcIwG2fZJCXo', start_cell = "A" + start_row, wks_name = sheet_name , col_names = False, row_names = False, clean = False, credentials = creds, df_size = False)
	print(sheet_name + " finished!")

def store_data_WAU(dataframe, creds, week_diff):

	sheet_name = 'WAU'
	start_row = str(week_diff - 1)
	d2g.upload(df = dataframe, gfile = '1skrKAOLZLPoul59kgjbY6jQwE60VxVCOcIwG2fZJCXo', start_cell = "A" + start_row, wks_name = sheet_name , col_names = False, row_names = False, clean = False, credentials = creds, df_size = False)
	print(sheet_name + " finished!")

def store_data_MAU(dataframe, creds, sheet, new_month):

	sheet_name = 'MAU'
	start_row = ''
	if new_month: start_row = str(len(sheet.get_all_records()) + 1)
	else:start_row = str(len(sheet.get_all_records()))
	d2g.upload(df = dataframe, gfile = '1skrKAOLZLPoul59kgjbY6jQwE60VxVCOcIwG2fZJCXo', start_cell = "A" + start_row, wks_name = sheet_name , col_names = False, row_names = False, clean = False, credentials = creds, df_size = False)
	print(sheet_name + " finished!")

def store_data_firstBuy(dataframe, creds, week_diff):
	sheet_name = 'FirstBuy'
	start_row = str(week_diff - 1)
	d2g.upload(df = dataframe, gfile = '1skrKAOLZLPoul59kgjbY6jQwE60VxVCOcIwG2fZJCXo', start_cell = "A" + start_row, wks_name = sheet_name , col_names = False, row_names = False, clean = False, credentials = creds, df_size = False)
	print(sheet_name + " finished!")

#-------------------------------------------------------------------------------------------------------------------------------------------------------------#

def main():
	#set dates
	project_start_date = date(2019, 1, 1)
	current_date = date.today()
	first = current_date.replace(day=1)
	last_month = first - datetime.timedelta(days=1)
	last_month_first = '"' + str(last_month.year) + '-' + str(last_month.month) + '-' + '1' + '"'
	yesterday = current_date - datetime.timedelta(days = 1)
	date_diff = (yesterday - project_start_date).days
	week_diff = math.ceil(date_diff/7)
	two_weeks_before = current_date - datetime.timedelta(weeks = 2)
	is_new_month = (yesterday.day < 7)
	yesterday = '"' + str(yesterday) + '"'

	two_weeks_before = '"' + str(two_weeks_before) + '"'
	#set mysql connections

	connect = pymysql.connect(host='funnow-prod-go-20190514-for-metabase.cqajv42imxem.ap-northeast-1.rds.amazonaws.com', port=3306, user='funnow', passwd='**********', db='FunNow_V2')
	cursor = connect.cursor()
	#store dataFrame to a python list

	df_list = [get_dataFrame_for_Register_sheet(two_weeks_before, yesterday, cursor),\
	get_dataFrame_for_DAU_sheet(two_weeks_before, yesterday, cursor),\
	get_dataFrame_for_WAU_sheet(two_weeks_before, yesterday, cursor),\
	get_dataFrame_for_MAU_sheet(last_month_first, yesterday, cursor),\
	get_dataFrame_for_FirstBuy_sheet(two_weeks_before, yesterday, cursor)]

	#set google sheet information
	scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
	creds = ServiceAccountCredentials.from_json_keyfile_name("trySheets-708d0bd4d3dc.json", scope)
	client = gspread.authorize(creds)
	sheet_file = client.open("DownloadAndActiveUsers")

	#store data to google sheet
	store_data_register(df_list[0], creds, date_diff)
	store_data_DAU(df_list[1], creds, date_diff)
	store_data_WAU(df_list[2], creds, week_diff)
	store_data_MAU(df_list[3], creds, sheet_file.get_worksheet(10), is_new_month)
	store_data_firstBuy(df_list[4], creds, week_diff)
	print("Program finished")

if __name__ == '__main__':
	main()
