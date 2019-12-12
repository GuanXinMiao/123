"""
Author : Guan Xin Miao
Output: https://docs.google.com/spreadsheets/d/1-fNBj4V266e6D5HNGRNZ6Kfi02kVXfRsnlvEa7PNflQ/edit?ts=5df1ab95#gid=1236490588
Purpose: store
 all database variable information to google sheet document
"""
import pandas as pd
import pymysql
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g

#
#
def store_data_to_google_sheet(database_list, row_nums, sheet_name, credentials, cursor):

	for i in range(len(database_list)):
		sqlStr = """
		SHOW FULL columns FROM {}
		""".format(database_list[i])
		cursor.execute(sqlStr)
		print('executed\n')


		df = pd.DataFrame(list(cursor.fetchall()))


		NameCol = pd.DataFrame(df[0])
		TypeCol = pd.DataFrame(df[1])
		isNullCol = pd.DataFrame(df[3])
		DefaultCol = pd.DataFrame(df[5])
		commentCol = pd.DataFrame(df[8])


		data_cols = [NameCol, TypeCol, isNullCol, DefaultCol, commentCol]
		col_letter = ['E', 'H', 'K', 'G', 'N']

		for col in range(len(data_cols)):
			d2g.upload(df = data_cols[col], gfile = '1-fNBj4V266e6D5HNGRNZ6Kfi02kVXfRsnlvEa7PNflQ', start_cell = col_letter[col] + row_nums[i], wks_name =  sheet_name, col_names = False, row_names = False, clean = False, credentials = creds, df_size = False)
			print(col)

		print(database_list[i] + " finished upload")

def main():
	conn = pymysql.connect(host='funnow-prod-go-20190514-for-metabase.cqajv42imxem.ap-northeast-1.rds.amazonaws.com', port=3306, user='funnow', passwd='b5D_C4-7f', db='FunNow_V2')
	cursor = conn.cursor()

	#set google sheet upload
	scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
	creds = ServiceAccountCredentials.from_json_keyfile_name("trySheets-708d0bd4d3dc.json", scope)
	client = gspread.authorize(creds)

	database_list = ['Resource', 'ResourceGroup']
	row_nums = ['6', '14']

	sheet = 'Resource'

	store_data_to_google_sheet(database_list, row_nums, sheet, creds, cursor)

	print('program finished')

if __name__ == '__main__':
	main()
