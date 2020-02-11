#coding=utf-8
import json
import pymysql

def inquire(table_name):
	conn = pymysql.connect("localhost","root","passwd","bigdata")
	cursor = conn.cursor()
	sql = "SELECT * FROM %s"%(table_name)
	cursor.execute(sql)
	data = cursor.fetchall()
	data_json = json.dumps(data)
	# print(data)
	# print(j)
	cursor.close()
	conn.close()
	return data_json