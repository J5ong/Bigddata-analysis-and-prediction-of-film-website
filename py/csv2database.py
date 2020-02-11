#coding=utf-8
import pymysql

config = {
			'host':'127.0.0.1',
			'port':3306,
			'user':'root',
			'passwd':'passwd',
			'charset':'utf8mb4',
			'local_infile':1
		}

conn = pymysql.connect(**config)
cur = conn.cursor()

def load_csv(csv_file_path,table_name,database='bigdata'):
	with open(file_path,'r',encoding='utf-8') as file:
		#读取csv文件第一行字段名，创建表
		reader = file.readline()
		print(reader)
		b = reader.split(',')
		colum = ''
		for a in b:
			colum = colum + a + ' varchar(255),'
		colum = colum[:-1]

		#编写sql
		create_sql = 'create table if not exists ' + table_name + ' ' + '(' + colum + ')' + ' DEFAULT CHARSET=utf8'
		data_sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' IGNORE 1 LINES"% (csv_file_path,table_name)

		# "LOAD DATA LOCAL INFILE 'C:\\Users\\松少\\Desktop\\gkd\\完成的表格\\电影类型数量1.csv' INTO TABLE hehe FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES"

		#使用数据库
		cur.execute('use %s' % database)
		#设置编码格式
		cur.execute('SET NAMES utf8;')
		cur.execute('SET character_set_connection=utf8;')
		#执行create_sql，创建表
		cur.execute(create_sql)
		#执行data_sql，导入数据
		cur.execute(data_sql)

		conn.commit()
		#关闭连接
		conn.close()
		cur.close()

file_path = r'C:\\Users\\松少\\Desktop\\gkd\\完成的表格\\test_collection.csv'	
table_name = 'test_collection2'
load_csv(file_path,table_name)