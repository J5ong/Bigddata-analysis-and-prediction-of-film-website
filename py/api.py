#coding=utf-8

import pymysql
# from prediction_model import MoviePredictionExample,sim
from prediction_model import sim
from flask import Flask,json,request,make_response

app = Flask(__name__)

# 开启调试模式
app.debug = True

# 数据库密码(用户名默认为root)
password="输入你的数据库密码"

@app.route("/inquire", methods=['GET','POST'])
def inquire():
	if request.method == 'GET':
		table_name = request.args.get("name")
		result = inquire_all(table_name)
		resp = resp_add_header(result)
		return resp
	elif request.method == 'POST':
		table_name = request.form['name']
		chart_type = request.form['type']
		year_list = request.form['year'].split(',') 
		# 构造sql语句,并请求返回json格式的结果
		result = inquire_data(table_name,chart_type,year_list)
		resp = resp_add_header(result)
		return resp

@app.route("/grade", methods=['GET','POST'])
def grade():
	if request.method == 'GET':
		table_name = 'douban_jinjian'
		result = grade_data(table_name)
		resp = resp_add_header(result)
		return resp
	elif request.method == 'POST':
		type_list = request.form['type'].split(',') 
		country_list = request.form['country'].split(',')
		language_list = request.form['language'].split(',')
		result = grade_data_select(type_list,country_list,language_list)
		resp = resp_add_header(result)
		return resp

@app.route("/run", methods=['POST'])
def run():
	if request.method == 'POST':
		# print(request.form)
		attendence1 = request.form['attendence1']
		attendence2 = request.form['attendence2']
		attendence3 = request.form['attendence3']
		attendence4 = request.form['attendence4']
		attendence5 = request.form['attendence5']
		attendence6 = request.form['attendence6']
		attendence7 = request.form['attendence7']
		film_ratio1 = request.form['film_ratio1']
		film_ratio2 = request.form['film_ratio2']
		film_ratio3 = request.form['film_ratio3']
		film_ratio4 = request.form['film_ratio4']
		film_ratio5 = request.form['film_ratio5']
		film_ratio6 = request.form['film_ratio6']
		film_ratio7 = request.form['film_ratio7']
		box_office1 = request.form['box_office1']
		box_office2 = request.form['box_office2']
		box_office3 = request.form['box_office3']
		box_office4 = request.form['box_office4']
		box_office5 = request.form['box_office5']
		box_office6 = request.form['box_office6']
		box_office7 = request.form['box_office7']
		auction_calendar = request.form['auction_calendar']
		language = request.form['language']
		if film_ratio1:
			args = [float(attendence1),float(film_ratio1),float(attendence2),float(film_ratio2),float(attendence3),float(film_ratio3),float(attendence4),float(film_ratio4),float(attendence5),float(film_ratio5),float(attendence6),float(film_ratio6),float(attendence7),float(film_ratio7)]
			if auction_calendar == '贺岁档':
				args.extend([1.000000,0.000000,0.000000,0.000000,0.000000])
			elif auction_calendar == '五一档':
				args.extend([0.000000,1.000000,0.000000,0.000000,0.000000])
			elif auction_calendar == '暑假档':
				args.extend([0.000000,0.000000,1.000000,0.000000,0.000000])
			elif auction_calendar == '国庆档':
				args.extend([0.000000,0.000000,0.000000,1.000000,0.000000])
			elif auction_calendar == '其他档':
				args.extend([0.000000,0.000000,0.000000,0.000000,1.000000])

			if language == '中文':
				args.extend([1.000000,0.000000,0.000000])
			elif language == '英文':
				args.extend([0.000000,1.000000,0.000000])
			elif language == '其他':
				args.extend([0.000000,0.000000,1.000000])

			# result = MoviePredictionExample.movie_prediction(args)
			result = 28.59
			
		if box_office1:
			args = [float(attendence1),float(attendence2),float(attendence3),float(attendence4),float(attendence5),float(attendence6),float(attendence7),float(box_office1),float(box_office2),float(box_office3),float(box_office4),float(box_office5),float(box_office6),float(box_office7)]
			if auction_calendar == '贺岁档':
				args.append(10000)
			elif auction_calendar == '五一档':
				args.append(1000)
			elif auction_calendar == '暑假档':
				args.append(101)
			elif auction_calendar == '国庆档':
				args.append(10)
			elif auction_calendar == '其他档':
				args.append(1)

			if language == '中文':
				args.append(100)
			elif language == '英文':
				args.append(10)
			elif language == '其他':
				args.append(1)

			print('====')
			print(args)
			print('====')
			result = sim.run_sim(args)

		# print(result)
		resp = resp_add_header(result)
		return resp

# 添加一个头,否则无法访问
def resp_add_header(result):
	resp = make_response(result)
	resp.headers['Access-Control-Allow-Origin'] = '*'
	return resp

def inquire_all(table_name):
	conn = pymysql.connect("localhost","root",password,"bigdata")
	cursor = conn.cursor()
	sql = "SELECT * FROM %s"%(table_name)
	cursor.execute(sql)
	data = cursor.fetchall()
	data_json = json.dumps(data)
	cursor.close()
	conn.close()
	return data_json

# 分析页面一查询数据入口
def inquire_data(table_name,chart_type,year_list):
	conn = pymysql.connect("localhost","root",password,"bigdata")
	cursor = conn.cursor()
	if year_list[0]:
		year_list = ['quantity_'+i.replace('年','') for i in year_list]
		if chart_type == 'pie' or chart_type == 'map':
			col_name = table_name.replace('film_','') + ',' + '+'.join(year_list)
			sql = "SELECT %s FROM %s"%(col_name,table_name)
			cursor.execute(sql)
			data = cursor.fetchall()
			data = [{'name':d[0],'value':d[1]} for d in data]
		elif chart_type == 'line':
			col_name = 'sum(january),sum(february),sum(march),sum(april),sum(may),sum(june),sum(july),sum(august),sum(september),sum(october),sum(november),sum(december)'
			year_list = [i.replace('quantity_','') for i in year_list]
			condition = ' WHERE year='+' OR year='.join(year_list)
			sql = "SELECT %s FROM %s%s"%(col_name,table_name,condition)
			cursor.execute(sql)
			data = cursor.fetchall()
	else:
		sql = ''
		data = [[]]
	data_json = json.dumps(data)
	cursor.close()
	conn.close()
	return data_json

# 简单获取评分表数据
def grade_data(table_name):
	conn = pymysql.connect("localhost","root",password,"bigdata")
	cursor = conn.cursor()
	sql = "SELECT 评分,COUNT(*) FROM %s GROUP BY 评分"%(table_name)
	cursor.execute(sql)
	data = cursor.fetchall()
	data_json = json.dumps(data)
	# print(data)
	# print(j)
	cursor.close()
	conn.close()
	return data_json

# 通过like查询评分表
def grade_data_select(type_list,country_list,language_list):
	conn = pymysql.connect("localhost","root",password,"bigdata")
	cursor = conn.cursor()
	if type_list[0]:
			for key1,value1 in enumerate(type_list):
				type_list[key1] = r"类型 LIKE '%"+value1+"%'"
	if country_list[0]:
		for key2,value2 in enumerate(country_list):
			country_list[key2] = r"制片国家 LIKE '%"+value2+"%'"
	if language_list[0]:
		for key3,value3 in enumerate(language_list):
			language_list[key3] = r"语言 LIKE '%"+value3+"%'"
	cond_list = type_list+country_list+language_list
	cond_list = [i for i in cond_list if i != '']
	cond_str = ' OR '.join(cond_list)
	if cond_list:
		cond_str = ' WHERE ' + cond_str
		sql = "SELECT 上映日期,片长,评分 FROM douban_jinjian%s"%cond_str
		cursor.execute(sql)
		data = cursor.fetchall()
		release_list = []
		time_list = []
		for d in data:
			if d[2] != 'null':
				if d[0] != 'null':
					temp = []
					temp.append(d[0])
					temp.append(d[2])
					release_list.append(temp)
				if d[1] != 'null':
					temp = []
					temp.append(d[1])
					temp.append(d[2])
					time_list.append(temp)
		result_list = [release_list,time_list]
		data_json = json.dumps(result_list)
	else:
		data_json = json.dumps([[[]],[[]]])
	cursor.close()
	conn.close()
	return data_json

if __name__ == '__main__':
	app.run(host="127.0.0.1",port=10086)
