import numpy as np
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import pandas_udf, PandasUDFType,regexp_replace,lit

data_file = r'/root/Desktop/Job/bigData/data.csv'
data_original = spark.read.csv(data_file,header=True,inferSchema=True)
data = data_original.select("日期","上座率","综合票房（万元）","电影片名","上映天数信息","排座占比","场次","排片占比","累计综合票房","人次")
data = data.filter(data['场次']>50)
data = data.withColumn('temp',regexp_replace('上映天数信息','零点场','0'))
data = data.filter(data.temp.like('上映%') | data.temp.like('0'))
data = data.replace('上映首日','1','temp')
data = data.withColumn('released_day',regexp_replace('temp',r'(\D*)','')).drop('temp')
data = data.sort(['电影片名','日期'])
data = data.withColumn('signal',lit(0))

# 天数小于7的剔除（data_step1)
@pandas_udf(data.schema, functionType=PandasUDFType.GROUPED_MAP)
def step1(pdf):
	if pdf.shape[0] < 7:
		pdf.signal = 1
	return pdf

data_step1 = data.groupby('电影片名').apply(step1)
data_step1 = data_step1.filter(data_step1.signal==0)


# 剔除上座率含有'--'的数据（data_step2)
data_step2 = data_step1
@pandas_udf(data_step2.schema, functionType=PandasUDFType.GROUPED_MAP)
def step2(pdf):
	result_count = pdf.loc[:,'上座率'].value_counts()
	if '--' in result_count.index:
		if result_count['--'] > 0:
			pdf = pd.DataFrame(columns=pdf.columns)
	return pdf
data_step2 = data_step2.groupby('电影片名').apply(step2)

# 嵘哥定制版
# 选出冰点值（data_step3)
data_step3 = data_step2.withColumn('排片占比',regexp_replace('排片占比',r'<0.1%','0.05')).withColumn('排片占比',regexp_replace('排片占比',r'%',''))
@pandas_udf(data_step3.schema, functionType=PandasUDFType.GROUPED_MAP)
def step3(pdf):
	first_day = '91020233'
	ratio_value = '100'
	for index,row in pdf.iterrows():
		if row['排片占比'] < ratio_value and int(row['released_day']) > 7:
			first_day = row['日期']
			ratio_value = row['排片占比']
		elif row['排片占比'] == ratio_value and row['日期'] < first_day and int(row['released_day']) > 7:
			first_day = row['日期']
			ratio_value = row['排片占比']
	if first_day != '91020233':
		pdf.loc[pdf['日期']==first_day,'signal'] = 1
	return  pdf

data_step3 = data_step3.groupby('电影片名').apply(step3)
data_step3 = data_step3.filter((data_step3['signal']==1) | (data_step3['released_day']<8))
data_step3 = data_step3.withColumn('signal',lit(0))

schema = StructType([
	StructField('电影片名',StringType(),True),
	StructField('日期',IntegerType(),True),
	StructField('上座率1',DoubleType(),True),
	StructField('排片占比1',DoubleType(),True),
	StructField('上座率2',DoubleType(),True),
	StructField('排片占比2',DoubleType(),True),
	StructField('上座率3',DoubleType(),True),
	StructField('排片占比3',DoubleType(),True),
	StructField('上座率4',DoubleType(),True),
	StructField('排片占比4',DoubleType(),True),
	StructField('上座率5',DoubleType(),True),
	StructField('排片占比5',DoubleType(),True),
	StructField('上座率6',DoubleType(),True),
	StructField('排片占比6',DoubleType(),True),
	StructField('上座率7',DoubleType(),True),
	StructField('排片占比7',DoubleType(),True),
	StructField('结果天数',IntegerType(),True),
	StructField('类型',StringType(),True),
	StructField('档期',StringType(),True),
	StructField('制片地区',StringType(),True),
	])

data_yr.groupBy('电影片名').apply(yerong).show()

@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def yerong(pdf):
	pdf[['排片占比']] = pdf[['排片占比']].astype(object)
	pdf[['released_day']] = pdf[['released_day']].astype(int)
	columns = ['电影片名','日期','上座率1','排片占比1','上座率2','排片占比2','上座率3','排片占比3','上座率4','排片占比4','上座率5','排片占比5','上座率6','排片占比6','上座率7','排片占比7','结果天数','类型','档期','制片地区']
	df = pd.DataFrame(columns=columns)
	df.loc[0,'电影片名'] = pdf.iloc[0,2]
	df.loc[0,'日期'] = pdf.iloc[0,0]
	for i in range(1,8):
		if not pdf.loc[pdf.released_day==i,'上座率'].empty:
			string = '上座率%d' % (i)
			value_a = pdf.loc[pdf.released_day==i,'上座率'].values[0]
			value_a = round(float(value_a.replace('%',''))/100,4)
			df.loc[0,string] = value_a
		if not pdf.loc[pdf.released_day==i,'排片占比'].empty:
			string = '排片占比%d' % (i)
			value_b = pdf.loc[pdf.released_day==i,'排片占比'].values[0]
			value_b = round(value_b/100,4)
			df.loc[0,string] = value_b
	if not pdf.loc[pdf.released_day>7,'released_day'].empty:
		df.loc[0,'结果天数'] = pdf.loc[pdf.released_day>7,'released_day'].values[0]
	# 类型计算(film_type长度22)
	film_type_list = ['0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0']
	film_type_name = ['剧情','喜剧','科幻','奇幻','动画','动作','冒险 ','家庭','传记','运动',['音乐','歌舞'],'战争','悬疑',['惊悚','恐怖'],'犯罪','爱情','古装','武侠','儿童','历史','灾难','西部']
	for index,value in enumerate(film_type_list):
		if isinstance(film_type_name[index],list):
			if film_type_name[index][0] in pdf.iloc[0,5] or film_type_name[index][1] in pdf.iloc[0,5]:
				film_type_list[index] = '1'
		else:
			if film_type_name[index] in pdf.iloc[0,5]:# pdf.iloc[0,5]是类型的值
				film_type_list[index] = '1'
	film_type = ''.join(film_type_list)
	df.loc[0,'类型'] = film_type
	# 档期计算(auction_calendar长度5)
	auction_calendar_list = ['0','0','0','0','0']
	pdf[['日期']] = pdf[['日期']].astype(object)
	date = pdf['日期'].map(lambda x:str(x)[4:])
	for index, value in date.iteritems():
		if value>='0101' and value<='0228':
			# 贺岁档（1.1——2.28）
			auction_calendar_list[0] = '1'
		elif value>='0501' and value<='0503':
			# 五一档（5.1——5.3）
			auction_calendar_list[1] = '1'
		elif value>='0701' and value<='0830':
			# 暑假档（7.1——8.30)
			auction_calendar_list[2] = '1'
		elif value>='1001' and value<='1007':
			# 国庆档（10.1-10.7）
			auction_calendar_list[3] = '1'
		else:
			# 其他档
			auction_calendar_list[4] = '1'
	auction_calendar = ''.join(auction_calendar_list)
	df.loc[0,'档期'] = auction_calendar
	# 制片国家计算
	region_list = ['0','0','0']
	temp_str = pdf.iloc[0,6].replace('中国大陆','').replace('美国','').replace('/','')
	if '中国' in pdf.iloc[0,6]:
		region_list[0] = '1'
	if '美国' in pdf.iloc[0,6]:
		region_list[1] = '1'
	if len(temp_str) > 1:
		region_list[2] = '1'
	region = ''.join(region_list)
	df.loc[0,'制片地区'] = region
	return df

output_yr = data_yr.groupBy('电影片名').apply(yerong)
output_yr.show()
output_yr.filter(output_yr["上座率7"].isNotNull()).filter(output_yr["结果天数"].isNotNull()).toPandas().to_csv("/root/Desktop/Job/bigData/output_yr.csv",encoding="utf_8_sig")

# 引入豆瓣数据集
douban_file = r'/root/Desktop/Job/bigData/douban.csv'
douban = spark.read.csv(douban_file,header=True,inferSchema=True)
douban = douban.select("电影名称","类型","导演","编剧","主演","制片国家/地区","语言","评分","上映日期")

# output的数据也要处理一下
output_xx = output_xx.withColumn('date',output_xx["日期"][0:4])
douban = douban.withColumn('_date',douban["上映日期"][0:4])

# 豆瓣集名称清洗
split_space =  pandas_udf(lambda s: s.map(lambda x:x.split(' ')[0]),StringType())
douban_new = douban.withColumn("电影名称",split_space("电影名称"))

cond = [douban_new["电影名称"]==output_xx['电影片名'],(output_xx["date"]>=douban_new["_date"]-1) & (output_xx["date"]<=douban_new["_date"]+1)]
result_a = output_xx.join(douban_new, cond, "left").drop("电影名称","date","_date")
result_a.filter(result_a["类型"].isNotNull()).toPandas().to_csv("/root/Desktop/Job/bigData/191118.csv",encoding="utf_8_sig")


# 选择电影的最后一天(大主教定制)
@pandas_udf(temp1.schema, functionType=PandasUDFType.GROUPED_MAP)
def select_last_day(pdf):
	last_day = pdf['日期'].max()
	if last_day:
		pdf.loc[pdf['日期']==last_day,'signal'] = 1
	return pdf
output = temp1.groupby('电影片名').apply(select_last_day)
output = output.filter(output.signal==1).drop('signal','released_day')

# hx数据集定制版
@pandas_udf(data_step1.schema, functionType=PandasUDFType.GROUPED_MAP)
def select_last_day(pdf):
	last_day = pdf['日期'].max()
	if last_day:
		pdf.loc[pdf['日期']==last_day,'signal'] = 1
	return pdf
data_hx = data_step1.groupby('电影片名').apply(select_last_day)
data_hx = data_hx.filter((data_hx['signal']==1) | (data_hx['released_day']<8)).drop('signal')

schema = StructType([
	StructField('电影片名',StringType(),True),
	StructField('日期',IntegerType(),True),
	StructField('上座率1',DoubleType(),True),
	StructField('排片占比1',DoubleType(),True),
	StructField('综合票房（万元）1',DoubleType(),True),
	StructField('上座率2',DoubleType(),True),
	StructField('排片占比2',DoubleType(),True),
	StructField('综合票房（万元）2',DoubleType(),True),
	StructField('上座率3',DoubleType(),True),
	StructField('排片占比3',DoubleType(),True),
	StructField('综合票房（万元）3',DoubleType(),True),
	StructField('上座率4',DoubleType(),True),
	StructField('排片占比4',DoubleType(),True),
	StructField('综合票房（万元）4',DoubleType(),True),
	StructField('上座率5',DoubleType(),True),
	StructField('排片占比5',DoubleType(),True),
	StructField('综合票房（万元）5',DoubleType(),True),
	StructField('上座率6',DoubleType(),True),
	StructField('排片占比6',DoubleType(),True),
	StructField('综合票房（万元）6',DoubleType(),True),
	StructField('上座率7',DoubleType(),True),
	StructField('排片占比7',DoubleType(),True),
	StructField('综合票房（万元）7',DoubleType(),True),
	StructField('综合票房（万元）',DoubleType(),True),
	StructField('类型',StringType(),True),
	StructField('档期',StringType(),True),
	StructField('制片地区',StringType(),True),
	])

data_hx.groupBy('电影片名').apply(hx).show()

@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def hx(pdf):
	pdf[['排片占比']] = pdf[['排片占比']].astype(object)
	pdf[['released_day']] = pdf[['released_day']].astype(int)
	columns = ['电影片名','日期','上座率1','排片占比1','综合票房（万元）1','上座率2','排片占比2','综合票房（万元）2','上座率3','排片占比3','综合票房（万元）3','上座率4','排片占比4','综合票房（万元）4','上座率5','排片占比5','综合票房（万元）5','上座率6','排片占比6','综合票房（万元）6','上座率7','排片占比7','综合票房（万元）7','累计综合票房','类型','档期','制片地区']
	df = pd.DataFrame(columns=columns)
	df.loc[0,'电影片名'] = pdf.iloc[0,3]
	df.loc[0,'日期'] = pdf.iloc[0,0]
	for i in range(1,8):
		if not pdf.loc[pdf.released_day==i,'上座率'].empty:
			string = '上座率%d' % (i)
			value_a = pdf.loc[pdf.released_day==i,'上座率'].values[0]
			value_a = round(float(value_a.replace('%',''))/100,4)
			df.loc[0,string] = value_a
		if not pdf.loc[pdf.released_day==i,'排片占比'].empty:
			string = '排片占比%d' % (i)
			value_b = pdf.loc[pdf.released_day==i,'排片占比'].values[0]
			value_b = round(float(value_b.replace('%','').replace('<0.1','0.05'))/100,4)
			df.loc[0,string] = value_b
		if not pdf.loc[pdf.released_day==i,'综合票房（万元）'].empty:
			string = '综合票房（万元）%d' % (i)
			value_c = pdf.loc[pdf.released_day==i,'综合票房（万元）'].values[0]
			df.loc[0,string] = value_c
	if not pdf.loc[pdf.released_day>7,'累计综合票房'].empty:
		df.loc[0,'累计综合票房'] = pdf.loc[pdf.released_day>7,'累计综合票房'].values[0]
	# 类型计算(film_type长度22)
	film_type_list = ['0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0']
	film_type_name = ['剧情','喜剧','科幻','奇幻','动画','动作','冒险 ','家庭','传记','运动',['音乐','歌舞'],'战争','悬疑',['惊悚','恐怖'],'犯罪','爱情','古装','武侠','儿童','历史','灾难','西部']
	for index,value in enumerate(film_type_list):
		if isinstance(film_type_name[index],list):
			if film_type_name[index][0] in pdf.iloc[0,6] or film_type_name[index][1] in pdf.iloc[0,6]:
				film_type_list[index] = '1'
		else:
			if film_type_name[index] in pdf.iloc[0,6]:# pdf.iloc[0,6]是类型的值
				film_type_list[index] = '1'
	film_type = ''.join(film_type_list)
	df.loc[0,'类型'] = film_type
	# 档期计算(auction_calendar长度5)
	auction_calendar_list = ['0','0','0','0','0']
	pdf[['日期']] = pdf[['日期']].astype(object)
	date = pdf['日期'].map(lambda x:str(x)[4:])
	for index, value in date.iteritems():
		if value>='0101' and value<='0228':
			# 贺岁档（1.1——2.28）
			auction_calendar_list[0] = '1'
		elif value>='0501' and value<='0503':
			# 五一档（5.1——5.3）
			auction_calendar_list[1] = '1'
		elif value>='0701' and value<='0830':
			# 暑假档（7.1——8.30)
			auction_calendar_list[2] = '1'
		elif value>='1001' and value<='1007':
			# 国庆档（10.1-10.7）
			auction_calendar_list[3] = '1'
		else:
			# 其他档
			auction_calendar_list[4] = '1'
	auction_calendar = ''.join(auction_calendar_list)
	df.loc[0,'档期'] = auction_calendar
	# 制片国家计算
	region_list = ['0','0','0']
	temp_str = pdf.iloc[0,7].replace('中国大陆','').replace('美国','').replace('/','')
	if '中国' in pdf.iloc[0,7]:
		region_list[0] = '1'
	if '美国' in pdf.iloc[0,7]:
		region_list[1] = '1'
	if len(temp_str) > 1:
		region_list[2] = '1'
	region = ''.join(region_list)
	df.loc[0,'制片地区'] = region
	if pdf.shape[0] < 8:
		df = pd.DataFrame(columns=columns)
	return df

output_hx = data_hx.groupBy('电影片名').apply(hx)
output_hx.show()
output_hx.filter(output_hx["累计综合票房"].isNotNull()).toPandas().to_csv("/root/Desktop/Job/bigData/output_hx.csv",encoding="utf_8_sig")