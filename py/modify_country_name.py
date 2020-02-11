import pandas as pd
import json

data = pd.read_csv('film_country.csv')
with open('country_name.json','r',encoding='utf-8') as f:
	load_dict = json.load(f)

def name_change(c_name):
	if c_name in dict_new.keys():
		name = dict_new[c_name]
	else:
		name = c_name
	return name

dict_new = {value:key for key, value in load_dict.items()}
data.loc[:,'country'] = data.loc[:,'country'].map(lambda x:name_change(x))


data.to_csv("new.csv",encoding="utf_8_sig")