import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from csv import reader
from pyspark import SparkContext
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql.functions import isnan
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, ArrayType
from datetime import datetime
from dateutil import parser
import numpy as np
import json

sc = SparkContext()

spark = SparkSession \
		.builder \
		.appName("final") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()

def mapper(line, head, mymap):
	res = []
	for val, hd in zip(line, head):
		if val == "" or val == " " or val == "NaN" or val == "Unspecified":
			continue
		try:
			Int = int(val)
			res.append((hd+'\t'+'int', Int))
		except:
			try:
				Flt = float(val)
				if val.upper() == 'NAN' or Flt == float("inf"):
					res.append((hd+'\t'+'text', (val, len(val))))
				else:
					res.append((hd+'\t'+'float', Flt))
			except:
				try:
					Dt = parser.parse(val, ignoretz=True)
					res.append((hd+'\t'+'datetime', (val, Dt)))
				except:
					try:
						Dt = datetime.strptime(val, "%Y-%y")
						res.append((hd+'\t'+'datetime', (val, Dt)))
					except:
						res.append((hd+'\t'+'text', (val, len(val))))
	return tuple(res)


def list_summary(key, vals):
	key = key.split('\t')
	col_name = key[0]
	col_type = key[1]
	count, distinct_vals_count, top5, top5_count, \
		max_val, min_val, mean, stddev = \
		0, 0, 0, 0, 0, 0, 0, 0
	if col_type == 'int' or col_type == 'float':
		count = len(vals)
		distinct_vals, vals_freq = np.unique(vals, return_counts=True)
		distinct_vals_count = len(distinct_vals)
		top5_index = vals_freq.argsort()[-5:][::-1]
		top5 = distinct_vals[top5_index]
		if col_type == 'int':
			top5 = [int(i) for i in top5]
		top5_count = vals_freq[top5_index]
		max_val = np.max(vals)
		min_val = np.min(vals)
		mean = np.mean(vals)
		stddev = np.std(vals)
	if col_type == 'datetime':
		dt_columns = list(zip(*vals))
		dt_origin = dt_columns[0]
		dt_datetime = dt_columns[1]
		count = len(dt_origin)
		distinct_vals, vals_freq = np.unique(dt_origin, return_counts = True)
		distinct_vals_count = len(distinct_vals)
		top5_index = vals_freq.argsort()[-5:][::-1]
		top5 = distinct_vals[top5_index]
		top5_count = vals_freq[top5_index]
		max_ind = np.argmax(dt_datetime)
		max_val = dt_origin[max_ind]
		min_ind = np.argmin(dt_datetime)
		min_val = dt_origin[min_ind]
	if col_type == 'text':
		text_columns = list(zip(*vals))
		text_origin = text_columns[0]
		text_len = text_columns[1]
		count = len(text_origin)
		distinct_vals, vals_freq = np.unique(text_origin, return_counts = True)
		distinct_vals_count = len(distinct_vals)
		top5_index = vals_freq.argsort()[-5:][::-1]
		top5 = distinct_vals[top5_index]
		top5_count = vals_freq[top5_index]
		longest_ind = np.argmax(text_len)
		max_val = text_origin[longest_ind]
		shrotest_ind = np.argmin(text_len)
		min_val = text_origin[shrotest_ind]
		mean = np.mean(text_len)
	return (col_name, (count, distinct_vals_count, top5, top5_count, \
	 col_type, max_val, min_val, mean, stddev))


def generate_json(key, vals):
	jsonCol = {}
	jsonCol['column_name'] = key
	jsonCol['number_non_empty_cells'] = 0
	jsonCol['number_empty_cells'] = 0
	jsonCol['number_distinct_values'] = 0
	jsonCol['frequent_values'] = 0
	jsonCol['data_types'] = []
	freq_val_list = []
	freq_val_count = []
	for val in vals:
		count, distinct_vals_count, top5, top5_count, \
			col_type, max_val, min_val, mean, stddev = val
		jsonCol['number_non_empty_cells'] += count
		jsonCol['number_distinct_values'] += distinct_vals_count
		freq_val_list += list(top5)
		freq_val_count += list(top5_count)
		if col_type == 'int':
			jsonInt = {}
			jsonInt['type'] = "INTEGER (LONG)"
			jsonInt['count'] = int(count)
			jsonInt['max_value'] = int(max_val)
			jsonInt['min_value'] = int(min_val)
			jsonInt['mean'] = mean
			jsonInt['stddev'] = stddev
			jsonCol['data_types'].append(jsonInt)
		elif col_type == 'float':
			jsonFlt = {}
			jsonFlt['type'] = "REAL"
			jsonFlt['count'] = int(count)
			jsonFlt['max_value'] = max_val
			jsonFlt['min_value'] = min_val
			jsonFlt['mean'] = mean
			jsonFlt['stddev'] = stddev
			jsonCol['data_types'].append(jsonFlt)
		elif col_type == 'datetime':
			jsonDate = {}
			jsonDate['type'] = "DATE/TIME"
			jsonDate['count'] = int(count)
			jsonDate['max_value'] = max_val
			jsonDate['min_value'] = min_val
			jsonCol['data_types'].append(jsonDate)
		else:
			jsonText = {}
			jsonText['type'] = "TEXT"
			jsonText['count'] = int(count)
			jsonText['shortest_values'] = min_val
			jsonText['longest_values'] = max_val
			jsonText['average_length'] = mean
			jsonCol['data_types'].append(jsonText)
	jsonCol['number_empty_cells'] = num_row - jsonCol['number_non_empty_cells']
	freq_val_count = np.array(freq_val_count)
	first5_index = list(freq_val_count.argsort()[-5:][::-1])
	first5 = [freq_val_list[i] for i in first5_index]
	first5_count = freq_val_count[first5_index]
	jsonCol['frequent_values'] = [(i,int(j)) for i,j in zip(first5, first5_count)]
	jsonCol['number_non_empty_cells'] = int(jsonCol['number_non_empty_cells'])
	jsonCol['number_empty_cells'] = int(jsonCol['number_empty_cells'])
	jsonCol['number_distinct_values'] = int(jsonCol['number_distinct_values'])
	return (1, jsonCol)

# 2mqz-v5im
# '2bmr-jdsv'
datasetSize = np.load("datasetSize.npy")
datasetName = list(datasetSize[:, 0])

start = int(sys.argv[1])
end = int(sys.argv[2])
cnt = start
for fileName in datasetName[start:end]:
	cnt += 1
	print("="*40)
	print("Processing file: %s (#%d of 1900)" % (fileName, cnt))

	folder = '/user/hm74/NYCOpenData/'
	# fileName = '2mqz-v5im'
	lines = sc.textFile(folder+fileName+'.tsv.gz').map(lambda x: x.split('\t'))
	header = lines.first()
	data = lines.filter(lambda row: row != header)
	num_row = data.count()

	print("-"*40)
	print("Finish loading data")

	map_num_val = []
	for hd in header:
		for tp in ['int', 'float', 'datetime', 'text']:
			map_num_val.append(hd+'\t'+tp)


	after_fmap = data.flatMap(lambda s: mapper(s, header, map_num_val))
	after_fmap_groupByKey = after_fmap.groupByKey()
	key_list = after_fmap_groupByKey.map(lambda x: (x[0], list(x[1])))

	print("-"*40)
	print("Map to column name + type")

	col_summary = key_list.map(lambda x: list_summary(x[0], x[1]))
	col_summary_group = col_summary.groupByKey()
	col_group_list = col_summary_group.map(lambda x: (x[0], list(x[1])))

	print("-"*40)
	print("Group types by column name")

	col_json = col_group_list.map(lambda x: generate_json(x[0], x[1]))
	col_json_group = col_json.groupByKey().map(lambda x: (x[0], list(x[1])))

	print("-"*40)
	print("Generating json ...")

	jsons = col_json_group.collect()[0][1]
	jsonDict = {}
	jsonDict['dataset_name'] = fileName
	jsonDict['columns'] = [0]*len(header)
	head_list = [i for i in header]

	for js in jsons:
		ind = head_list.index(js['column_name'])
		jsonDict['columns'][ind] = js

	print("-"*40)
	print("Writing json to file ...")

	# print(json.dumps(jsonDict))
	with open('mapReduceJson/'+fileName+'.json', 'w') as outfile:
		json.dump(jsonDict, outfile)

	print("="*40)
