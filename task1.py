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

def stringType(list_str):
    Integer = []
    Float = []
    Date = []
    DateOrigin = []
    Text = []
    TextLen = []
    for string in list_str:     
        try:
            Int = int(string)
            Integer.append(Int)
        except:
            try:
                Flt = float(string)
                Float.append(Flt)
            except:
                try:
                    Dt = parser.parse(string, ignoretz=True)
                    Date.append(Dt)
                    DateOrigin.append(string)
                except:
                    try:
                        Dt = datetime.strptime(string, "%Y-%y")
                        Date.append(Dt)
                        DateOrigin.append(string)
                    except:
                        Text.append(string)
                        TextLen.append(len(string))
    return Integer, Float, Date, DateOrigin, Text, TextLen


def handleIntFloat(vals):
	count = len(vals)
	maxVal = np.max(vals)
	minVal = np.min(vals)
	mean = np.mean(vals)
	std = np.std(vals)
	return count, maxVal, minVal, mean, std


def handleDate(dates, dates_origin):
	count = len(dates)
	maxInd = np.argmax(dates)
	maxVal = dates_origin[maxInd]
	minInd = np.argmin(dates)
	minVal = dates_origin[minInd]
	return count, maxVal, minVal


def handleText(texts, texts_len):
	count = len(texts)
	longestInd = np.argmax(texts_len)
	longestVal = texts[longestInd]
	shortestInd = np.argmin(texts_len)
	shortestVal = texts[shortestInd]
	avgLen = np.mean(texts_len)
	return count, shortestVal, longestVal, avgLen

# add for loop for files
# list of files order by size

# fileName = '2232-dj5q.tsv.gz'

datasetSize = np.load("datasetSize.npy")
datasetName = list(datasetSize[:, 0])

start = int(sys.argv[1])
end = int(sys.argv[2])
cnt = start
for fileName in datasetName[start:end]:
	cnt += 1
	print("="*40)
	print("Processing file: %s (#%d of 1900)" % (fileName, cnt))
	folder='/user/hm74/NYCOpenData/'
	tsv_rdd=spark.read.format("csv") \
		.option("header","true") \
		.option("delimiter",'\t') \
		.load(folder+fileName+'.tsv.gz')

	jsonDict = {}
	jsonDict['dataset_name'] = fileName
	jsonDict['columns'] = []

	tsv_columns = tsv_rdd.columns
	columns_rename = []
	for clmn in tsv_columns:
		new_name = clmn.replace('.','')
		columns_rename.append(new_name)

	tsv_columns = columns_rename
	tsv_df = tsv_rdd.toDF(*tsv_columns)

	for colname in tsv_columns:
		print("-"*40)
		print("Processing column:", colname.encode("utf-8"))
		column = tsv_df.select(colname)
		totCount = column.count()
		empty = column.select(colname).where(col(colname).isNull())
		emptyCount = empty.count()
		nonEmpty = column.select(colname).where(col(colname).isNotNull())
		nonEmptyCount = nonEmpty.count()
		distinctCount = nonEmpty.distinct().count()
		top5 = nonEmpty.groupBy(colname).count().sort(col("count").desc()).rdd.map(lambda x:(x[0], x[1])).collect()[:5]
		jsonCol = {}
		jsonCol['column_name'] = colname
		jsonCol['number_non_empty_cells'] = nonEmptyCount
		jsonCol['number_empty_cells'] = emptyCount
		jsonCol['number_distinct_values'] = distinctCount
		jsonCol['frequent_values'] = top5
		jsonCol['data_types'] = []
		nonEmp_list = nonEmpty.rdd.map(lambda x:x[0]).collect()
		Int, Flt, Date, DateOrigin, Text, TextLen = stringType(nonEmp_list)
		if len(Int) > 0:
			countInt, maxValInt, minValInt, meanInt, stdInt = handleIntFloat(Int)
			jsonInt = {}
			jsonInt['type'] = "INTEGER (LONG)"
			jsonInt['count'] = countInt
			jsonInt['max_value'] = int(maxValInt)
			jsonInt['min_value'] = int(minValInt)
			jsonInt['mean'] = meanInt
			jsonInt['stddev'] = stdInt
			jsonCol['data_types'].append(jsonInt)
		if len(Flt) > 0:
			countFlt, maxValFlt, minValFlt, meanFlt, stdFlt = handleIntFloat(Flt)
			jsonFlt = {}
			jsonFlt['type'] = "REAL"
			jsonFlt['count'] = countFlt
			jsonFlt['max_value'] = maxValFlt
			jsonFlt['min_value'] = minValFlt
			jsonFlt['mean'] = meanFlt
			jsonFlt['stddev'] = stdFlt
			jsonCol['data_types'].append(jsonFlt)
		if len(Date) > 0:
			countDate, maxValDate, minValDate = handleDate(Date, DateOrigin)
			jsonDate = {}
			jsonDate['type'] = "DATE/TIME"
			jsonDate['count'] = countDate
			jsonDate['max_value'] = maxValDate
			jsonDate['min_value'] = minValDate
			jsonCol['data_types'].append(jsonDate)
		if len(Text) > 0:
			countText, shortestText, longestText, avgLenText = handleText(Text, TextLen)
			jsonText = {}
			jsonText['type'] = "TEXT"
			jsonText['count'] = countText
			jsonText['shortest_values'] = shortestText
			jsonText['longest_values'] = longestText
			jsonText['average_length'] = avgLenText
			jsonCol['data_types'].append(jsonText)
		jsonDict['columns'].append(jsonCol)
	# 	print(json.dumps(jsonCol))
	print("="*40+"\n")

	with open('json/'+fileName+'.json', 'w') as outfile:
	    json.dump(jsonDict, outfile)
