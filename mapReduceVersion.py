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

folder = '/user/hm74/NYCOpenData/'
fileName = '2bmr-jdsv'
lines = sc.textFile(folder+fileName+'.tsv.gz').map(lambda x: x.split('\t'))
# fmlines = sc.textFile(folder+fileName+'.tsv.gz').flatMap(lambda x: x.split('\t'))
header = lines.first()
data = lines.filter(lambda row: row != header)

# jsonDict = {}
# jsonDict['dataset_name'] = fileName
# jsonDict['columns'] = []

# tsv_columns = tsv_rdd.columns
# columns_rename = []

# data = sc.textFile('path_to_data')
# header = data.first() #extract header
# data = data.filter(row => row != header)

# for clmn in tsv_columns:
# 	new_name = clmn.replace('.','')
# 	columns_rename.append(new_name)

# tsv_columns = columns_rename
# tsv_df = tsv_rdd.toDF(*tsv_columns)
map_num_val = []
for hd in header:
	for tp in ['int', 'float', 'datetime', 'text']:
		map_num_val.append(hd+'\t'+tp)

def mapper(line, head, mymap):
	res = []
	for val, hd in zip(line, head):
		# res.append(([hd, 'int'],val))
		try:
			Int = int(val)
			res.append((mymap.index(hd+'\t'+'int'), Int))
		except:
			try:
				Flt = float(val)
				if val.upper() == 'NAN' or Flt == float("inf"):
					res.append((mymap.index(hd+'\t'+'text'), val))
				else:
					res.append((mymap.index(hd+'\t'+'float'), val))
			except:
				try:
					Dt = parser.parse(val, ignoretz=True)
					res.append((mymap.index(hd+'\t'+'datetime'), (val, Dt)))
				except:
					try:
						Dt = datetime.strptime(val, "%Y-%y")
						res.append((mymap.index(hd+'\t'+'datetime'), (val, Dt)))
					except:
						res.append((mymap.index(hd+'\t'+'text'), val))
	return tuple(res)

after_map = data.map(lambda s: mapper(s, header, map_num_val))
after_fmap = data.flatMap(lambda s: mapper(s, header, map_num_val))
after_fmap_groupByKey = after_fmap.groupByKey()
key_list = after_fmap_groupByKey.map(lambda x: (x[0], list(x[1])))

# use map to deal with key_list!!!

jsonDict = {}
jsonDict['dataset_name'] = fileName
jsonDict['columns'] = []


# after_fmap_red = after_fmap.reduce(lambda a, b: a + b)


def stringType(vals):
    Integer = []
    IntegerCount = []
    Float = []
    FloatCount = []
    Date = []
    DateOrigin = []
    DateCount = []
    Text = []
    TextLen = []
    TextCount = []
    for val in vals:
        string = val[0]
        cnt = val[1]
        try:
            Int = int(string)
            Integer.append(Int)
            IntegerCount.append(cnt)
        except:
            try:
                Flt = float(string)
                if string.upper() == 'NAN' or Flt == float("inf"):
                    Text.append(string)
                    TextLen.append(len(string))
                    TextCount.append(cnt)
                else:
                    Float.append(Flt)
                    FloatCount.append(cnt)
            except:
                try:
                    Dt = parser.parse(string, ignoretz=True)
                    Date.append(Dt)
                    DateCount.append(cnt)
                    DateOrigin.append(string)
                except:
                    try:
                        Dt = datetime.strptime(string, "%Y-%y")
                        Date.append(Dt)
                        DateCount.append(cnt)
                        DateOrigin.append(string)
                    except:
                        Text.append(string)
                        TextLen.append(len(string))
                        TextCount.append(cnt)
    return Integer, IntegerCount, Float, FloatCount, Date, DateOrigin, DateCount, Text, TextLen, TextCount


def handleIntFloat(vals, counts):
	count = np.sum(counts)
	maxVal = np.max(vals)
	minVal = np.min(vals)
	mean = np.dot(vals, counts)/count
	std = np.sqrt(np.dot((np.asarray(vals)-mean)**2, counts)/count)
	return count, maxVal, minVal, mean, std


def handleDate(dates, dates_origin, counts):
	count = np.sum(counts)
	maxInd = np.argmax(dates)
	maxVal = dates_origin[maxInd]
	minInd = np.argmin(dates)
	minVal = dates_origin[minInd]
	return count, maxVal, minVal


def handleText(texts, texts_len, counts):
	count = np.sum(counts)
	longestInd = np.argmax(texts_len)
	longestVal = texts[longestInd]
	shortestInd = np.argmin(texts_len)
	shortestVal = texts[shortestInd]
	avgLen = np.dot(texts_len, counts)/count
	return count, shortestVal, longestVal, avgLen

# add for loop for files
# list of files order by size

# fileName = '2232-dj5q.tsv.gz'

datasetSize = np.load("datasetSize.npy")
datasetName = list(datasetSize[:, 0])
fileName = datasetName[1662]

# start = int(sys.argv[1])
# end = int(sys.argv[2])
# cnt = start
# for fileName in datasetName[start:end]:


	# cnt += 1
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

	# for colname in tsv_columns:
list_val = []

def checkUdf(val):
	list_val.append(val)
	print(val)
	return 1

check = udf(lambda x: checkUdf(x))

nonEmpty.withColumn('test', check(colname))

colname = 'Base_Number'
print("-"*40)
print("Processing column:", colname.encode("utf-8"))
column = tsv_df.select(colname)
totCount = column.count()
empty = column.select(colname).where(col(colname).isNull())
emptyCount = empty.count()
nonEmpty = column.select(colname).where(col(colname).isNotNull())
nonEmptyCount = nonEmpty.count()
col_group = nonEmpty.groupBy(colname).count()
distinctCount = col_group.count()
nonEmp_list = col_group.sort(col("count").desc()).rdd.map(lambda x:(x[0], x[1])).collect()
top5 = nonEmp_list[:5]
jsonCol = {}
jsonCol['column_name'] = colname
jsonCol['number_non_empty_cells'] = nonEmptyCount
jsonCol['number_empty_cells'] = emptyCount
jsonCol['number_distinct_values'] = distinctCount
jsonCol['frequent_values'] = top5
jsonCol['data_types'] = []
Int, IntCount, Flt, FltCount, Date, DateOrigin, DateCount, Text, TextLen, TextCount = stringType(nonEmp_list)
if len(Int) > 0:
	countInt, maxValInt, minValInt, meanInt, stdInt = handleIntFloat(Int, IntCount)
	jsonInt = {}
	jsonInt['type'] = "INTEGER (LONG)"
	jsonInt['count'] = int(countInt)
	jsonInt['max_value'] = int(maxValInt)
	jsonInt['min_value'] = int(minValInt)
	jsonInt['mean'] = meanInt
	jsonInt['stddev'] = stdInt
	jsonCol['data_types'].append(jsonInt)
if len(Flt) > 0:
	countFlt, maxValFlt, minValFlt, meanFlt, stdFlt = handleIntFloat(Flt, FltCount)
	jsonFlt = {}
	jsonFlt['type'] = "REAL"
	jsonFlt['count'] = int(countFlt)
	jsonFlt['max_value'] = maxValFlt
	jsonFlt['min_value'] = minValFlt
	jsonFlt['mean'] = meanFlt
	jsonFlt['stddev'] = stdFlt
	jsonCol['data_types'].append(jsonFlt)
if len(Date) > 0:
	countDate, maxValDate, minValDate = handleDate(Date, DateOrigin, DateCount)
	jsonDate = {}
	jsonDate['type'] = "DATE/TIME"
	jsonDate['count'] = int(countDate)
	jsonDate['max_value'] = maxValDate
	jsonDate['min_value'] = minValDate
	jsonCol['data_types'].append(jsonDate)
if len(Text) > 0:
	countText, shortestText, longestText, avgLenText = handleText(Text, TextLen, TextCount)
	jsonText = {}
	jsonText['type'] = "TEXT"
	jsonText['count'] = int(countText)
	jsonText['shortest_values'] = shortestText
	jsonText['longest_values'] = longestText
	jsonText['average_length'] = avgLenText
	jsonCol['data_types'].append(jsonText)
jsonDict['columns'].append(jsonCol)
# 	print(json.dumps(jsonCol))
print("="*40+"\n")

	# with open('json/'+fileName+'.json', 'w') as outfile:
	#     json.dump(jsonDict, outfile)