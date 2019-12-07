import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from csv import reader
from pyspark import SparkContext
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql.functions import isnan
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, ArrayType, TimestampType, DateType
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

def checkDate(val):
	try:
		Dt = parser.parse(val, ignoretz=True)
		return Dt
	except:
		return None

checkDtTm = udf(lambda x: checkDate(x), TimestampType())
checkDt = udf(lambda x: checkDate(x), DateType())

# add for loop for files
# list of files order by size

# fileName = '2232-dj5q.tsv.gz'

datasetSize = np.load("datasetSize.npy")
datasetName = list(datasetSize[:, 0])

# start = int(sys.argv[1])
# end = int(sys.argv[2])
# cnt = start

# for fileName in datasetName[1858:1859]:

# fileName = datasetName[1858]
fileName = "2bmr-jdsv"
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

	# for colname in tsv_columns[1:2]:
colname = "Pickup_Start_Date"
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


		# speed up for datetime


jsonCol = {}
jsonCol['column_name'] = colname
jsonCol['number_non_empty_cells'] = nonEmptyCount
jsonCol['number_empty_cells'] = emptyCount
jsonCol['number_distinct_values'] = distinctCount
jsonCol['frequent_values'] = top5
jsonCol['data_types'] = []

# nonEmp_nonFloat = nonEmpty_nonInt.select(colname).where(nonEmpty_nonInt[colname].cast('float').isNull())

col_and_date = col_group.withColumn("date", checkDt(colname)).select(colname, "date")

# maxDate = col_and_date.select(colname).where(col("date") == col_and_date.select(max("date")))
maxDate = col_and_date.select(max("date")).collect()[0][0]
maxDateTime = col_and_date.select(colname).where(col_and_date["date"] == maxDate).collect()[0][0]



nonEmp_date = col_and_date.select("date").where(col_and_date["date"].isNotNull())
countDate = nonEmp_date.count()
if countDate > 0 :
	maxValDate = nonEmp_date.select(F.max("date")).rdd.map(lambda x:(x[0])).collect()[0]
	minValDate = nonEmp_date.select(F.min("date")).rdd.map(lambda x:(x[0])).collect()[0]
	jsonDate = {}
	jsonDate['type'] = "DATE/TIME"
	jsonDate['count'] = int(countDate)
	jsonDate['max_value'] = maxValDate
	jsonDate['min_value'] = minValDate
	jsonCol['data_types'].append(jsonDate)



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
