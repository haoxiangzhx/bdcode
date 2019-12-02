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
            Dt = parser.parse(String)
            Date.append(Dt)
            DateOrigin.append(Dt)
        except:
            try:
                Dt = datetime.strptime(string, "%Y%m")
                if string[:4] < '2020':
                    Date.append(Dt)
                    DateOrigin.append(Dt)
                else:
                    Int.append(int(string))
            except:
                try:
                    Dt = datetime.strptime(string, "%Y-%y")
                    Date.append(Dt)
                    DateOrigin.append(Dt)
                except:
                    try:
                        Int = int(string)
                        Integer.append(Int)
                    except:
                        try:
                            Flt = float(string)
                            Float.append(Flt)
                        except:
                            Text.append(string)
                            TextLen.append(len(string))
    return Integer, Float, Date, DateOrigin, Text, TextLen

# For columns that contain at least one value of type INTEGER / REAL report:
# ● Maximum value
# ● Minimum value ● Mean
# ● Standard Deviation
# For columns that contain at least one value of type DATE report:
# ● Maximum value
# ● Minimum value
# For columns that contain at least one value of type TEXT report:
# ● Top-5 Shortest value(s) (the values with shortest length)
# ● Top-5 Longest values(s) (the values with longest length)
# ● Average value length
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

# add

fileName = '2232-dj5q.tsv.gz'
folder='/user/hm74/NYCOpenData/'
tsv_rdd=spark.read.format("csv") \
	.option("header","true") \
	.option("delimiter",'\t') \
	.load(folder+fileName)

jsonDict = {}
jsonDict['dataset_name'] = fileName
jsonDict['columns'] = []

tsv_columns = tsv_rdd.columns
tsv_df = tsv_rdd.toDF(*tsv_columns)
# fileName = '2232-dj5q.tsv'
# data = sc.textFile(fileName, 1)
# data = data.mapPartitions(lambda row: reader(row, delimiter = "\t"))
# header = tsv_rdd.first()
# data = tsv_rdd.filter(lambda row: row != header)
# data = spark.createDataFrame(data, header)

# ['category', 'single men', 'single women', 'total single adults', 
# 'families with children', 'total families', 'total adults in families', 
# 'total children', 'data period']
# 
# For each column in the dataset collection, you will extract the following metadata
# 1. Number of non-empty cells
# 2. Number of empty cells (i.e., cell with no data)
# 3. Number of distinct values
# 4. Top-5 most frequent value(s)
# 5. Data types (a column may contain values belonging to multiple types)
# 
# Identify the data types for each distinct column value as one of the following:
# ● INTEGER (LONG)
# ● REAL
# ● DATE/TIME
# ● TEXT
# 
# colname = 'single men'
# colname = 'SY1617 TOTAL REMOVALS/SUSPENSIONS'


for colname in tsv_columns:
	column = tsv_df.select(colname)
	totCount = column.count()
	empty = column.select(colname).where(col(colname).isNull())
	emptyCount = empty.count()
	nonEmpty = column.select(colname).where(col(colname).isNotNull())
	nonEmptyCount = nonEmpty.count()
	distinctCount = nonEmpty.distinct().count()
	top5 = nonEmpty.groupBy(colname).count().sort(col("count").desc()).rdd.map(lambda x:(x[0], x[1])).collect()[:5]
	json = {}
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
		jsonData = {}
		jsonData['type'] = "INTEGER (LONG)"
	if len(Flt) > 0:
		countFlt, maxValFlt, minValFlt, meanFlt, stdFlt = handleIntFloat(Int)
	if len(Date) > 0:
		countDate, maxValDate, minValDate = handleDate(Date, DateOrigin)
	if len(Text) > 0:
		countText, shortestText, longestText, avgLenText = handleText(Text, TextLen)

# .isDigit()
# nonEmpty = column.filter((col(colname) != "") & (col(colname) != " ") & (col(colname) != "NaN") & (col(colname) != "Unspecified") & (~isnan(col(colname))))
# empty = column.filter((col(colname) == "") | (col(colname) == " ") | (col(colname) == "NaN") | (col(colname) == "Unspecified") | (isnan(col(colname))))
# emptyCount = empty.count()

# nonEmpty = column.filter(~col(colname).isin(empty))

# emptyCount = totCount - nonEmptyCount

# distinctCount = nonEmpty.distinct().count()

# top5 = nonEmpty.groupBy(colname).count().sort(col("count").desc()).take(5)

# df_b.filter(~col('id').isin(a_ids))
# 
# test = sortCol
# colname = 'single men'
# test = test.withColumn(colname, test[colname].cast('int'))
# test.withColumn(colname, test[colname].cast('int'))
# test.show()

# test  = test.sort(col(colname).desc())
# test.select(
#   colname,
#   col(colname).cast(IntegerType()).isNotNull().alias("Value")
# ).show()

# def toInteger(number):
# 	try:
# 		nb = int(number)
# 		return nb
# 	except ValueError:
# 		return None

# sortCol = nonEmpty.sort(col(colname).desc())
# toInt = udf(lambda x:  toInteger(x))
# dfInt = sortCol.select(colname, toInt(sortCol[colname]).cast('int').isNotNull().alias("Value"))
# # dfInt = sortCol.withColumn(colname, toInt(sortCol[colname]).cast('int').isNotNull().alias("Value")).where(col(colname) != null)
# maxInt = dfInt.agg({colname: "max"})

# # changedTypedf = test.withColumn("int", toInt(test[colname]))
# changedTypedf.filter(isNotNull(changedTypedf['int']))

# 
# TODO: s0 add true/false column!! s1 toInt, s2 to Float, s3 toString, s4, to time
# 
# nb = None
# for cast in (int, float):
#     try:
#         nb = cast(number)
#         print(cast)
#         break
#     except ValueError:
#         pass

# column.groupBy(col(colname)) \
#     .withColumn("n", count(colname)) \
#     .show()
# for colname in ['single men']:
	# col = data[] 



