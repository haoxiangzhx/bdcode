import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from csv import reader
from pyspark import SparkContext
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql.functions import isnan
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, ArrayType, DateType, TimestampType
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

def checkInteger(val):
	if val.isdigit():
		return True
	else:
		return False

checkInt = F.udf(lambda x: checkInteger(x))

def checkDate(val):
	try:
		Dt = parser.parse(val, ignoretz=True)
		return Dt
	except:
		return None

checkDt = F.udf(lambda x: checkDate(x), TimestampType())

# add for loop for files
# list of files order by size

# fileName = '2232-dj5q.tsv.gz'

datasetSize = np.load("datasetSize.npy")
datasetName = list(datasetSize[:, 0])

start = int(sys.argv[1])
end = int(sys.argv[2])
cnt = start
# for fileName in datasetName[start:end]:

# fileName = '2bmr-jdsv'
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
		empty = column.select(colname).where(F.col(colname).isNull())
		emptyCount = empty.count()
		nonEmpty = column.select(colname).where(F.col(colname).isNotNull())
		nonEmptyCount = nonEmpty.count()
		col_group = nonEmpty.groupBy(colname).count()
		distinctCount = col_group.count()
		nonEmp_list = col_group.sort(F.col("count").desc()).rdd.map(lambda x:(x[0], x[1])).collect()
		top5 = nonEmp_list[:5]

		jsonCol = {}
		jsonCol['column_name'] = colname
		jsonCol['number_non_empty_cells'] = nonEmptyCount
		jsonCol['number_empty_cells'] = emptyCount
		jsonCol['number_distinct_values'] = distinctCount
		jsonCol['frequent_values'] = top5
		jsonCol['data_types'] = []

		nonEmpty_int = nonEmpty.select(colname).where(checkInt(colname)=='true')
		countInt = nonEmpty_int.count()
		if countInt > 0:
			int_df = nonEmpty_int.withColumn(colname, nonEmpty[colname].cast('int'))
			maxValInt = int_df.select(F.max(colname)).rdd.map(lambda x:(x[0])).collect()[0]
			minValInt = int_df.select(F.min(colname)).rdd.map(lambda x:(x[0])).collect()[0]
			meanInt = int_df.select(F.mean(colname)).rdd.map(lambda x:(x[0])).collect()[0]
			stdInt = int_df.select(F.stddev(colname)).rdd.map(lambda x:(x[0])).collect()[0]
			jsonInt = {}
			jsonInt['type'] = "INTEGER (LONG)"
			jsonInt['count'] = int(countInt)
			jsonInt['max_value'] = int(maxValInt)
			jsonInt['min_value'] = int(minValInt)
			jsonInt['mean'] = meanInt
			jsonInt['stddev'] = stdInt
			jsonCol['data_types'].append(jsonInt)

		nonEmpty_nonInt = nonEmpty.select(colname).where(checkInt(colname)=='false')
		nonEmp_float = nonEmpty_nonInt.select(colname).where(nonEmpty_nonInt[colname].cast('float').isNotNull())
		countFlt = nonEmp_float.count()
		if countFlt > 0:
			flt_df = nonEmp_float.withColumn(colname, nonEmp_float[colname].cast('float'))
			maxValFlt = flt_df.select(F.max(colname)).rdd.map(lambda x:(x[0])).collect()[0]
			minValFlt = flt_df.select(F.min(colname)).rdd.map(lambda x:(x[0])).collect()[0]
			meanFlt = flt_df.select(F.mean(colname)).rdd.map(lambda x:(x[0])).collect()[0]
			stdFlt = flt_df.select(F.stddev(colname)).rdd.map(lambda x:(x[0])).collect()[0]
			jsonFlt = {}
			jsonFlt['type'] = "REAL"
			jsonFlt['count'] = int(countFlt)
			jsonFlt['max_value'] = maxValFlt
			jsonFlt['min_value'] = minValFlt
			jsonFlt['mean'] = meanFlt
			jsonFlt['stddev'] = stdFlt
			jsonCol['data_types'].append(jsonFlt)

		nonEmp_nonFloat = nonEmpty_nonInt.select(colname).where(nonEmpty_nonInt[colname].cast('float').isNull())
		col_and_date = nonEmp_nonFloat.withColumn("date", checkDt(colname))
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

		nonEmp_nonDate = col_and_date.select(colname).where(col_and_date["date"].isNull())
		countText = nonEmp_nonDate.count()
		if countText > 0:
			text_df = nonEmp_nonDate.withColumn("len", F.length(colname)).sort(F.col("len").desc())
			longestText = text_df.first()[0]
			shortestText = text_df.sort(F.col("len").asc()).first()[0]
			avgLenText = text_df.select(F.mean("len")).first()[0]
			jsonText = {}
			jsonText['type'] = "TEXT"
			jsonText['count'] = int(countText)
			jsonText['shortest_values'] = shortestText
			jsonText['longest_values'] = longestText
			jsonText['average_length'] = avgLenText
			jsonCol['data_types'].append(jsonText)

		jsonDict['columns'].append(jsonCol)

	print("="*40+"\n")

with open('json/'+fileName+'.json', 'w') as outfile:
    json.dump(jsonDict, outfile)