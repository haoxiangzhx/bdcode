import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from csv import reader
from pyspark import SparkContext
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql.functions import isnan
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, ArrayType

sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("final") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

fileName = '2232-dj5q.tsv.gz'
folder='/user/hm74/NYCOpenData/'
tsv_rdd=spark.read.format("csv") \
	.option("header","true") \
	.option("delimiter",'\t') \
	.load(folder+fileName)

columns = tsv_rdd.columns
tsv_df = tsv_rdd.toDF(*columns)
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
colname = 'single men'

column = data.select(colname)
totCount = column.count()

nonEmpty = column.filter((data[colname] != "") & (data[colname] != " ") & (data[colname] != "NaN") & (data[colname] != "Unspecified") & (~isnan(data[colname])))
nonEmptyCount = nonEmpty.count()

emptyCount = totCount - nonEmptyCount

distinctCount = nonEmpty.distinct().count()

top5 = nonEmpty.groupBy(colname).count().sort(col("count").desc()).take(5)

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

def toInteger(number):
	try:
		nb = int(number)
		return nb
	except ValueError:
		return None

sortCol = nonEmpty.sort(col(colname).desc())
toInt = udf(lambda x:  toInteger(x))
dfInt = sortCol.select(colname, toInt(sortCol[colname]).cast('int').isNotNull().alias("Value"))
# dfInt = sortCol.withColumn(colname, toInt(sortCol[colname]).cast('int').isNotNull().alias("Value")).where(col(colname) != null)
maxInt = dfInt.agg({colname: "max"})

# changedTypedf = test.withColumn("int", toInt(test[colname]))
changedTypedf.filter(isNotNull(changedTypedf['int']))

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



