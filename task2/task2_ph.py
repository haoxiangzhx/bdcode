import sys
import pyspark
from pyspark import SparkContext

from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

import pickle
import pandas as pd
import csv

from functools import reduce
from string import printable

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession \
        .builder \
        .appName("task2_1") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

start = int(sys.argv[1])
end = int(sys.argv[2])
cnt = start

pkl_dict=pickle.load(open('labels_ph.pkl','rb'))

filenames=pkl_dict['files']
colnames=pkl_dict['cols']
targets=pkl_dict['targets']
ids=pkl_dict['ids']

nfile=len(filenames)

# nulltype=['-','OTHER','N/A','Unspecified','UNKNOWN','-']
nulltype=['-','_','other','n/a','nan','unspecified','unknown']

folder='/user/hm74/NYCOpenData/'

#####################################################################################
## Single file--phone number
'''
cnt=0

filename='72mk-a8z7'
colname='ORGANIZATION PHONE'
target='phone_number'
id_=229


filename='8isn-pgv3'
colname='Owner Telephone'
target='phone_number'
id_=231


tsv_table=pd.read_table('NYCOpenData/'+filename+'.tsv.gz')
tsv_columns=tsv_table.columns
mySchema=StructType([StructField(col_,StringType(),True) for col_ in tsv_columns])
tsv_df = sqlContext.createDataFrame(tsv_table,schema=mySchema)

new_columns=list()
for clmn in tsv_columns:
	new_name = clmn.replace('\n','')
	new_name=''.join(ch for ch in new_name if ch in printable)
	new_columns.append(new_name)

tsv_df=reduce(lambda data, idx: data.withColumnRenamed(tsv_columns[idx], new_columns[idx]), range(len(tsv_columns)), tsv_df)

colname=''.join(ch for ch in colname if ch in printable)

col_df=tsv_df.select(F.col(colname).alias("value"))
clean_df=col_df.where(F.col(colname).isNotNull())

rm_df=clean_df.where((F.lower(F.col(colname)) =='-')|(F.lower(F.col(colname)) =='_')|(F.lower(F.col(colname)) ==nulltype[2])|(F.lower(F.col(colname)) ==nulltype[3])|(F.lower(F.col(colname)) ==nulltype[4])|(F.lower(F.col(colname)) ==nulltype[5])|(F.lower(F.col(colname)) ==nulltype[6]))
if rm_df.count()>0:
	clean_df=clean_df.subtract(rm_df)

trans=str.maketrans("","",'- )(X`*#')
clean_rdd=clean_df.rdd \
	.map(lambda x: x[0].translate(trans)) \
	.filter(lambda x: (len(x)==11) | (len(x)==10)) \
	.map(lambda x: x[1:] if len(x)==11 else x) \
	.map(lambda x:(x,))

trim_df=clean_rdd.toDF(['value'])

trim_df.createOrReplaceTempView("trim_df")
distinct_df=spark.sql("SELECT value, count(*) as count \
	FROM trim_df \
	GROUP BY value")


gt_df=distinct_df.withColumn('label',F.lit(target))

gt_rdd=gt_df.rdd \
	.sortBy(lambda x:x[0],True)
gt_list=[[row[0],row[2],row[1]]  for row in gt_rdd.collect()]

with open('labels/'+filename+'_'+"{0:0=3d}".format(id_)+'.csv','w',newline='', encoding='utf-8') as f:
	writer=csv.writer(f)
	writer.writerow(['value','label','count'])
	writer.writerows(gt_list)



'''
################################

for filename in filenames[start:end]:
	colname=colnames[cnt]
	target=targets[cnt]
	id_=ids[cnt]
	print("="*40)
	print("Processing file: %s %s id=%d)" % (filename,colname.encode("utf-8"),id_))

	tsv_table=pd.read_table('NYCOpenData/'+filename+'.tsv.gz')
	tsv_columns=tsv_table.columns
	mySchema=StructType([StructField(col_,StringType(),True) for col_ in tsv_columns])
	tsv_df = sqlContext.createDataFrame(tsv_table,schema=mySchema)

	new_columns=list()
	for clmn in tsv_columns:
		new_name = clmn.replace('\n','')
		new_name=''.join(ch for ch in new_name if ch in printable)
		new_columns.append(new_name)

	tsv_df=reduce(lambda data, idx: data.withColumnRenamed(tsv_columns[idx], new_columns[idx]), range(len(tsv_columns)), tsv_df)

	colname=''.join(ch for ch in colname if ch in printable)

	col_df=tsv_df.select(F.col(colname).alias("value"))
	clean_df=col_df.where(F.col(colname).isNotNull())

	rm_df=clean_df.where((F.lower(F.col(colname)) =='-')|(F.lower(F.col(colname)) =='_')|(F.lower(F.col(colname)) ==nulltype[2])|(F.lower(F.col(colname)) ==nulltype[3])|(F.lower(F.col(colname)) ==nulltype[4])|(F.lower(F.col(colname)) ==nulltype[5])|(F.lower(F.col(colname)) ==nulltype[6]))
	if rm_df.count()>0:
		clean_df=clean_df.subtract(rm_df)

	# trans=str.maketrans("","",'- )(X`*#')
			# .map(lambda x: x[0].lower().translate(trans)) \

	clean_rdd=clean_df.rdd \
		.map(lambda x: ''.join(c for c in x[0] if c.isdigit())) \
		.filter(lambda x: (len(x)==11) | (len(x)==10)) \
		.map(lambda x: x[1:] if len(x)==11 else x) \
		.map(lambda x:(x,))

	trim_df=clean_rdd.toDF(['value'])

	trim_df.createOrReplaceTempView("trim_df")
	distinct_df=spark.sql("SELECT value, count(*) as count \
		FROM trim_df \
		GROUP BY value")

	gt_df=distinct_df.withColumn('label',F.lit(target))

	gt_rdd=gt_df.rdd \
		.sortBy(lambda x:x[0],True)
	gt_list=[[row[0],row[2],row[1]]  for row in gt_rdd.collect()]

	with open('labels/'+filename+'_'+"{0:0=3d}".format(id_)+'.csv','w',newline='', encoding='utf-8') as f:
		writer=csv.writer(f)
		writer.writerow(['value','label','count'])
		writer.writerows(gt_list)

	cnt+=1


################################
# read saved ground truth
'''

load_gt=pd.read_csv('labels/'+filename+'.csv',header=0)
load_gt = sqlContext.createDataFrame(load_gt)


'''

