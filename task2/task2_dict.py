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
        .appName("task2_2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

start = int(sys.argv[1])
end = int(sys.argv[2])
cnt = start

pkl_dict=pickle.load(open('labels_dict.pkl','rb'))

filenames=pkl_dict['files']
colnames=pkl_dict['cols']
ids=pkl_dict['ids']

nulltype=['other','n/a','nan','unspecified','unknown','no name','noname','tbd','.','-','_'] # check tbd when save in json

folder='/user/hm74/NYCOpenData/'

borough_=['k','m','q','r','x','bronx','manhattan','queens','staten island']


pkl_dict=pickle.load(open('color_dict.pkl','rb'))

color1_=pkl_dict['color1']
color2_=pkl_dict['color2']


pkl_dict=pickle.load(open('agency_dict.pkl','rb'))
agency_=pkl_dict

pkl_dict=pickle.load(open('building_dict.pkl','rb'))
build_class_=pkl_dict

pkl_dict=pickle.load(open('car_dict.pkl','rb'))
carmake_=pkl_dict['car_make']
carmodel_=pkl_dict['car_model']
vehicle_=pkl_dict['car_type']

pkl_dict=pickle.load(open('level_dict.pkl','rb'))
level_=pkl_dict

pkl_dict=pickle.load(open('study_dict.pkl','rb'))
study_=pkl_dict['area']
subject_=pkl_dict['subject']


def semantic_type(v):
	if v in color1_+color2_:
		return 'color'
	else:
		for c in color2_:
			if set(v).issubset(set(c)):
				return 'color'
	words_=v.split('-')
	if len(words_)>=2 and (set(words_) & set(build_class_)):
		return 'building_classification'
	if v in agency_:
		return 'city_agency'
	if v in borough_:
		return 'borough'
	if set(v) & set(level_):
		return 'school_level'
	if set(v) & set(vehicle_):
		return 'vehicle_type'
	words_=v.split(' ')
	if set(words_) & set(carmake_):
		if v in carmodel_:
			return 'car_model'
		else:
			return 'car_make'
	if v in study_:
		return 'area_of_study'
	if set(words_) & set(subject_):
		return 'subject_in_school'

	return None




##############################################################################

for filename in filenames[start:end]:
	colname=colnames[cnt]
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

	col_df=tsv_df.select(F.col(colname))
	clean_df=col_df.where(F.col(colname).isNotNull())

	rm_df=clean_df.where((F.lower(F.col(colname)) =='-')|(F.lower(F.col(colname)) =='_')|(F.lower(F.col(colname)) ==nulltype[2])|(F.lower(F.col(colname)) ==nulltype[3])|(F.lower(F.col(colname)) ==nulltype[4])|(F.lower(F.col(colname)) ==nulltype[5])|(F.lower(F.col(colname)) ==nulltype[6])||(F.lower(F.col(colname)) ==nulltype[7]))
	if rm_df.count()>0:
		clean_df=clean_df.subtract(rm_df)

	trans=str.maketrans("","",'&/)(`*#')

	clean_rdd=clean_df.rdd \
		.map(lambda x: x[0].lower().translate(trans)) \
		.map(lambda x: x.replace('   ',' ')) \
		.map(lambda x: x.replace('  ',' ')) \
		.map(lambda x:(x,1)) \
		.reduceByKey(lambda x,y: x+y) \
		.sortBy(lambda x: x[0],True)

	clean_list=clean_rdd.collect()
	value_list=[row[0] for row in clean_list]
	count_list=[row[1] for row in clean_list]


	label_list=list()
	for idx,v in enumerate(value_list):
		ct=count_list[idx]
		label=semantic_type(v)
		if label != None:
			label_list.append([v,label,ct])

	with open('labels_city/'+filename+'_'+"{0:0=3d}".format(id_)+'.csv','w',newline='', encoding='utf-8') as f:
		writer=csv.writer(f)
		writer.writerow(['value','label','count'])
		writer.writerows(label_list)
	cnt+=1
