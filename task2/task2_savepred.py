import sys
import pyspark
from pyspark import SparkContext

from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

import pickle
import pandas as pd
import csv
import json

from functools import reduce
from string import printable


sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession \
        .builder \
        .appName("task2_json") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

start = int(sys.argv[1])
end = int(sys.argv[2])
cnt = start

pkl_dict=pickle.load(open('labels_all.pkl','rb'))

filenames=pkl_dict['files']
colnames=pkl_dict['cols']
ids=pkl_dict['ids']

# file='2bmr-jdsv'
# col_='DBA'
# id_=0


nulltype=['other','n/a','nan','unspecified','unknown','no name','noname','tbd','.','-','_'] # check tbd when save in json

semantictype=['person_name','business_name', 'phone_number', 'address', 'street_name', \
 'city', 'neighborhood', 'lat_lon_cord', 'zip_code', 'borough', 'school_name', 'color',  \
 'car_make', 'city_agency', 'area_of_study','subject_in_school', 'school_level', 'college_name', \
 'website', 'building_classification', 'vehicle_type', 'location_type', 'park_playground', 'other']


json_dict={}
json_spec=list()

for idx,file in enumerate(filenames[start:end]):
	col_=colnames[idx]
	id_=ids[idx]

	print("="*40)
	print("Processing file: %s %s id=%d)" % (file,col_.encode("utf-8"),id_))

	col_dict=dict()
	col_dict['column_name']=file+'.'+col_

	if id_ ==263:
		col_dict['semantic_types']=[ \
		{'semantic_type':'car_make','count':118816}, \
		{'semantic_type':'other','label':'car_model','count':719}]
		json_spec.append(col_dict)
		# print(col_dict.encode("utf-8"))
		continue
	elif id_==264:
		col_dict['semantic_types']=[ \
		{'semantic_type':'other','label':'car_model','count':646}]
		json_spec.append(col_dict)
		# print(col_dict.encode("utf-8"))
		continue

	sem_list=list()

	tsv_table=pd.read_csv('labels_all/'+file+'_'+"{0:0=3d}".format(id_)+'.csv')
	tsv_columns=tsv_table.columns
	mySchema=StructType([StructField('value',StringType(),True),
		StructField('label',StringType(),True),
		StructField('count',IntegerType (),True)])
	pred_df = sqlContext.createDataFrame(tsv_table,schema=mySchema)
	pred_df=pred_df.where((F.lower(F.col('value'))!='.')&(F.lower(F.col('value')) !='-')&(F.lower(F.col('value')) !='_')&(F.lower(F.col('value')) !=nulltype[0])&(F.lower(F.col('value')) !=nulltype[1])&(F.lower(F.col('value')) !=nulltype[2])&(F.lower(F.col('value')) !=nulltype[3])&(F.lower(F.col('value')) !=nulltype[4])&(F.lower(F.col('value')) !=nulltype[5])&(F.lower(F.col('value')) !=nulltype[6]) &(F.lower(F.col('value')) !=nulltype[7]))


	pred_df.createOrReplaceTempView("pred_df")
	pred_df=spark.sql("SELECT label, sum(count) as sum \
		FROM pred_df \
		GROUP BY label")
		
	pred_list=pred_df.rdd.collect()

	label_list=[row[0] for row in pred_list]
	count_list=[row[1] for row in pred_list]


	for idx,lb in enumerate(label_list):
		sem_dict=dict()
		if lb not in semantictype:
			sem_dict['semantic_type']='other'
			sem_dict['label']=lb
		else:
			sem_dict['semantic_type']=lb
		sem_dict['count']=count_list[idx]
		sem_list.append(sem_dict)

	col_dict['semantic_types']=sem_list

	json_spec.append(col_dict)
	# print(col_dict.encode("utf-8"))

json_dict['predicted_types']=json_spec

with open('json/task2.json', 'w') as f:
    json.dump(json_dict, f)
