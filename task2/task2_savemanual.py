# import sys
# import pyspark
# from pyspark import SparkContext

# from pyspark.sql import SparkSession,SQLContext
# from pyspark.sql.types import *
# from pyspark.sql import functions as F

import pickle
import pandas as pd
import csv
import json

from functools import reduce
from string import printable
import math


# start = int(sys.argv[1])
# end = int(sys.argv[2])
# cnt = start



# nulltype=['other','n/a','nan','unspecified','unknown','no name','noname','tbd','.','-','_'] # check tbd when save in json

# semantictype=['person_name','business_name', 'phone_number', 'address', 'street_name', \
#  'city', 'neighborhood', 'lat_lon_cord', 'zip_code', 'borough', 'school_name', 'color',  \
#  'car_make', 'city_agency', 'area_of_study','subject_in_school', 'school_level', 'college_name', \
#  'website', 'building_classification', 'vehicle_type', 'location_type', 'park_playground', 'other']

def checkfloat(v):
	try:
		float(v)
		return True
	except ValueError:
		return False


json_dict={}
json_spec=list()

manual=pd.read_csv('labels_gt.csv',header=0)
manual_label=manual.values.tolist()
for idx, gt in enumerate(manual_label):

	id_=gt[0]
	file=gt[1]
	col_=gt[2]
	col_dict=dict()
	col_dict['column_name']=file+'.'+col_
	sem_list=list()
	for lb in gt[3:]:
		if not checkfloat(lb):
			sem_dict={}
			sem_dict['semantic_type']=lb
			sem_list.append(sem_dict)
	col_dict['manual_labels']=sem_list
	json_spec.append(col_dict)
json_dict['actual_types']=json_spec
			

# print(json_dict)

with open('task2-manual-labels.json', 'w') as f:
    json.dump(json_dict, f)