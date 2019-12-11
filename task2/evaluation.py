import pandas as pd
import csv
import json

from functools import reduce
from string import printable
import math

def checkfloat(v):
	try:
		float(v)
		return True
	except ValueError:
		return False

semantictype=['person_name','business_name', 'phone_number', 'address', 'street_name', \
 'city', 'neighborhood', 'lat_lon_cord', 'zip_code', 'borough', 'school_name', 'color',  \
 'car_make', 'city_agency', 'area_of_study','subject_in_school', 'school_level', 'college_name', \
 'website', 'building_classification', 'vehicle_type', 'location_type', 'park_playground']

othertype=['year','job','state','country','village','hamlet','building_name','site_name','car_model','address_number']


eval_=dict()
for type_ in semantictype+othertype:
	eval_[type_]={'type':type_,'correct':0,'sum_recall':0,'sum_precision':0}

labels_comp=pd.read_csv('evaluation.csv',header=0)
labels=labels_comp.values.tolist()
for idx, lbs in enumerate(labels):
	manual_=lbs[3:12]
	pred_=lbs[12:21]
	manual_=[man for man in manual_ if not checkfloat(man)]
	pred_=[pred for pred in pred_ if not checkfloat(pred)]
	corr=list(set(manual_) & set(pred_))
	# precision=[pred for pred in pred_ if pred not in corr]
	# recall=[man for man in manual_ if man not in corr]
	for lb in list(set(manual_+pred_)):
		if lb in corr:
			eval_[lb]['correct']+=1
		if lb in manual_:
			eval_[lb]['sum_recall']+=1
		if lb in pred_:
			eval_[lb]['sum_precision']+=1


for type_ in semantictype+othertype:
	if eval_[type_]['correct'] >0:
		eval_[type_]['recall']=float(eval_[type_]['correct']/eval_[type_]['sum_recall'])
		eval_[type_]['precision']=float(eval_[type_]['correct']/eval_[type_]['sum_precision'])
	print(eval_[type_])


# print(len(semantictype+othertype))
# print(len(semantictype))
# print(len(othertype))


