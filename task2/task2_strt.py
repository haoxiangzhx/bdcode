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

pkl_dict=pickle.load(open('labels_str.pkl','rb'))

filenames=pkl_dict['files']
colnames=pkl_dict['cols']
targets=pkl_dict['targets']
ids=pkl_dict['ids']

# nfile=len(filenames)

# split(' '), along,at,

street_=['street','streets','st','expy','expwy','expressway','express','boardwalk','walk', \
'pkw','pkwy','parkway','broadway','avenue','ave','av','road','rd','ro', \
'blvd','boulevard','blv','drive','dr','driveway', 'pl','place',"ct", 'railroad', \
'enter','entrance','exit','ext','freeway','loop', 'roadbed','boundary','slip', \
'highway','turnpike','line','lane','ramp', 'terrace','ter','terr', \
'bowery','bend','circle','cir', 'crescent', 'alley','path', \
'north','west','east','south','s','w','n','e','approach', 'thrway','thruway','throughway',\
'tunnel','bridge','neck','concourse','overpass','way','roadway'] 

building_=['plaza','mall','malls','stadium','center','ctr','aquatics','apartments']
park_=['garden','gardens','lake','pool','pond','park','playground','plgd','shore', \
'square','sqr', 'field', 'ballfield','esplanade','gore','pier','beach', 'fish','ferry', \
'zoo','marsh','run','tennis','berry','court','courts','courthouse','courtyard','zone', \
'green','course','valley','manor','cove','edge','crese','shoreline'] 
site_=['complex','terminal','bid','memorial','stops','stop',\
'central','highbridge','cemetery'] # and so ..
school_=['school','academy'] # school_name
college_=['college','university'] # college_name
strttype_=['bend','dead end','intersection','intersections', \
'ramp','nycta subway','pedestrian overpass','under viaduct','truck route'] #subway, boardwalk

# loctype_single=['street','pier','beach','esplanade','park','school','terminal']
loctype_full=['public school','private parochial school','chain store', \
'abandoned building','airport terminal','body of water']


# loctype_part=['street','pier','beach','esplanade','park','school', \
# 'subway','terminal', 'airport','bank','church','' \
# 'abandoned building','airport terminal'] #? remove / 2 or 1
# loc+str types:location_type, 

#on ,at, most address, some avenue, check if has digit
#addressL number split to int or has digit+ street key words
business_=['llc','limit']

nulltype=['other','n/a','nan','unspecified','unknown','no name','noname','tbd','.','-','_'] # check tbd when save in json

folder='/user/hm74/NYCOpenData/'

def checkfloat(v):
	try:
		float(v)
		return True
	except ValueError:
		return False


def semantic_type(v):
	if checkfloat(v):
		if math.isnan(v):
			return None
	# print(type(v))
	keywords=v.split(' ')
	if v=='bowery' or v =='broadway':	
		return 'street_name'
	if len(keywords) ==1 and not any(ch.isdigit() for ch in keywords[0]):
		return 'location_type'
	else:
		if ct in strttype_ or ct in loctype_full:
			return 'location_type'
		if (set(keywords) & set(['at','on','along','and','corner','between'])) and (set(keywords) & set(street_)):
			return 'address'
		if len(keywords)==2:
			if (keywords[1]=='bowery' or keywords[1]=='broadway') and any(ch.isdigit() for ch in keywords[0]):
				return'address'
			elif (keywords[0]=='us' or keywords[0] =='i') and keywords[1].isdigit():
				return 'street_name'
		if any(ch.isdigit() for ch in keywords[0]) and len(keywords) >= 3:
			return 'address'
		strt_candi=set(keywords)  & set(street_)
		park_candi=set(keywords) & set(park_)
		build_candi=set(keywords) & set(building_)
		site_candi=set(keywords)  & set(site_)
		sch_candi=set(keywords)  & set(school_)
		col_candi=set(keywords)  & set(college_)
		busi_candi=set(keywords)  & set(business_)
		temp_kw=keywords[::-1]
		# print(keywords,temp_kw)
		strt_index=[temp_kw.index(ele) if ele!=-1 else 10000 for ele in (list(strt_candi) if strt_candi else [-1] ) ]
		park_index=[temp_kw.index(ele) if ele!=-1 else 10000 for ele in (list(park_candi) if park_candi else [-1])]
		build_index=[temp_kw.index(ele) if ele!=-1 else 10000 for ele in (list(build_candi) if build_candi else [-1])]
		site_index=[temp_kw.index(ele) if ele!=-1 else 10000 for ele in (list(site_candi) if site_candi else [-1])]
		sch_index=[temp_kw.index(ele) if ele!=-1 else 10000 for ele in (list(sch_candi) if sch_candi else [-1])]
		col_index=[temp_kw.index(ele) if ele!=-1 else 10000 for ele in (list(col_candi) if col_candi else [-1])]
		busi_index=[temp_kw.index(ele) if ele!=-1 else 10000 for ele in (list(busi_candi) if busi_candi else [-1])]
		index_=[min(strt_index),min(park_index),min(build_index),min(site_index),min(sch_index),min(col_index),min(busi_index)]
		candi_=['street_name','park_playground','building_name','site_name','school_name','college_name','business_name']
		if min(index_)==10000:
			return None
		else:
			label_=candi_[index_.index(min(index_))]
			return label_

#####################################################################################
## Single file--phone number
'''s
# filename='4twk-9yq2'
# colname='CrossStreet2'
# target='street_name'
# id_=238


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

rm_df=clean_df.where((F.lower(F.col(colname)) =='.')|(F.lower(F.col(colname)) =='-')|(F.lower(F.col(colname)) =='_')|(F.lower(F.col(colname)) ==nulltype[0])|(F.lower(F.col(colname)) ==nulltype[1])|(F.lower(F.col(colname)) ==nulltype[2])|(F.lower(F.col(colname)) ==nulltype[3])|(F.lower(F.col(colname)) ==nulltype[4])|(F.lower(F.col(colname)) ==nulltype[5])|(F.lower(F.col(colname)) ==nulltype[6]) |(F.lower(F.col(colname)) ==nulltype[7]))
if rm_df.count()>0:
	clean_df=clean_df.subtract(rm_df)

# trans1=str.maketrans("","",')(`*#')
trans=str.maketrans("-&/"," "*3,')(`*#')

clean_rdd=clean_df.rdd \
	.map(lambda x: x[0].lower().translate(trans)) \
	.map(lambda x: x.replace('   ',' ')) \
	.map(lambda x: x.replace('  ',' ')) \
	.map(lambda x:(x,1)) \
	.reduceByKey(lambda x,y: x+y)

clean_list=clean_rdd.collect()
value_list=[row[0] for row in clean_list]
count_list=[row[1] for row in clean_list]


label_list=list()
for idx,v in enumerate(value_list):
	ct=count_list[idx]
	label=semantic_type(v)
	if label != None:
		label_list.append([v,label,ct])

gt_rdd=sc.parallelize(label_list) \
    .sortBy(lambda x: x[2],False)

gt_list=[[row[0],row[1],row[2]]  for row in gt_rdd.collect()]

with open('labels_str/'+filename+'_'+"{0:0=3d}".format(id_)+'.csv','w',newline='', encoding='utf-8') as f:
	writer=csv.writer(f)
	writer.writerow(['value','label','count'])
	writer.writerows(gt_list)


'''
################################
# '''

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

	rm_df=clean_df.where((F.lower(F.col(colname)) =='.')|(F.lower(F.col(colname)) =='-')|(F.lower(F.col(colname)) =='_')|(F.lower(F.col(colname)) ==nulltype[0])|(F.lower(F.col(colname)) ==nulltype[1])|(F.lower(F.col(colname)) ==nulltype[2])|(F.lower(F.col(colname)) ==nulltype[3])|(F.lower(F.col(colname)) ==nulltype[4])|(F.lower(F.col(colname)) ==nulltype[5])|(F.lower(F.col(colname)) ==nulltype[6]) |(F.lower(F.col(colname)) ==nulltype[7]))
	if rm_df.count()>0:
		clean_df=clean_df.subtract(rm_df)

	# trans1=str.maketrans("","",')(`*#')
	trans=str.maketrans("-&/"," "*3,')(`*#')

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

	# gt_rdd=sc.parallelize(label_list) \
	#     .sortBy(lambda x: x[1],True) # -> True

	# gt_list=[[row[0],row[1],row[2]]  for row in gt_rdd.collect()]

	with open('labels_str/'+filename+'_'+"{0:0=3d}".format(id_)+'.csv','w',newline='', encoding='utf-8') as f:
		writer=csv.writer(f)
		writer.writerow(['value','label','count'])
		writer.writerows(label_list)


	cnt+=1
