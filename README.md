# CS6513 Big Data Final Project

In this task, we explore different big data strategies (such as map reduce in spark and spark rdd programming) to effectively identify the data types of each column from a large collection of open datasets. Also, we develop methods to detect semantic types of specific columns. More details are in our report.

## Group Members:
- Haoxiang Zhang
- Shuya Zhao

## Structure

The structure of our project is below:

* [Task1](task1)
	* [version1](task1/slowVersion.py) 
	* [version2](task1/task1.py)
  * [version3](task1/mapReduceVersion.py)
* [Task2](task2/)
	* [Manually label columns](https://github.com/haoxiangzhx/bdcode/blob/master/task2/task2_gt0.py)
	* [Predict street_name/address/park_playground/location_type](https://github.com/haoxiangzhx/bdcode/blob/master/task2/task2_strt.py)
	* [Predict city/borough/neighborhood](https://github.com/haoxiangzhx/bdcode/blob/master/task2/task2_city.py)
	* [Save task2.json](https://github.com/haoxiangzhx/bdcode/blob/master/task2/task2_savepred.py)
	* [Save task2-manual-labels.json](https://github.com/haoxiangzhx/bdcode/blob/master/task2/task2_savemanual.py)
	* [Calculate Precision and Recall](https://github.com/haoxiangzhx/bdcode/blob/master/task2/evaluation.py)
	* [Visualize histogram](https://github.com/haoxiangzhx/bdcode/blob/master/task2/visualize.py)
* [Task2 Additional Strategy](task2AdditionalStrategy) 
* [Utils](utils)	

## Prerequisites

**1. a large collection of open datasets**

You can find these datasets on many open data portal such as [NYC Open Data](https://opendata.cityofnewyork.us/data/)

**2. access to NYU dumbo**

## How to run task1
- `module load python/gnu/3.6.5`

- `module load spark/2.4.0`

- `time spark-submit task1/task1.py 200 400` to run from 200th file to 400th file

- `time spark-submit --master local[6] --conf spark.executor.memoryOverhead=3G --conf spark.driver.maxResultSize=2G --driver-memory 16G task1.py` to deal with large datasets
