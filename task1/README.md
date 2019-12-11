# Task1

## Prerequisites

**1. a large collection of open datasets**

You can find these datasets on many open data portal such as [NYC Open Data](https://opendata.cityofnewyork.us/data/)

**2. access to NYU dumbo**

## How to run task1
- `module load python/gnu/3.6.5`

- `module load spark/2.4.0`

- `time spark-submit task1/task1.py 200 400` to run from 200th file to 400th file

- `time spark-submit --master local[6] --conf spark.executor.memoryOverhead=3G --conf spark.driver.maxResultSize=2G --driver-memory 16G task1.py` to deal with large datasets
