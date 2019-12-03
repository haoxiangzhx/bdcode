import os
import csv
import numpy as np

csv_file = open('./NYCOpenData/datasets.tsv')
csv_reader = csv.reader(csv_file, delimiter='\t')

file_size = []
for row in csv_reader:
	file_name = row[0]
	file_path = './NYCOpenData/'+file_name+'.tsv.gz'
	size = os.path.getsize(file_path)
	file_size.append([file_name, size])

def sortSecond(val): 
    return val[1]

file_size.sort(key = sortSecond, reverse = False)

datasetSize = np.array(file_size)
np.save("datasetSize", datasetSize)

datasetSize = np.load("datasetSize.npy")
datasetName = list(datasetSize[:, 0])

with open('datasetSize.csv', 'w', newline='') as out_f: # Python 3
    w = csv.writer(out_f, delimiter='\t')        # override for tab delimiter
    w.writerows(file_size) 