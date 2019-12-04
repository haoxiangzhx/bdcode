import pandas as pd
import numpy as np
import os  
import shutil

groundtruth_pd = pd.read_csv('./groundtruth.csv', sep=',', header=0)
filename = groundtruth_pd['Filename'].tolist()
filename_list = []
for file in np.unique(filename):
    filename_list.append(file.strip())


for file in filename_list[1:]:
	source = 'NYCOpenData/'+file+'.tsv.gz'
	destination = 'task2Data'
	dest = shutil.move(source, destination, copy_function = shutil.copytree)