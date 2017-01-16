#!/usr/bin/python

import csv
import itertools
import argparse
from sets import Set
import pickle
import gzip
import tarfile
import os
from subprocess import check_call
from os import listdir
from os.path import isfile, join
from shutil import copyfile
import glob

def argparser():
    parser = argparse.ArgumentParser(description='Specify parameters')
    parser.add_argument('-i', '--input', dest='input_folder', default=None, required=True, help='Full path to input folder to read from')
    parser.add_argument('-o', '--output', dest='output_folder', default=None, required=False, help='Full path to output folder to write to')
    parser.add_argument('-t', '--temp', dest='temp_folder', default='/tmp/dcc', required=False, help='Full path to location for temporary file creation')
    parser.add_argument('-d', '--delimiter', dest='delimiter', default='\t', required=False, help='Delimiter')
    parser.add_argument('-v', '--verbose', dest='verbose', default=False, action='store_true', required=False)
    args = parser.parse_args()
    return args

def gUnZipFIle(fullFIlePath):
    zipFile = gzip.open(fullFIlePath,"rb")
    unCompressedFile = open('/Users/Sergey/Work/output/sample_compress/sample_compress.data',"wb")
    decoded = zipFile.read()
    unCompressedFile.write(decoded)
    zipFile.close()
    unCompressedFile.close()



def transform_file(input_folder):
    os.chdir(input_folder)
    for file in glob.glob("*.data.*"):
        print("Data file found: " + file)

#    with open(input_fname,'rb') as f:
#        #Skip headers from original file
#        h = int(params.skip)
#        [f.readline() for x in xrange(h)]
#
#        reader = csv.DictReader(f,delimiter=params.delimiter)
#        headers = reader.fieldnames
#        output_file = gzip.open(params.temp_folder + fname + '.data.gz','w+')
#        writer = csv.writer(output_file,delimiter=params.delimiter)
#
#        for column in compressed_columns:
#            dictionaries[column]={}
#            counters[column]=0
#
#        for line in reader:
#            outrow = list()
#            for header in headers:
#    # fill dictionary
#                if header in compressed_columns:
#                    if line[header] not in dictionaries[header]:
#                        dictionaries[header][line[header]]=counters[header]
#                        counters[header] = counters[header]+1
#                    outrow.append(dictionaries[header][line[header]])
#                else:
#                    outrow.append(line[header])
#            writer.writerow(outrow)
#        output_file.close()
#        if params.verbose:
#            print('---------------------')
#            print('Dictionaries created: ')
#            print('---------------------')
#        for header in compressed_columns:
#            dict_name = params.temp_folder + fname + '.map.'+header
#            if params.verbose:
#                print(dict_name)
#            with open(dict_name, 'wb') as handle:
#                pickle.dump(dictionaries[header], handle, protocol=pickle.HIGHEST_PROTOCOL)
#                handle.close()
#                gZipFile(dict_name)


params = argparser()
#Set output_folder parameter if not defined
if params.output_folder == None:
    params.output_folder = params.input_folder

#Get only files from input directory
onlyfiles = [f for f in listdir(params.input_folder) if isfile(join(params.input_folder, f)) and not f.startswith('.')]

if params.verbose:
    print('Number of files in input directory: ' + str(len(onlyfiles)))
    print('')
    print('Files: ')
    print(onlyfiles)


transform_file('/Users/Sergey/Work/output/sample_compress/')
