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

def transform_file(temp_folder, output_fname):
    os.chdir(temp_folder)
    for file in glob.glob("*.header"):
        header = file
        print("Header file found: " + header)

    for file in glob.glob("*.data"):
        data_file = file
        print("Data file found: " + data_file)

    maps = []
    column_dictionary = dict()
    i = 0
    for file in glob.glob("*.map.*"):
        maps.append(file,)
#        print file
    print('')
    while i < len(maps):
        print("Map file found: " + maps[i])
        column_dictionary[maps[i].split('.')[-1]] = pickle.load(open(temp_folder+maps[i],'rb'))
        i = i+1
#    DEBUG
#    print("Fieldnames: " + str(fieldnames))
#    print("")
#    print("Column_dictionary: " + str(column_dictionary))

    with open(output_fname,'w+') as fo:
        fieldnames_str = None
        with open(header,'rb') as f:
            for line in f:
    #                print(line)
                fieldnames_str = line
                fo.write(line)
        with open(data_file,'rb') as f:
            fieldnames = fieldnames_str[:-1].split(params.delimiter)
            reader = csv.DictReader(f,delimiter=params.delimiter,fieldnames=fieldnames)
            writer = csv.writer(fo,delimiter=params.delimiter)
            for line in reader:
#                print("Line: " +str(line))
                result = []
                for field in fieldnames:
                    v = None
                    if field in column_dictionary:
#                        DEBUG
#                        print("Field: " + str(field))
#                        print("column_dictionary[field]: " + str(column_dictionary[field]))
                        v = column_dictionary[field][int(line[field])]
                        line[field] = v
                    result.append(line[field])
                writer.writerow(result)

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
    print('')

transform_file('/Users/Sergey/Work/temp/','/Users/Sergey/Work/temp/output/sample_2.txt.csv')
