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

def untar_file(fullFilePath, temp_folder):
    tar = tarfile.open(fullFilePath)
    tar.extractall(path=temp_folder)
    tar.close()

def gunzip_file(fullFilePath):
    zipFile = gzip.open(fullFilePath,"rb")
#    DEBUG
#    print("FullFilePath: "+ fullFilePath)
#    print("FullFilePath for output file: " + fullFilePath[:-3])
    unCompressedFile = open(fullFilePath[:-3],"wb")
    decoded = zipFile.read()
    unCompressedFile.write(decoded)
    zipFile.close()
    unCompressedFile.close()
    #Remove .gz file
    try:
        if os.path.isfile(fullFilePath):
            os.unlink(fullFilePath)
        #elif os.path.isdir(file_path): shutil.rmtree(file_path)
    except Exception as e:
        print(e)

def delete_temp_files(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)

def decompress_file(temp_folder, output_fname):
    os.chdir(temp_folder)
    for file in glob.glob("*.header"):
        header = file
        print('')
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
    return(output_fname)

params = argparser()

#Set output_folder parameter if not defined
if params.output_folder == None:
    params.output_folder = params.input_folder

#Check wether temp_folder exists
if  not os.path.exists(params.temp_folder):
#    print(os.path.exists(params.output_folder))
    print("Temp directory doesn't exist.")
    print("Exiting script.")
    exit()

#Check wether output_folder exists
if  not os.path.exists(params.output_folder):
#    print(os.path.exists(params.output_folder))
    print("Output directory doesn't exist.")
    print("Exiting script.")
    exit()

#Get only files from input directory
onlyfiles = [f for f in listdir(params.input_folder) if isfile(join(params.input_folder, f)) and not f.startswith('.')]

if params.verbose:
    print('Number of files in input directory: ' + str(len(onlyfiles)))
    print('')
    print('Files: ')
    print(onlyfiles)
    print('')

i=0
#Process files one by one from input directory
while i < len(onlyfiles):
    print('Processing file: ')
    print(onlyfiles[i])
    print('----------------')

    untar_file(params.input_folder+onlyfiles[i], params.temp_folder)
#    untar_dir = params.temp_folder
    os.chdir(params.temp_folder)
    gz_files = []
    for file in glob.glob("*.gz"):
        gz_files.append(file,)
#        print file
    print('')
#    DEBUG
#    print("GZ_files: " + str(gz_files))
    j = 0
    while j < len(gz_files):
        if params.verbose:
            print("GZ file found: " + gz_files[j])
            print("Gunzip file path: " + params.temp_folder + gz_files[j])
        gunzip_file(params.temp_folder + gz_files[j])
        j = j+1
    a = decompress_file(params.temp_folder, onlyfiles[i][:-4])
    print('')
    print("Output_fname: " + onlyfiles[i][:-4])
    print('')
#    print("Result: " + a)
#    Copy output csv file to output directory
    copyfile(a,params.output_folder+a)
    delete_temp_files(params.temp_folder)
#    break
    i = i+1
print("Done.")
