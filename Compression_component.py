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

def argparser():
    parser = argparse.ArgumentParser(description='Specify parameters')
    parser.add_argument('-i', '--input', dest='input_folder', default=None, required=True, help='Full path to input folder to read from')
    parser.add_argument('-o', '--output', dest='output_folder', default=None, required=False, help='Full path to output folder to write to')
    parser.add_argument('-t', '--temp', dest='temp_folder', default='/tmp/dcc', required=False, help='Full path to location for temporary file creation')
    parser.add_argument('-d', '--delimiter', dest='delimiter', default='\t', required=False, help='Delimiter')
    parser.add_argument('-s', '--skip', dest='skip', default=1, required=False, help='Number of header rows to skip prior to sorting data')
    parser.add_argument('-a', '--analysis', dest='analysis', default=50000, required=False, help='Number of rows for cardinality calculation')
    parser.add_argument('-v', '--verbose', dest='verbose', default=False, action='store_true', required=False)
    args = parser.parse_args()
    return args

def gZipFile(fullFilePath):
    check_call(['gzip', fullFilePath])

def analyze_file(fname):
    print('')
    print('==============')
    print('Analyzing file ' + fname)
    print('==============')
    with open(fname,'rb') as f:
        reader = csv.DictReader(f,delimiter=params.delimiter)
        #Skip headers
        h = int(params.skip)
        [f.readline() for x in xrange(h)]
        headers = reader.fieldnames
        #Define map
        unique_col_values = {}
        for i in headers:
            unique_col_values[i] = Set()
        for row in itertools.islice(reader, int(params.analysis)):
#            DEBUG
#            print(row)
            for fieldname in headers:
                value = row[fieldname]
                unique_col_values[fieldname].add(value)
        print('Fieldnames in file:')
        print('-------------------')
        for fieldname in headers:
            print('Fieldname: <' + fieldname + '> distinct count: ' + str(len(unique_col_values[fieldname]))+ '; cardinality %: ' + str(100*len(unique_col_values[fieldname])/int(params.analysis)) )

        print('----------------------------------')
        print('Fieldnames with cardinality < 5 %:')
        print('----------------------------------')
        i = 0
        compressed_columns = list()
        for fieldname in headers:
            if 100*len(unique_col_values[fieldname])/int(params.analysis) < 5:
                compressed_columns.append(fieldname)
                i = i+1
                print('Fieldname: <' + fieldname + '> distinct count: ' + str(len(unique_col_values[fieldname]))+ '; cardinality %: ' + str(100*len(unique_col_values[fieldname])/int(params.analysis)) )
#        DEBUG
#        print('Compressed columns: ')
#        print(compressed_columns)
        return compressed_columns

def make_header(fname):
    with open(fname,'rb') as f:
        #Create file containing header information
        output_headers = gzip.open(params.temp_folder+'sample_compress.header.gz','w+')
        #Number of lines in header from comand line parameters
        h = int(params.skip)
#        print(h)
        output_headers.writelines([f.readline() for x in xrange(h+1)])
        output_headers.close()

def transform_file(fname):
    with open(fname,'rb') as f:
        #Skip headers from original file
        h = int(params.skip)
        [f.readline() for x in xrange(h)]

        reader = csv.DictReader(f,delimiter=params.delimiter)
        headers = reader.fieldnames
        output_file = gzip.open(params.temp_folder + 'sample_compress.data.gz','w+')
        writer = csv.writer(output_file,delimiter=params.delimiter)

        dictionaries = {}
        counters = {}
        for column in compressed_columns:
            dictionaries[column]={}
            counters[column]=0

        for line in reader:
            outrow = list()
            for header in headers:
    # fill dictionary
                if header in compressed_columns:
                    if line[header] not in dictionaries[header]:
                        dictionaries[header][line[header]]=counters[header]
                        counters[header] = counters[header]+1
                    outrow.append(dictionaries[header][line[header]])
                else:
                    outrow.append(line[header])
            writer.writerow(outrow)
        output_file.close()
        print('---------------------')
        print('Dictionaries created: ')
        print('---------------------')
        for header in compressed_columns:
            dict_name = params.temp_folder+'sample_compress.map.'+header
            print(dict_name)
            with open(dict_name, 'wb') as handle:
                pickle.dump(dictionaries[header], handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
                gZipFile(dict_name)

def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

def delete_temp_files(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)


params = argparser()
#Set output_folder parameter if not defined
if params.output_folder == None:
    params.output_folder = params.input_folder

#Get only files from input directory
onlyfiles = [f for f in listdir(params.input_folder) if isfile(join(params.input_folder, f))]
print('Number of files in input directory: ' + str(len(onlyfiles)))
print('')
print('Files: ')
print(onlyfiles)

i=0
#Process files one by one from input directory
while i < len(onlyfiles):
#    DEBUG
#    print('==========')
#    print('Processing file: ')
#    print(onlyfiles[i])
    compressed_columns = analyze_file(params.input_folder+onlyfiles[i])
    make_header(params.input_folder+onlyfiles[i])
    transform_file(params.input_folder+onlyfiles[i])
    make_tarfile(params.output_folder + onlyfiles[i] + '.tar',params.temp_folder)
    delete_temp_files(params.temp_folder)
    i=i+1
