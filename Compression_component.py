#!/usr/bin/python

import csv
import itertools
import argparse
from sets import Set
import pickle
import gzip
import tarfile
import os


def argparser():
    parser = argparse.ArgumentParser(description='Specify parameters')
    parser.add_argument('-i', '--input', dest='input_folder', default=None, required=True, help='Full path to input folder to read from')
    parser.add_argument('-o', '--output', dest='output_folder', default=None, required=False, help='Full path to output folder to write to')
    parser.add_argument('-t', '--temp', dest='temp_dir', default='/tmp/dcc', required=False, help='Full path to location for temporary file creation')
    parser.add_argument('-d', '--delimeter', dest='delimeter', default='\t', required=False, help='Delimeter')
    parser.add_argument('-s', '--skip', dest='skip', default=1, required=False, help='Number of header rows to skip prior to sorting data')
    parser.add_argument('-a', '--analysis', dest='analysis', default=50000, required=False, help='Number of rows for cardinality calculation')
    parser.add_argument('-v', '--verbose', dest='verbose', default=False, action='store_true', required=False)
    args = parser.parse_args()
    return args

def analyze_file(fname):
    with open(fname,'rb') as f:
        #Create file containing header information
        output_headers = gzip.open(params.output_folder+'sample_compress.header.gz','w+')
        #Number of lines in header from comand line parameters
        h = int(params.skip)
#        print(h)
        output_headers.writelines([f.readline() for x in xrange(h)])
        output_headers.close()

        reader = csv.DictReader(f,delimiter=params.delimeter)
        headers = reader.fieldnames
        #Define map
        unique_col_values = {}
        for i in headers:
            unique_col_values[i] = Set()
        for row in itertools.islice(reader, int(params.analysis)):
#            print(row)
            for fieldname in headers:
                value = row[fieldname]
                unique_col_values[fieldname].add(value)
        print('Fieldnames in file:')
        for fieldname in headers:
            print('Fieldname: <' + fieldname + '> distinct count: ' + str(len(unique_col_values[fieldname]))+ '; cardinality %: ' + str(100*len(unique_col_values[fieldname])/int(params.analysis)) )

        print('')
        print('Fieldnames with cardinality < 5 %:')

        i = 0
        compressed_columns = list()
        for fieldname in headers:
            if 100*len(unique_col_values[fieldname])/int(params.analysis) < 5:
                compressed_columns.append(fieldname)
                i = i+1
                print('Fieldname: <' + fieldname + '> distinct count: ' + str(len(unique_col_values[fieldname]))+ '; cardinality %: ' + str(100*len(unique_col_values[fieldname])/int(params.analysis)) )
        print('Compressed columns: ')
        print(compressed_columns)
        return compressed_columns

def transform_file(fname):
    with open(fname,'rb') as f:
        #Skip headers from original file
        h = int(params.skip)
        [f.readline() for x in xrange(h)]

        reader = csv.DictReader(f,delimiter='\t')
        headers = reader.fieldnames
        output_file = gzip.open(params.output_folder + 'sample_compress.data.gz','w+')
        writer = csv.writer(output_file,delimiter='\t')

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
        print dictionaries['Type']
        print('Dictionaries created: ')
        for header in compressed_columns:
            dict_name = params.output_folder+'sample_compress.map.'+header+'.gz'
            print(dict_name)
            with open(dict_name, 'wb') as handle:
                pickle.dump(dictionaries[header], handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()

def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

params = argparser()
#Set output_folder parameter if not defined
if params.output_folder == None:
    params.output_folder = params.input_folder

compressed_columns = analyze_file(params.input_folder+'sample_compress')
transform_file(params.input_folder+'sample_compress')

make_tarfile(params.output_folder + 'sample_compress.tar',params.output_folder)
