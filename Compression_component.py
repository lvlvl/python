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

params = argparser()
#Set output_folder parameter if not defined
if params.output_folder == None:
    params.output_folder = params.input_folder

analyze_file(params.input_folder+'sample_10')