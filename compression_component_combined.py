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
    parser.add_argument('-s', '--skip', dest='skip', default=1, required=False, help='Number of header rows to skip prior to sorting data')
    parser.add_argument('-a', '--analysis', dest='analysis', default=50000, required=False, help='Number of rows for cardinality calculation')
    parser.add_argument('-v', '--verbose', dest='verbose', default=False, action='store_true', required=False)
    parser.add_argument('-c', '--cardinality', dest='card_treshold', default=5, required=False, type=int, help='Cardinality treshold')
    parser.add_argument('--compress', dest='compress', required=False, action='store_true', help='Compress files')
    parser.add_argument('--decompress', dest='decompress', required=False, action='store_true', help='Decompress files')
    args = parser.parse_args()
    return args

def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

def untar_file(fullFilePath, temp_folder):
    tar = tarfile.open(fullFilePath)
    tar.extractall(path=temp_folder)
    tar.close()

def gZipFile(fullFilePath):
    check_call(['gzip', fullFilePath])

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

def analyze_file(fname):
    if params.verbose:
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
        sum_length = {}
        for i in headers:
            unique_col_values[i] = Set()
            sum_length[i] = 0
        for row in itertools.islice(reader, int(params.analysis)):
#            DEBUG
#            print(row)
            for fieldname in headers:
                value = row[fieldname]
                sum_length[fieldname] = sum_length[fieldname] + len(str(value))
                unique_col_values[fieldname].add(value)
        if params.verbose:
            print('Fieldnames in file:')
            print('-------------------')
            for fieldname in headers:
                print('Fieldname: <' + fieldname + '>  distinct count: ' + str(len(unique_col_values[fieldname]))+ ';  cardinality %: ' + str(100*len(unique_col_values[fieldname])/int(params.analysis)) + ';  AVG length of the field: ' + str(sum_length[fieldname]/int(params.analysis) ))
#                print('Fieldname: <' + fieldname + '>' )
#                print('    Distinct count: ' + str(len(unique_col_values[fieldname])) + ';')
#                print('    Cardinality %: ' + str(100*len(unique_col_values[fieldname])/int(params.analysis)) + ';')
#                print('    AVG length of the field: ' + str(sum_length[fieldname]/int(params.analysis)) + ';')
            print('----------------------------------')
            print('Fieldnames with cardinality < ' + str(params.card_treshold) + ' %:')
            print('----------------------------------')
        i = 0
        compressed_columns = list()
        for fieldname in headers:
            if 100*len(unique_col_values[fieldname])/int(params.analysis) < params.card_treshold:
                compressed_columns.append(fieldname)
                i = i+1
                if params.verbose:
                    print('Fieldname: <' + fieldname + '> distinct count: ' + str(len(unique_col_values[fieldname]))+ '; cardinality %: ' + str(100*len(unique_col_values[fieldname])/int(params.analysis)) )
#        DEBUG
#        print('Compressed columns: ')
#        print(compressed_columns)
        return compressed_columns

def make_header(fname):
    input_fname = params.input_folder + fname
    with open(input_fname,'rb') as f:
        #Create file containing header information
        output_headers = gzip.open(params.temp_folder + fname + '.header.gz','w+')
        #Number of lines in header from comand line parameters
        h = int(params.skip)
#       DEBUG
#        print(h)
        output_headers.writelines([f.readline() for x in xrange(h+1)])
        output_headers.close()

def transform_file(fname):
    input_fname = params.input_folder + fname
    with open(input_fname,'rU') as f:
        #Skip headers from original file
        h = int(params.skip)
        [f.readline() for x in xrange(h)]

        reader = csv.DictReader(f,delimiter=params.delimiter)
        headers = reader.fieldnames
        output_file = gzip.open(params.temp_folder + fname + '.data.gz','w+')
        writer = csv.writer(output_file,delimiter=params.delimiter)

        dictionaries = {}
        counters = {}
        for column in compressed_columns:
            dictionaries[column]={}
            counters[column]=0


        try:
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
            if params.verbose:
                print('---------------------')
                print('Dictionaries created: ')
                print('---------------------')
            for header in compressed_columns:
                dict_name = params.temp_folder + fname + '.map.'+header
                if params.verbose:
                    print(dict_name)
                with open(dict_name, 'wb') as handle:
                    d = dictionaries[header]
                    d = dict((v,k) for k,v in d.iteritems())
                    pickle.dump(d, handle, protocol=pickle.HIGHEST_PROTOCOL)
    #                pickle.dump(dictionaries[header], handle, protocol=pickle.HIGHEST_PROTOCOL)
                    handle.close()
                    gZipFile(dict_name)
        except Exception as e:
            print('Error: ')
            print(e)
            print('Line caused the error: ')
            print(line)

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
        with open(header,'rU') as f:
            for line in f:
    #                print(line)
                fieldnames_str = line
                fo.write(line)
        with open(data_file,'rU') as f:
            fieldnames = fieldnames_str[:-1].split(params.delimiter)
            reader = csv.DictReader(f,delimiter=params.delimiter,fieldnames=fieldnames)
            writer = csv.writer(fo,delimiter=params.delimiter)
            try:
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
            except Exception as e:
                print('Error: ')
                print(e)
                print('Line caused the error: ')
                print(line)
    return(output_fname)

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

if params.compress:
    i=0
    #Process files one by one from input directory
    while i < len(onlyfiles):
    #    DEBUG
    #    print('==========')
        if params.verbose == False:
            print('Processing file: ')
            print(onlyfiles[i])
            print('----------------')
        compressed_columns = analyze_file(params.input_folder+onlyfiles[i])
        make_header(onlyfiles[i])
        transform_file(onlyfiles[i])
        make_tarfile(params.output_folder + onlyfiles[i] + '.tar',params.temp_folder)
        delete_temp_files(params.temp_folder)
        i=i+1

    print('Processed files: ' + str(i))
    print('DONE.')
else:
    if params.decompress:
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
        #    DEBUG
        #    if i == 1:
        #        break
            i = i+1
        print("Done.")
    else:
        print("Method is not specified.")
        print('Exiting script.')
