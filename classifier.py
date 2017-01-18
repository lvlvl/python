#!/usr/bin/python

import csv
import urllib
import urllib2
import json
import StringIO
from StringIO import StringIO
import argparse
import sys
import time
import multiprocessing
from os import listdir
from os.path import isfile, join

reload(sys)
sys.setdefaultencoding('utf-8')

if sys.version_info < (2, 7):
    def __writeheader(self):
        header = dict(zip(self.fieldnames, self.fieldnames))
        self.writerow(header)
        csv.DictWriter.writeheader = __writeheader

def getwriter(header, fname_out, delimiter, writer_cache, file_cache):
#    global writer_cache
#    global file_cache
    if not fname_out in writer_cache:
        output_file = open(fname_out,'w+')
        writer = csv.DictWriter(output_file,fieldnames=header,delimiter=delimiter)
        if sys.version_info < (2, 7):
            __writeheader(writer)
        else:
            writer.writeheader()
        writer_cache[fname_out]=writer
        file_cache[fname_out]=output_file
    return writer_cache[fname_out]

# Choose method
def getmethod(line):
    if 'api_method' in line:
        return line['api_method']
    return 'all'

def argparser():
    parser = argparse.ArgumentParser(description='Specify input and output filenames')
    parser.add_argument('-i', '--input', dest='input_folder', default=None, required=True, help='Full path to input folder to read from')
    parser.add_argument('-o', '--output', dest='output_folder', default=None, required=False, help='Full path to output folder to write to')
    parser.add_argument('-r', '--rejected', dest='rejected_filename', default=None, required=False, help='Full path to rejected filename')
    parser.add_argument('-u', '--url', dest='url', default='http://invhdp01:8080/sax/v1/media/', required=False, help='REST API')
    parser.add_argument('-v', '--verbose', dest='verbose', default=False, action='store_true', required=False)
    parser.add_argument('-d', '--delimiter', dest='delimiter', default='\t', required=False)
    parser.add_argument('-b', '--bad', dest='bad', default=100, required=False, help='Number of bad rows allowed')
    parser.add_argument('-s', '--sample', dest='sample', default=1000, required=False, help='Size of sample for pre-validation')
    parser.add_argument('-p','--numProcessors', required=False, type=int,
    					default=multiprocessing.cpu_count(),
    					help='Number of processors to use. ' + \
    					"Default for this machine is %d" % (multiprocessing.cpu_count(),) )
    args = parser.parse_args()
    return args

def validation(fname, fname_out):
    c = 0
    br = 0
    flag = True
    writer_cache = dict()
    file_cache = dict()
#    DEBUG
#    print(fname)
#    print(fname_out)
#    print(params.delimiter)
    with open(fname,'rb') as f:
        reader = csv.DictReader(f,delimiter=params.delimiter)
        for line in reader:
#            DEBUG
#            print(line)
            if (not line["urlip"]): # and (not line["ua"]):
                br = br+1
                writer = getwriter(reader.fieldnames,fname_out,params.delimiter,writer_cache,file_cache)
                writer.writerow(dict(line.items()))
            else:
                pass
            c = c+1
            if (br >= params.bad):
                print("File contains many errors.")
                flag = False
                break
            if c > params.sample:
                break
        print("Number of bad rows: "+str(br))
        print("Number of lines validated: "+str(c))
        print("")
        #Close all open files
        for key, value in file_cache.iteritems():
            value.close()
    return flag

def main(fname, fname_out):
    i = 1
    j = 1
    writer_cache = dict()
    file_cache = dict()
    print("Processing file %s" % fname)
    with open(fname,'rb') as f:
        reader = csv.DictReader(f,delimiter=params.delimiter)
        headers = reader.fieldnames
#        DEBUG
#        print(headers)
        for line in reader:
            request = params.url+getmethod(line)+'?'
            #If we don't have ua column, we need to pass empty ua to REST service
            #otherwise it will return incorrect response
            if not 'ua' in headers:
                request = request + 'ua=&'
            request = request  + urllib.urlencode(line)
            if params.verbose:
                print("Request "+str(i)+": "+request)
                i =i+1
            t = 9 # Number of retry attempts
            while t >= 0:
                success = False
                try:
                    response = urllib2.urlopen(request).read()
                    if params.verbose:
                        print("Response "+str(j)+": "+response)
                        j=j+1
                    io = StringIO(response)
                    parsed = json.load(io)

                    wr_output = 'output'
                    writer = getwriter(reader.fieldnames+sorted(parsed.keys()),fname_out,params.delimiter,writer_cache,file_cache)
#                    DEBUG
#                    print ('Headers: ' + str(reader.fieldnames+sorted(parsed.keys())))
#                    print('Line: ' + str(line))
#                    print('')
#                    print('Line item: ' + str(line.items()))
#                    print('')
#                    print('Parsed item: ' + str(parsed.items()))
                    writer.writerow(dict(line.items()+parsed.items()))
                    success = True
                except Exception as e:
                    print("Eroor message from REST API: " + str(e))
                    print("Request: " + request)
                    print("Response: " + response)
                    time.sleep(5) #Sleep for 5 seconds
                t=t-1
                if success:
                    t = -100 #Condition to exit from while
    print("Output file %s has been created." % fname_out)
#    print("Exiting script.")
#Close all open files
    for key, value in file_cache.iteritems():
        value.close()
    return fname_out

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


#Validation of files one by one from input directory
i=0
files_validated = []
while i < len(onlyfiles):
    if params.verbose == True:
        print('Validating file: ')
        print(onlyfiles[i])
        print('----------------')
    fname = None
    fname = params.input_folder + onlyfiles[i]
    fname_out = None
    fname_out = params.output_folder + onlyfiles[i] + '.rejected'

    v = validation(fname, fname_out)
    if v == True:
        print("Validation of file %s passed!" % (onlyfiles[i]))
        files_validated.append(onlyfiles[i])
#        DEBUG
#        print(files_validated)
    else:
        print("Validation of file %s failed!" % (onlyfiles[i]))
    i=i+1

print("Only next passed validation files will be processed:")
print(files_validated)

#Multiprocessing files
#Start pool
pool = multiprocessing.Pool(processes=params.numProcessors)

print("Number of processes choosen: " + str(params.numProcessors))

#Build task list
tasks = []
i = 0
while i < len(files_validated):
#DEBUG
#    print('Processing file: ')
#    print(files_validated[i])
#    print('----------------')
    fname = None
    fname = params.input_folder + files_validated[i]
    fname_out = None
    fname_out = params.output_folder + files_validated[i] + '.out'

    tasks.append( (fname, fname_out, ) )
    i=i+1

#DEBUG
#print("Tasks:")
#print(tasks)
#print("======")


#Run tasks
results = [pool.apply_async(main, (t[0],t[1])) for t in tasks]
#DEBUG
#print("Results:")
#print(results)
#print("======")
#Process results
#for result in results:
#    processed_fname = result.get()
#    print("Result: output file %s has been created" % (processed_fname) )

pool.close()
pool.join()


#Processing files one by one from input directory
#i=0
#while i < len(files_validated):
#    DEBUG
#    print('==========')
#    if params.verbose == False:
#        print('Processing file: ')
#        print(files_validated[i])
#        print('----------------')
#    fname = None
#    fname = params.input_folder + files_validated[i]
#    fname_out = None
#    fname_out = params.output_folder + files_validated[i] + '.out'
#    main(fname, fname_out)
#    i=i+1

#Close all open files
#for key, value in file_cache.iteritems():
#    value.close()
