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

writer_cache = dict()
file_cache = dict()

reload(sys)
sys.setdefaultencoding('utf-8')

if sys.version_info < (2, 7):
    def __writeheader(self):
        header = dict(zip(self.fieldnames, self.fieldnames))
        self.writerow(header)
        csv.DictWriter.writeheader = __writeheader

def getwriter(header, fname_out, delimiter):
    global writer_cache
    global file_cache
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
    parser.add_argument('-i', '--input', dest='input_filename', default=None, required=True, help='Full path to input filename')
    parser.add_argument('-o', '--output', dest='output_filename', default=None, required=False, help='Full path to output filename')
    parser.add_argument('-r', '--rejected', dest='rejected_filename', default=None, required=False, help='Full path to rejected filename')
    parser.add_argument('-u', '--url', dest='url', default='http://invhdp01:8080/sax/v1/media/', required=False, help='REST API')
    parser.add_argument('-v', '--verbose', dest='verbose', default=False, action='store_true', required=False)
    parser.add_argument('-d', '--delimiter', dest='delimiter', default='\t', required=False)
    parser.add_argument('-b', '--bad', dest='bad', default=100, required=False, help='Number of bad rows allowed')
    parser.add_argument('-s', '--sample', dest='sample', default=1000, required=False, help='Size of sample for pre-validation')
    args = parser.parse_args()
    return args

def validation():
    fname = params.input_filename
    c = 0
    br = 0
    flag = True
    fname_out = None
    if not params.output_filename:
        fname_out = params.input_filename + '.rejected'
    else:
        fname_out = params.output_filename + '.rejected'
    with open(fname,'rb') as f:
        reader = csv.DictReader(f,delimiter=params.delimiter)
        for line in reader:
#            DEBUG
#            print(line)
            if (not line["urlip"]): # and (not line["ua"]):
                br = br+1
                writer = getwriter(reader.fieldnames,fname_out,params.delimiter)
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
    return flag

def main():
    fname = params.input_filename
    i = 1
    j = 1
    fname_out = None
    if not params.output_filename:
        fname_out = params.input_filename + '.out'
    else:
        fname_out = params.output_filename + '.out'
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
                    writer = getwriter(reader.fieldnames+sorted(parsed.keys()),fname_out,params.delimiter)
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
                    print(e)
                    if sys.version_info < (2, 7):
                        time.sleep(5) #Sleep for 5 seconds
                    else:
                        sleep(5) #Sleep for 5 seconds
                t=t-1
                if success:
                    t = -100 #Condition to exit from while
    print("Output file has been created.")
    print("Exiting script.")
    output_file.close()

params = argparser()
v = validation()
if v == True:
    print("Validation passed!")
else:
    print("Validation failed!")
    sys.exit("Exiting script.")

main()
for key, value in file_cache.iteritems():
    value.close()
