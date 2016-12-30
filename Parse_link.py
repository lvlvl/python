#!/usr/bin/python

import csv
import urllib
import urllib2
import json
import StringIO
from StringIO import StringIO
import argparse
import sys

output_file = None
rejected_file = None
writer = None
writer_rejected = None

reload(sys)
sys.setdefaultencoding('utf-8')

def getwriter(header):
    global output_file
    global writer
    if writer == None:
        if not params.output_filename:
            fname_out = params.input_filename+'.out'
        else:
            fname_out = params.output_filename
        output_file = open(fname_out,'w+')
        writer = csv.DictWriter(output_file,fieldnames=header,delimiter=params.delimeter)
        writer.writeheader()
    return writer

def getwriter_rejected(header):
    global rejected_file
    global writer_rejected
    if writer_rejected == None:
        if not params.rejected_filename:
            fname_out = params.input_filename+'.rejected'
        else:
            fname_out = params.rejected_filename
        rejected_file = open(fname_out,'w+')
        writer_rejected = csv.DictWriter(rejected_file,fieldnames=header,delimiter=params.delimeter)
        writer_rejected.writeheader()
        writer_rejected.close()
    return writer_rejected

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
    parser.add_argument('-d', '--delimeter', dest='delimeter', default='\t', required=False)
    parser.add_argument('-b', '--bad', dest='bad', default=100, required=False, help='Number of bad rows allowed')
    parser.add_argument('-s', '--sample', dest='sample', default=1000, required=False, help='Size of sample for prevalidation')
    args = parser.parse_args()
    return args

def validation():
    fname = params.input_filename
    c = 0
    br = 0
    flag = True
    with open(fname,'rb') as f:
        reader = csv.DictReader(f,delimiter=params.delimeter)
        for line in reader:
#            DEBUG
#            print(line)
            if (not line["urlip"]): # and (not line["ua"]):
                br = br+1
                writer_rejected = getwriter_rejected(reader.fieldnames)
                writer_rejected.writerow(dict(line.items()))
            else:
                pass
            c = c+1
            if (br >= params.bad) or (c > params.sample):
                print("File contains many errors.")
                flag = False
                break
        print("Number of bad rows: "+str(br))
        print("Number of lines validated: "+str(c))
        print("")
    return flag

def main():
    fname = params.input_filename
    i = 1
    j = 1
    with open(fname,'rb') as f:
        reader = csv.DictReader(f,delimiter=params.delimeter)
        headers = reader.fieldnames
#        DEBUG
#        print(headers)
        for line in reader:
            request = params.url+getmethod(line)+'?'
            #If we don't have ua column
            if not 'ua' in headers:
                request = request + 'ua=&'
            request = request  + urllib.urlencode(line)
            if params.verbose:
                print("Request "+str(i)+": "+request)
                i =i+1
            response = urllib2.urlopen(request).read()
            if params.verbose:
                print("Response "+str(j)+": "+response)
                j=j+1
            io = StringIO(response)
            parsed = json.load(io)
            writer = getwriter(reader.fieldnames+sorted(parsed.keys()))
            writer.writerow(dict(line.items()+parsed.items()))

    output_file.close()

params = argparser()
v = validation()
if v == True:
    print("Validation passed!")
else:
    print("Validation failed!")
    sys.exit("Exiting script!")

main()
