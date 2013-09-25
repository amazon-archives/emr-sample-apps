# Copyright 2011-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

#!/usr/bin/env python
# encoding: utf-8
"""
convert_netflix.py

place this file in the same directory as the Netflix 'training_data' folder
containing movie_titles.txt, then run:

$ python convert_netflix.py

To upload the resulting files to an S3 bucket, get s3cmd from:

http://s3tools.org/s3cmd

Then run:

$ s3cmd put --force netflix-data/ratings* s3://<yourbucket>/netflix-data/ 

convert_netflix.py reformats Netflix Prize data so that each line contains:

userid movieid rating

Output files are chunked for upload to S3 and placed
in a directory called 'netflix-data'.  This takes about 20 minutes
on a 2 Ghz laptop and the resulting files are 428MB compressed.

Original format:
userid, rating, date

$ head mv_0000001.txt 
1:
1488844,3,2005-09-06
822109,5,2005-05-13
885013,4,2005-10-19
30878,4,2005-12-26
823519,3,2004-05-03
893988,3,2005-11-17
124105,4,2004-08-05

convert_netflix.py converts these input files 
to a set of files where each line contains: [userid movieid rating]

$ head user_movie_rating_1.txt 
1488844 1 3
822109 1 5
885013 1 4
30878 1 4
823519 1 3
893988 1 3
124105 1 4
1248029 1 3
1842128 1 4
2238063 1 3

Created by Peter Skomoroch on 2009-03-09.
Copyright (c) 2009 Data Wrangling. All rights reserved.
"""

import sys
import os
import re

CHUNK_FILES = True

def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        pass

def main(args):
    outfile = open('reformatted_movie_titles.txt', 'w')
    movie_title_file = open('movie_titles.txt','r')
    movie_title_exp=re.compile("([\w]+),([\w]+),(.*)")
    movie_titles={}
    for line in movie_title_file:
      m = movie_title_exp.match(line.strip())
      outfile.write('%s\t%s\n' % (m.group(1), m.group(3)))
    outfile.close()
    movie_title_file.close()  
  
    in_dir= args[1] #'training_set'
    out_dir = args[2] #'netflix-data'
    filenames = [in_dir +'/' + file for file in os.listdir(in_dir)]
    rating_count = 0
    L = 0
    outfile_num = 0
    mkdir(out_dir)
    outfilename = out_dir+ '/' + 'ratings_'+ str(outfile_num) +'.txt'    
    output_file = open(outfilename, 'w') 
    for i, moviefile in enumerate(filenames): 
        # if i+1 in (10774, 175, 11064, 4472, 
        # 16265, 9628, 299, 16948, 9368, 8627, 10627): # for sample dataset
        if i % 100 == 0: print "processing movie %s " % (i+1)
        f = open(moviefile,'r')
        for j, line in enumerate(f.readlines()):
            if j == 0:
                movieid = line.split(':')[0]
            else:
                (userid, rating, date) = line.split(',')
                nextline = ' '.join([userid, movieid, rating+'\n'])
                L += len(nextline) # when this is 65536, we start a new file
                if L/1000 > 65536 and CHUNK_FILES:
                    output_file.close()
                    # os.system('gzip ' + outfilename)
                    outfile_num += 1
                    outfilename = out_dir+ '/' + \
                    'ratings_'+ str(outfile_num) +'.txt'
                    print "--- starting new file: %s" % outfilename 
                    output_file = open(outfilename, 'w')
                    L = len(nextline)
                output_file.write(nextline)
                rating_count += 1
        f.close()   
    output_file.close()
    # os.system('gzip ' + outfilename)      

if __name__ == '__main__':
    main(sys.argv)

