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
similarity.py

Example Hadoop streaming Python code to generate an item to item similarity
matrix and create related item recommendations using a MapReduce approach.  
Can be applied to explicit or implicit user rating data, or other datasets 
such as document term counts.  

The final MapReduce step finds top related items for each item in the input
data and writes to a tab delimited file that can be loaded into MySQL.

The similarity matrix produced in the third MapReduce step can be loaded 
into another process to generate item recommendations tailored to specific
users.  

For more on pairwise calculationss with MapReduce, see:

Elsayed T, Lin J and Oard D: Pairwise Document Similarity in Large 
Collections with MapReduce. Proceedings of the 46th Annual Meeting of 
the Association for Computational Linguistics(ACL2008), Companion Volume,
Columbus, Ohio 2008, 265268.

http://web.jhu.edu/HLTCOE/Publications/acl08_elsayed_pairwise_sim.pdf 

For a similar example using Dumbo, see:
http://dumbotics.com/2009/02/24/recommending-jira-issues/

This code is intended as a demonstration of MapReduce streaming concepts for
use with Amazon Elastic MapReduce and Hadoop 18.3. For high performance 
applications you will probably want to convert to Java or C++ and take full
advantage of Hadoop features like sequencefiles and combiners.

Input data should have the form:

User Item Rating
2 1 5
3 1 4
2 2 1
3 2 2
4 2 1

A corresponding file with item ids and names is also used as input for step4:

Item Name
1 candy
2 tofu

Run the following to test locally on a small 7 artist Audioscrobbler dataset:

$ python similarity.py

The sample input file provided with the code contains user "playcounts"
for the following artists found in the Audioscrobbler dataset:

id  artistname  total_plays
1001820 2Pac    12950
700     De La Soul      7243
1011819 A Tribe Called Quest  7601
1003557 Garth Brooks  2976
1000143 Toby Keith  1905
1012511 Kenny Rogers  1881
1000418 Mark Chesnutt   184

Similarity scores generated for the 7 artist sample in 
output/related_movies.tsv:

x	y	sim	count	name
1000143	1000143	1	0	Toby Keith
1000143	1003557	0.2434	809	Garth Brooks
1000143	1000418	0.1068	120	Mark Chesnutt
1000143	1012511	0.0758	237	Kenny Rogers
1000143	700	0.0479	114	De La Soul
1000143	1001820	0.0339	585	2Pac
1000143	1011819	0.0028	169	A Tribe Called Quest

700	700	1	0	De La Soul
700	1011819	0.2972	3050	A Tribe Called Quest
700	1001820	0.1012	2250	2Pac
700	1000143	0.0479	114	Toby Keith
700	1012511	0.0426	270	Kenny Rogers
700	1003557	0.0197	212	Garth Brooks



Created by Peter Skomoroch on 2009-03-27.
http://www.datawrangling.com
Copyright (c) 2009 Data Wrangling LLC. All rights reserved.
"""

import sys, os
import re
import csv
from math import sqrt, log, exp
from random import random
from collections import defaultdict

EPS = 20 # shrink similarity for items with a small corating base by N/(N+eps)
CONF_LEVEL = 1.96 # this corresponds to a 95% confidence limit

def fisher_z(x):  
  # The Fisher z' transform
  z = .5*( log((1+x)/(1-x)) )
  return z

def inverse_fisher_z(x):  
  # The Fisher z' inverse transform
  zi = (exp(2*x)-1)/(exp(2*x)+1)
  return zi
  
def fisher_sigma(n):  
  # The standard deviation in Fisher's space:
  sigma = 1/(sqrt(n-3)) 
  return sigma
     
def pearsonr(sum_xx, sum_x, sum_xy, sum_y, sum_yy, n): 
  # All inuts are integers 
  r_num = n * sum_xy - sum_x*sum_y
  r_denom = sqrt((n *sum_xx - sum_x**2)*(n *sum_yy - sum_y**2))
  if r_denom == 0.0:
    return 0.0
  else: 
    return round(r_num / float(r_denom), 4)  

def calc_similarity(sum_xx, sum_x, sum_xy, sum_y, sum_yy, n, N): 
    pearson = pearsonr(sum_xx, sum_x, sum_xy, sum_y, sum_yy, n)
    # Edge cases: avoid divide by zero errors for r = 1 or  n < 4
    if n < 4:
      return 0
    if pearson > .9999:
      pearson = .9999
    if pearson < -.9999:
      pearson = -.9999  
    fisher_lower = inverse_fisher_z( fisher_z(pearson) - fisher_sigma(n)*CONF_LEVEL)
    fisher_upper = inverse_fisher_z( fisher_z(pearson) + fisher_sigma(n)*CONF_LEVEL)
    # if the sign changed, set to 0
    if pearson >= 0 and fisher_lower < 0:
      fisher_lower = 0
    if pearson <= 0 and fisher_lower > 0:
      fisher_lower = 0
    # Alternatively:  
    if fisher_lower*fisher_upper < 0:
      fisher_lower = min(abs(fisher_lower),abs(fisher_upper))
    else:
      fisher_lower = min(fisher_lower,fisher_upper)        
    # Divide all resulting similarities by contant log(N=total_users) to keep
    # them in range [0,1]. You can add a (1-alpha) factor (possibly passed as
    # a parameter) to dial popularity weight effect up and down and get more
    # or less recs for popular items.    
    return fisher_lower * (n/(float(n + EPS )))**2 * (log(n)/log(N))


# Step 1: Count number of ratings for each item
def mapper1(args):
  for line in sys.stdin:
    user, item, rating = line.strip().split()
    sys.stdout.write('LongValueSum:%s\t1\n' % item)

def reducer1(args):
  '''
  Only needed for command line testing, use "aggregate" with Hadoop
  '''
  last_item, item_count = None, 0
  for line in sys.stdin:
    item = line.strip().split('\t')[0].split(':')[1]
    if last_item != item and last_item is not None:
      print '%s\t%s' % (last_item, item_count)    
      last_item, item_count = None, 0  
    last_item = item
    item_count += 1       
  print '%s\t%s' % (last_item, item_count)

# Step 2: Generate sorted item "postings" with KeyFieldBasedPartitioner      
def mapper2(scale, args):
  '''    
  If scale = 'log' we normalize the data to log scale
  Otherwise we leave the data unchanged, useful for implicit rating data.
  
  Emits with "userid,itemid" as key:
  
  17,70  3
  35,21  1
  49,19  2
  49,21  1
  49,70  4
  87,19  1
  87,21  2
  98,19  2
  
  '''
  for line in sys.stdin:
    user, item, rating = line.strip().split()    
    if scale=='log':
      rating = int(log(int(rating))) + 1
    sys.stdout.write('%s,%s\t%s\n' % (user,item,rating))
        
def reducer2(args):
  '''
  For each user, emit a row containing their "postings" (item,rating pairs)
  Also emit user rating sum and count for use later steps:
  
  17	1|3|70,3
  35	1|1|21,1
  49	3|7|19,2 21,1 70,4
  87	2|3|19,1 21,2
  98	1|2|19,2

  '''
  last_user, item_ratings = None, []
  user_count, user_sum = 0, 0
  for line in sys.stdin:
    (user_item, rating) = line.strip().split("\t")
    user, item = user_item.split(',')
    if last_user != user and last_user is not None:
      rating_string = ' '.join(item_ratings)
      print "%s\t%s|%s|%s" % (last_user, user_count, user_sum, rating_string)    
      (user_count, user_sum, item_ratings) = (0, 0, [])     
    last_user = user
    item_ratings.append('%s,%s' % (item,rating))
    user_count += 1
    user_sum += int(rating)
  rating_string = ' '.join(item_ratings)        
  print "%s\t%s|%s|%s" % (last_user, user_count, user_sum, rating_string)   


# Step 3: Item Similarity using Random Sampling & Distributed Cache
def P(item, user_count, item_counts, cutoff):
  '''
  P is an item dependent sample probability that ensures the
  number of coratings for a given item and user >= cutoff value
  '''
  item_count = float(item_counts[item])
  if item_count > 0:
    return max(cutoff/float(user_count), cutoff/item_count)
  return cutoff/float(user_count)
   
def mapper3(cutoff, itemcountfile, args):
  '''
  Similar to the concept of stop words in search, we use "stop users" to 
  help scale the mapreduce similarity computations
  
  Around 50 percent of users in the Netflix data have less than 100 ratings,
  90 percent have less than 500 ratings,99 percent have less than 1390 ratings.
  If we exclude the top 1 percent of most frequent raters, that represents
  4801 "stopusers" since netflix has 480189 users.
  
  Instead of excluding the prolific raters entirely, we downsample their 
  ratings to the "cutoff" rating count in a way that ensures infrequently 
  rated items are still included in the sample.
  
  Here cutoff = 100 and we supply the itemcount file generated in step1:
  
  ./similarity.py mapper3 100 output/artist_listen_counts.txt
  
  The output drops the user from the key entirely, instead it emits
  the pair of items as the key:
  
  19 21  2 1
  19 70  2 4
  21 70  1 4
  19 21  1 2
    
  This mapper is the main performance bottleneck.  One improvement
  would be to create a java Combiner to aggregate the
  outputs by key before writing to hdfs, another would be to use
  a vector format and SequenceFiles instead of streaming text
  for the matrix data    
    
  '''
  cutoff = int(cutoff) 
  reader = csv.reader(open(itemcountfile, 'rb'), delimiter='\t')
  item_counts = defaultdict(int)
  for item, count in reader:
    item_counts[item]=int(count)
  for line in sys.stdin:
    (user, vals) = line.strip().split("\t")
    (user_count, user_sum, item_ratings) = vals.split('|')
    items = [x.split(',') for x in item_ratings.split() if \
    random() < P(x.split(',')[0], user_count, item_counts, cutoff)]
    for i, y in enumerate(items):
      for x in items[:i]:
        sys.stdout.write("%s %s\t%s %s\n" % (x[0], y[0], x[1], y[1])) 
     
  
def reducer3(N, args):  
  '''
  Sum components of each corating pair across all users who rated both
  item x and item y, then calculate pairwise pearson similarity and corating
  counts.  The similarities are inverted because we do alphanumeric sort.
  This will be easier in Hadoop 19, where numerical sort order is possible
  in streaming jobs.
  
  19 0.4 21 2
  21 0.4 19 2
  19 0.6 70 1
  70 0.6 19 1
  21 0.1 70 1
  70 0.1 21 1  
  
  '''
  N = int(N) 
  (last_item_pair, similarity) = (None, 0)
  (sum_xx, sum_xy, sum_yy, sum_x, sum_y, n) = (0, 0, 0, 0, 0, 0)
  for line in sys.stdin:
    (item_pair, coratings) = line.strip().split("\t")
    (x, y) = map(int,coratings.split()) 
    if last_item_pair != item_pair and last_item_pair is not None:
      similarity = calc_similarity(sum_xx, sum_x, sum_xy, sum_y, sum_yy, n, N)
      print "%s %s %s %s" % (item_x, similarity, item_y, n)
      print "%s %s %s %s" % (item_y, similarity, item_x, n)      
      (sum_xx, sum_x, sum_xy, sum_y, sum_yy, n) = (0, 0, 0, 0, 0, 0)
    last_item_pair = item_pair
    item_x, item_y = last_item_pair.split()
    sum_xx += x*x
    sum_xy += x*y
    sum_yy += y*y
    sum_y += y
    sum_x += x
    n += 1
  similarity = calc_similarity(sum_xx, sum_x, sum_xy, sum_y, sum_yy, n, N)
  print "%s %s %s %s" % (item_x, similarity, item_y, n)
  print "%s %s %s %s" % (item_y, similarity, item_x, n)

# Step 4 
def mapper4(min_coratings, args):
  '''
  Emit items with similarity in key for ranking:
  
  19,0.4	70 1
  19,0.6	21 2
  21,0.6	19 2
  21,0.9	70 1
  70,0.4	19 1
  70,0.9	21 1
  
  '''
  for line in sys.stdin:
    item_x, similarity, item_y, n = line.strip().split()
    if int(n) >= int(min_coratings):
      sys.stdout.write('%s,%s\t%s %s\n' % (item_x, 
      str(1.0-float(similarity)), item_y, n))   
      
def reducer4(K, item_name_file, args):
  '''
  Read in item_name_file from cache and for each item, 
  emit K closest items in tab separated file:
  
  19	19	1	0	De La Soul
  19	70	0.6	1	A Tribe Called Quest
  19	21	0.4	2	2Pac
  
  ''' 
  name_reader = csv.reader(open(item_name_file, 'rb'), delimiter='\t')
  item_names = {}
  for itemid, name in name_reader:
    item_names[itemid] = name
  last_item, item_count = None, 0
  for line in sys.stdin:
    #'%s,%s\t%s\n'
    item_sim, vals = line.strip().split('\t')
    item_y, n = vals.split()
    item, similarity = item_sim.split(',')
    if last_item != item and last_item is not None:   
      last_item, item_count = None, 0
    last_item = item
    item_count += 1
    if item_count == 1:
      # first record, emit parent row
      print "\t".join([item, item, '1', '0', item_names[item]])
    if item_count <= int(K):
      print "\t".join([item, item_y, 
      str(round(1.0 - float(similarity),4)), n, item_names[item_y]])
      

if __name__ == "__main__":
  if len(sys.argv) == 1:
    print "Running sample Lastfm data for 7 artists..."
    # Step 1
    os.system('cat input/sample_user_artist_data.txt | '+ \
    ' ./similarity.py mapper1 | sort |' + \
    ' ./similarity.py reducer1 > artist_playcounts.txt')
    # Steps 2 & 3, 
    # pass in rating_count threshold = 100 to mapper3
    # pass in total_number_of_users =  148111 to reducer3    
    os.system('time cat input/sample_user_artist_data.txt |'+ \
    ' ./similarity.py mapper2 log | sort |' + \
    ' ./similarity.py reducer2 ' + \
    ' | ./similarity.py mapper3 100 artist_playcounts.txt ' + \
    '|sort |./similarity.py reducer3 147160 > artist_similarities.txt')
    # Step3, pass min_coratings ~ threshold/5 to mapper. 
    # pass K = 3 similar items to reducer...
    os.system('cat artist_similarities.txt  |'+ \
    ' ./similarity.py mapper4 20 | sort |' + \
    ' ./similarity.py reducer4 3 artist_data.txt' + \
    ' > related_artists.tsv')
    print "\nTop 25 related artists:"
    print "x\ty\tsim\tcount\tname"
    os.system('cat related_artists.tsv')    
        
  elif sys.argv[1] == "mapper1":
    mapper1(sys.argv[2:])
  elif sys.argv[1] == "reducer1":
    reducer1(sys.argv[2:])    
  elif sys.argv[1] == "mapper2":
    mapper2(sys.argv[2], sys.argv[3:])
  elif sys.argv[1] == "reducer2":
    reducer2(sys.argv[2:])
  elif sys.argv[1] == "mapper3":
    mapper3(sys.argv[2], sys.argv[3], sys.argv[4:])
  elif sys.argv[1] == "reducer3":
    reducer3(sys.argv[2], sys.argv[3:])
  elif sys.argv[1] == "mapper4":
    mapper4(sys.argv[2], sys.argv[3:])
  elif sys.argv[1] == "reducer4":
    reducer4(sys.argv[2], sys.argv[3], sys.argv[4:])    
  

        
