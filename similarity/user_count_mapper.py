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
user_count_mapper.py

Compute the count of values for each user (ratings, playcounts, etc). 

Run from the Amazon Elastic MapReduce console with the following 
parameters:

-input elasticmapreduce/samples/similarity/lastfm/input 
-output <yourbucket>/lastfm/output/user-counts 
-mapper elasticmapreduce/samples/similarity/user_count_mapper.py 
-reducer aggregate


Created by Peter Skomoroch on 2009-03-30.
Copyright (c) 2009 Data Wrangling LLC. All rights reserved.
"""

import sys

for line in sys.stdin:
  (user, item, rating) = line.strip().split()
  print "LongValueSum:%s\t1" % user

