'''
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
'''

#!/usr/bin/python
import sys 
import re 

def main(argv): 
    pattern = re.compile("[a-zA-Z][a-zA-Z0-9]*") 
    for line in sys.stdin: 
        for word in pattern.findall(line): 
            print "LongValueSum:" + word.lower() + "\t" + "1" 


if __name__ == "__main__": 
    main(sys.argv) 
