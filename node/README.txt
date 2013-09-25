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

The sample-mapper and sample-reducer output the tweet count by day for complex JSON-structured data, in this case Twitter data collected using DataSift (datasift.com). It also escapes special characters such as newlines and tabs, and outputs the tweet created_at field as the key. The reducer then rolls up this data by date, outputting the total number of tweets.

These files are part of an AWS article demonstrating the use of Node.js with Amazon EMR: http://aws.amazon.com/articles/Elastic-MapReduce/4684067773833949

The sample input data is located at s3://elastic-mapreduce/samples/node/data. The script to install Node.js on EMR is located in AmazonEMR's Bootstrap Actions repository.  To run the application, save the files in your Amazon S3 bucket and use the Elastic MapReduce command line client as follows:

    elastic-mapreduce --create --stream /
        --name NodeJSMapReduce /
        --input s3n://elastic-mapreduce/samples/node/data/*.json /
        --output s3n://<my-bucket>/node/output /
        --mapper s3n://elastic-mapreduce/samples/node/sample-mapper.js /
        --reducer s3n://elastic-mapreduce/samples/node/sample-reducer.js /
        --bootstrap-action s3n://elasticmapreduce/samples/node/install-node-bin-x86.sh /
        --instance-type m1.large --instance-count 3


