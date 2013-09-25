/*
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
*/

add jar ${SAMPLE}/libs/jsonserde.jar ;

drop table logs ;
create external table logs (requestBeginTime string, requestEndTime string, hostname string)
  partitioned by (dt string)
  row format 
    serde 'com.amazon.elasticmapreduce.JsonSerde'
    with serdeproperties ( 'paths'='requestBeginTime, requestEndTime, hostname' )
  location '${SAMPLE}/tables/impressions' ;

alter table logs recover partitions ;
show partitions logs ;

drop table local_metrics ;
create table local_metrics (requestTime double, hostname string) ;

from logs 
  insert overwrite table local_metrics 
    select ((cast(requestEndTime as bigint)) - (cast(requestBeginTime as bigint)) / 1000.0) as requestTime, hostname
      where dt = '${DATE}'
;

from local_metrics
  select transform ( '${DATE}', max(requestTime), min(requestTime), avg(requestTime), 'ALL' )
    using '${SAMPLE}/libs/upload-to-simple-db hostmetrics metrics date,hostname date tmax tmin tavg hostname' 
    as (output string)
;  

from local_metrics
  select transform ( '${DATE}', max(requestTime), min(requestTime), avg(requestTime), hostname )
    using '${SAMPLE}/libs/upload-to-simple-db hostmetrics metrics date,hostname date tmax tmin tavg hostname' 
    as (output string)
  group by hostname
;

