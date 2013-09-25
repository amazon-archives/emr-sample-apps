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

ADD JAR ${SAMPLE}/libs/jsonserde.jar ;

CREATE EXTERNAL TABLE impressions (
    requestBeginTime string, adId string, impressionId string, referrer string, 
    userAgent string, userCookie string, ip string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT 
    serde 'com.amazon.elasticmapreduce.JsonSerde'
    with serdeproperties ( 'paths'='requestBeginTime, adId, impressionId, referrer, userAgent, userCookie, ip' )
  LOCATION '${SAMPLE}/tables/impressions' ;

ALTER TABLE impressions ADD PARTITION (dt='2009-04-13-08-05') ;

CREATE TABLE tmp_impressions (
    requestBeginTime string, adId string, impressionId string, referrer string, 
    userAgent string, userCookie string, ip string
  )
  STORED AS SEQUENCEFILE ;

INSERT OVERWRITE TABLE tmp_impressions 
    SELECT 
      from_unixtime(cast((cast(i.requestBeginTime as bigint) / 1000) as int)) requestBeginTime, 
      i.adId, i.impressionId, i.referrer, i.userAgent, i.userCookie, i.ip
    FROM 
      impressions i
    WHERE 
      i.dt >= '${DAY}-${HOUR}-00' and i.dt < '${NEXT_DAY}-${NEXT_HOUR}-00'
  ;

CREATE EXTERNAL TABLE output_impressions (
    requestBeginTime string, adId string, impressionId string, referrer string, 
      userAgent string, userCookie string, ip string
    )
    PARTITIONED BY (day string, hour string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '${OUTPUT}/impressions'
  ;

INSERT OVERWRITE TABLE output_impressions PARTITION (day='${DAY}', hour='${HOUR}')
  SELECT 
    i.requestBeginTime, i.adId, i.impressionId, i.referrer, i.userAgent, i.userCookie, 
    i.ip
  FROM 
    tmp_impressions i
  WHERE
    i.referrer = 'twitter.com' and i.userAgent like '%Mozilla%'
  ;