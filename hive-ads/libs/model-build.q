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

CREATE EXTERNAL TABLE IF NOT EXISTS raw_logs (
 request_begin_time STRING,
 ad_id STRING,
 impression_id STRING, 
 page STRING,
 user_agent STRING,
 user_cookie STRING,
 ip_address STRING,
 clicked BOOLEAN )
PARTITIONED BY (
 day STRING,
 hour STRING )
STORED AS SEQUENCEFILE
LOCATION '${INPUT}/joined_impressions/';


ALTER TABLE raw_logs RECOVER PARTITIONS;


CREATE EXTERNAL TABLE IF NOT EXISTS feature_index (
 feature STRING,
 ad_id STRING,
 clicked_percent DOUBLE )
STORED AS SEQUENCEFILE
LOCATION '${OUTPUT}/feature_index/';


INSERT OVERWRITE TABLE feature_index
SELECT
 temp.feature,
 temp.ad_id,
 sum(if(temp.clicked = 'true', 1, 0)) / cast(count(1) as DOUBLE) as clicked_percent
FROM (
 SELECT concat('ua:', trim(lower(ua.feature))) as feature, ua.ad_id, ua.clicked
 FROM (
  MAP raw_logs.user_agent, raw_logs.ad_id, raw_logs.clicked
  USING '${LIBS}/split_user_agent.py' as feature, ad_id, clicked
  FROM raw_logs
 ) ua

 UNION ALL

 SELECT concat('ip:', regexp_extract(ip_address, '^([0-9]{1,3}\.[0-9]{1,3}).*', 1)) as feature, ad_id, cast(clicked as STRING) as clicked
 FROM raw_logs

 UNION ALL

 SELECT concat('page:', lower(page)) as feature, ad_id, cast(clicked as STRING) as clicked
 FROM raw_logs
) temp
GROUP BY temp.feature, temp.ad_id;