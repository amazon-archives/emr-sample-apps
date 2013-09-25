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



add jar ${LIB}/jsonserde.jar ;

create external table if not exists impressions (
    requestBeginTime string, adId string, impressionId string, referrer string, 
    userAgent string, userCookie string, ip string
  )
  partitioned by (dt string)
  row format 
    serde 'com.amazon.elasticmapreduce.JsonSerde'
    with serdeproperties ( 'paths'='requestBeginTime, adId, impressionId, referrer, userAgent, userCookie, ip' )
  location '${INPUT}/impressions' ;

alter table impressions recover partitions ;

create external table if not exists clicks (
    impressionId string
  )
  partitioned by (dt string)
  row format 
    serde 'com.amazon.elasticmapreduce.JsonSerde'
    with serdeproperties ( 'paths'='impressionId' )
  location '${INPUT}/clicks' ;

alter table clicks recover partitions ;

create external table if not exists joined_impressions (
  requestBeginTime string, adId string, impressionId string, referrer string, 
    userAgent string, userCookie string, ip string, clicked boolean
  )
  partitioned by (day string, hour string)
  stored as sequencefile
  location '${OUTPUT}/joined_impressions'
;

create table if not exists tmp_impressions (
  requestBeginTime string, adId string, impressionId string, referrer string, 
  userAgent string, userCookie string, ip string
)
stored as sequencefile;

insert overwrite table tmp_impressions 
  select 
    from_unixtime(cast((cast(i.requestBeginTime as bigint) / 1000) as int)) requestBeginTime, 
    i.adId, i.impressionId, i.referrer, i.userAgent, i.userCookie, i.ip
  from 
    impressions i
  where 
    i.dt > '${DAY}-${HOUR}-00' and i.dt < '${NEXT_DAY}-${NEXT_HOUR}-00'
;

create table if not exists tmp_clicks (
  impressionId string
) stored as sequencefile;

insert overwrite table tmp_clicks select 
  impressionId
  from 
    clicks c  
  where 
    c.dt > '${DAY}-${HOUR}-00' and c.dt < '${NEXT_DAY}-${NEXT_HOUR}-20'
;

insert overwrite table joined_impressions partition (day='${DAY}', hour='${HOUR}')
  select 
    i.requestBeginTime, i.adId, i.impressionId, i.referrer, i.userAgent, i.userCookie, 
    i.ip, (c.impressionId is not null) clicked
  from 
    tmp_impressions i left outer join tmp_clicks c on i.impressionId = c.impressionId
;

select clicked, ip from joined_impressions where day = '${DAY}' and hour = '${HOUR}' limit 1 ;
select count(1), clicked from joined_impressions where day = '${DAY}' and hour = '${HOUR}' group by clicked ;



