create external table cameras
using org.apache.hadoop.hbase.spark
options('hbase.table'='cameras',
'hbase.columns.mapping'='rowkey STRING :key, location STRING info:location, quantity STRING info:quantity',
'hbase.spark.use.hbasecontext'=false)
location '/tmp/video-processing/cameras';

create external table segments
using org.apache.hadoop.hbase.spark
options ('hbase.table'='segments',
'hbase.columns.mapping'='rowkey STRING :key, video_id STRING video:video_id, url STRING video:url, cover STRING video:cover, time_end STRING time:time_end, time_start STRING time:time_start', 
'hbase.spark.use.hbasecontext'=false)
location '/tmp/video-processing/segments';

create external table frames
using org.apache.hadoop.hbase.spark
options ('hbase.table'='frames',
'hbase.columns.mapping'='rowkey string :key, video_id string video:video_id, segment_id string video:segment_id, send_time string frame:send_time, content binary frame:content',
'hbase.spark.use.hbasecontext'=false)
location '/tmp/video-processing/fames';

create external table people
using org.apache.hadoop.hbase.spark
options('hbase.table'='people',
'hbase.columns.mapping'='rowkey string :key, video_id string frame:video_id, segment_id string frame:segment_id, frame_id string frame:frame_id, name string object:name, upper string object:upper, lower string object:lower',
'hbase.spark.use.hbasecontext'=false)
location '/tmp/video-processing/people';

create external table things
using org.apache.hadoop.hbase.spark
options('hbase.table'='things',
'hbase.columns.mapping'='rowkey string :key, video_id string frame:video_id, segment_id string frame:segment_id, frame_id string frame:frame_id, name string object:name, color string object:color',
'hbase.spark.use.hbasecontext'=false)
location '/tmp/video-processing/thing';