CREATE EXTERNAL TABLE videos
(rowkey STRING, 
location STRING, 
quantity INT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES 
('hbase.columns.mapping' = ':key, info:location, info:quantity')
TBLPROPERTIES ('hbase.table.name' = 'cameras');


CREATE EXTERNAL TABLE segments
(rowkey STRING, 
video_id STRING, 
url STRING, 
cover STRING,
time_start FLOAT, 
time_end FLOAT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES 
('hbase.columns.mapping' = ':key, video:video_id, video:url, video:cover, time:time_end, time:time_start')
TBLPROPERTIES ('hbase.table.name' = 'segments');

CREATE EXTERNAL TABLE trackings
(rowkey STRING, 
video_id STRING, 
segment_id STRING,
frame_id STRING,
name STRING, 
upper STRING, 
lower STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES 
('hbase.columns.mapping' = ':key, frame:video_id, frame:segment_id, frame:frame_id, object:name, object:upper, object:lower')
TBLPROPERTIES ('hbase.table.name' = 'trackings');

CREATE EXTERNAL TABLE frames
(rowkey STRING, 
video_id STRING, 
segment_id STRING, 
send_time FLOAT, 
content STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES 
('hbase.columns.mapping' = ':key, video:video_id, video:segment_id, frame:send_time, frame:content')
TBLPROPERTIES ('hbase.table.name' = 'frames');