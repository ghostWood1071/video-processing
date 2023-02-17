CREATE EXTERNAL TABLE video_processing
(rowkey STRING, 
video_id STRING, 
segment_id STRING, 
send_time STRING, 
frame_id STRING, 
frame STRING,
name STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES 
('hbase.columns.mapping' = ':key, video:video_id, video:segment_id, video:send_time, video:frame_id, video:frame, object:name')
TBLPROPERTIES ('hbase.table.name' = 'video-processing');

