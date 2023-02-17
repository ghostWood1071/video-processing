CREATE EXTERNAL TABLE hourly_weather(rowkey STRING, area_code STRING, time STRING, temp INT, humi INT, event STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, context:area_code, context:time, weather_info:temp, weather_info:humi, weather_info:event')
TBLPROPERTIES ('hbase.table.name' = 'hourly_weather');

