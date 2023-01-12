from happybase import *
from typing import *
from pyspark.sql import DataFrame

host = 'master,slave01,slave02'  
table = 'test' 
keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"  
valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
conf = {
        "hbase.zookeeper.quorum": host,  
        "hbase.mapred.outputtable": table,  
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",  
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",  
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
    } 

class WriteHbaseRow:
    def __init__(self) -> None:
        self.conn:Connection = Connection(host='10.0.2.195', port=9090, autoconnect=False)
        self.partition_id = None
        self.epoch_id = None

    def open(self, partition_id, epoch_id):
        self.partition_id = partition_id
        self.epoch_id = epoch_id
        self.conn.open()

    def process(self, row):
        table:Table = self.conn.table('video-processing')
        data:Dict[bytes, bytes] = {
            b'video:video_id': row['video_id'],
            b'video:segment_id': row['segment_id'],
            b'video:frame_id': row['frame_id'],
            b'object:name': row['name']
        }
        table.put(f'{self.epoch_id}-{self.partition_id}-{row["key"]}', data)
        self.conn.close()

    def close(self, err):
        self.conn.close()
        print(err)

def process_batch(df:DataFrame, id: int):
    df.rdd.saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)
    


        

