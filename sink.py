

class WriteHbaseRow:
    def __init__(self):
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

catalog = json.dumps({
  "table":{"namespace":"default","name":"video-processing"},
  "rowkey":"key",
  "columns":{
  "key":{"cf":"rowkey","col":"key","type":"string"},
  "video_id":{"cf":"video","col":"video_id","type":"string"},
  "segment_id":{"cf":"video","col":"segment_id","type":"string"},
  "frame_id":{"cf":"video","col":"frame_id","type":"string"},
  "name":{"cf":"object","col":"name","type":"string"},
  "frame":{"cf":"object","col":"frame","type":"string"},
  }
})