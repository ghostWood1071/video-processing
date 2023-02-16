from kafka.consumer import KafkaConsumer
consumer = KafkaConsumer('video', bootstrap_servers=["192.168.100.124:9092", "192.168.100.125:9093"])
for m in consumer:
    print("received")