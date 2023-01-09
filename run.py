import os
# import json
# f = open('file.import.json')
# config = json.load(f)
# f.close()
# job_name = config['job']
# py_file_names = ','.join(config['py_files'])
# file_names = ','.join(config['files'])
os.system(
"sudo spark-submit " +
"--packages " + 
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0, " +
        "com.hortonworks:shc-core:1.1.0.2.6.5.360-2 "+ 
"videojob.py " + 
"--files /yolov5s.pt, hbase-site.xml " 
"--py-files /models.zip, /utils.zip")
        #   + job_name +
        #   " --py-files "+py_file_names+" "
        #   "--files "+file_names)