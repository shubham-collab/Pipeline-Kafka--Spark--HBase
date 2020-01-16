# ==============================  RUN THIS SCRIPT USING FOLLOWING COMMAND:
# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 ~/WORK/kafkaSparkHbase/kafkaHbasetry.py localhost:9092 clicks

#IMPORTANT: START HBASE SERVER BEFORE THIS: hbase thrift start -p 9090

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row

from pyspark import SparkConf
from pyspark.streaming.kafka import  *;
#from pyspark_ext import *
import happybase
  
sc = SparkContext(appName="PythonStreamingRecieverKafkaWordCount")
ssc = StreamingContext(sc, 5)

sc.setLogLevel("ERROR")

# HBase related details
hbase_table = 'clicks'
hconn = happybase.Connection('localhost')  
ctable = hconn.table(hbase_table) 

broker, topic = sys.argv[1:],[]


kvs = KafkaUtils.createDirectStream(ssc, ['clicks'], {'bootstrap.servers': 'localhost:9092'})
print('chck start')


def SaveToHBase(rdd):
    print("=====Pull from Stream=====")
    rdd.collect()
    if not rdd.isEmpty():
        print("=some records=")
        for line in rdd.collect():
            print line.serial_id[1]
            ctable.put(('click' + line.serial_id[1]), { \
            b'clickinfo:studentid': (line.studentid[1]), \
            b'clickinfo:url': (line.url[1]), \
            b'clickinfo:time': (line.time[1]), \
            b'iteminfo:itemtype': (line.itemtype[1]), \
            b'iteminfo:quantity': (line.quantity[1])})


parsed = kvs.filter(lambda x: x != None and len(x) > 0 )
parsed = parsed.map(lambda x: x[1])
parsed = parsed.map(lambda rec: rec.split(","))
parsed = parsed.filter(lambda x: x != None and len(x) == 6 )
parsed = parsed.map(lambda data:Row(serial_id=(str,data[0]), \
		studentid=(str,data[1]), \
		url=(str,data[2]), \
		time=(str,data[3]), \
		itemtype=(str,data[4]), \
		quantity=(str,data[5])))



#parsed.pprint()
parsed.foreachRDD(SaveToHBase)



print ('chck end')






ssc.start()
ssc.awaitTermination()
