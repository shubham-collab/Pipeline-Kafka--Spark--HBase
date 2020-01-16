# Pipeline-Kafka--Spark--HBase
getting data from Kafka topic (clicks) to Spark (using PySpark) and then inserting the ingested data (with some business logic) into HBase table (clicks) using happybase module in python 


The python script uploaded is to be executed in Spark (pyspark shell).
Or else
Submit the uploaded python script with the correct artifact (depending on the compatible versions), e.g. mine worked with the following artifact:
org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0

please check yours depending on the compatility of versions.

Another thing to be remembered is to Boot Up the HBASE Server before submitting this spark job in your cluster, otherwise spark will not be able to 
connect with the HBase.

For any issue/help, kindly contact.

#PlayWithCode
