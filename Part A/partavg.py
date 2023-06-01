import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    def transaction_time(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[7])
            int(fields[11])
            return True
        except:
            return False

    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")  
    
    
    def mappingfunc(line):
        [_, _, _, _, _, _, _, value, _, _,_, block_timestamp,_,_,_] = line.split(',')
        date = time.strftime("%m/%Y",time.gmtime(int(block_timestamp)))
        vals = float(value)
        return (date, (vals, 1))
    
    t = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines = t.filter(transaction_time)
    date_vals = clean_lines.map(mappingfunc) 
    reducing = date_vals.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) 
    avg_transactions = reducing.map(lambda a: (a[0], str(a[1][0]/a[1][1]))) 
    avg_transactions = avg_transactions.map(lambda op: ','.join(str(tr) for tr in op))
 

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_avg' + date_time + '/avg_transaction.txt')
    
    my_result_object.put(Body=json.dumps(avg_transactions.take(100)))
    
    spark.stop()
    
    
    
    
    
    
    