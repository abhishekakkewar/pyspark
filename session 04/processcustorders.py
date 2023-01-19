import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
# import findspark
# findspark.init()


spark = SparkSession. \
        builder. \
        appName("custordersapp"). \
        master("local[2]"). \
        getOrCreate()

custorders = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(sys.argv[1])

custordersgrp = custorders.groupBy("city").agg(f.sum("orderamt").alias("totalbiz"), f.count("orderno").alias('NoOfOrders'))

custordersgrp.show()
