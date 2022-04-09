'''
    Basic RDD operations

    To execute the code using Spark submit, place the file at /home/cloudera/SparkRDD.py
    Execute it on cluster with following command - spark-submit --master yarn  SparkRDD.py
    C:\spark3>spark-submit --master local[*] examples/src/main/python/pi.py 10
'''

# ----------------------------------------count.py---------------------------------------
from pyspark import SparkContext
import SparkModuleCall

def testMethod(spark):
    #sc = SparkContext("local", "count app")
    df1 = spark.range(10)
    print("^^^^^^^^^^^^^^^^^^^")
    df1.show()
    counts = df1.count()
    print ("Number of elements in RDD -> %i" % (counts))
    SparkModuleCall.printName("Test")

# ----------------------------------------count.py---------------------------------------