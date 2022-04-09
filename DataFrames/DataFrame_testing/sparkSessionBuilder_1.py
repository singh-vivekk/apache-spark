from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

class SparkSessionBuilder():

    @staticmethod
    def __init__(self):
        sc=SparkContext("local","SparkContextBuilder-Application")
        spark=SparkSession.builder.appName("SparkSessionBuilder-Application").getOrCreate()

        self.sc, self.spark = sc, spark

        print("\n############################################################\n")
        print("Spark Version is : ", spark.version)
        print("Spark Application Name : ", spark.sparkContext.appName)
        print("Spark Application ID : ", spark.sparkContext.applicationId)
        # print("Spark Application Name : ", spark.conf)
        print("\n############################################################")


    @staticmethod
    def getSparkSession(self):

        print("from SparkSessionBuilder Class")
        print(self.sc)
        print(self.spark)
        return self.spark, self.sc
