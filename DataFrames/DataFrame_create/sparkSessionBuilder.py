from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

class SparkSessionBuilder(object):
    _instance=None

    sc = SparkContext("local", "SparkContextBuilder-Application")
    spark = SparkSession.builder.appName("SparkSessionBuilder-Application").getOrCreate()

    print("\n############################################################\n")
    print("Spark Version is : ", spark.version)
    print("Spark Application Name : ", spark.sparkContext.appName)
    print("Spark Application ID : ", spark.sparkContext.applicationId)
    # print("Spark Application Name : ", spark.conf)
    print("\n############################################################")

    def __new__(self):
        if not self._instance:
            self._instance=super(SparkSessionBuilder,self).__new__(self)

        return self._instance