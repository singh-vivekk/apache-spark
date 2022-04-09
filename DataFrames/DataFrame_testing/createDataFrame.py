# from pyspark.context import SparkContext
# from pyspark.sql.session import SparkSession
from pyspark.sql.types import IntegerType

from SparkBasics.DataFrames.DataFrame_testing.sparkSessionBuilder import SparkSessionBuilder

''' 
    We can't have two different sparkcontext/session in same application
    Hence, importing the same application ID 
'''

class CreateDF():

    def __init__(self):
        self.spark, self.sc = SparkSessionBuilder(self).getSparkSession(self)
        print("from CreateDF class...")
        print(self.sc)
        print(self.spark)
        print("Spark Application ID in CreateDF : ", self.spark)

    # sc=SparkContext("local","SparkContext-Application : Select")
    # spark=SparkSession.builder.appName("SparkSession-Application").getOrCreate()

    # print("\n############################################################")
    # print("Spark Version is : ", spark.version)
    # print("Spark Application Name : ", spark.sparkContext.appName)
    # print("Spark Application Name : ", spark.conf)
    # print("\n############################################################")

    def createDictDF(self,object):
        dictData=[{"name":"Vivek", "age":30},{"name":"Aakash","age":28}]

        dictDF=self.spark.createDataFrame(dictData)
        print("\n ---- Creating DF from python Dict List ---- \n")
        dictDF.show()


    def createTupleDF(self, object):
        tupleData = [("Vivek", 30,"Pune"), ("Aakash", 28,"Mumbai")]

        tupleDF=self.spark.createDataFrame(tupleData).toDF("name","age","city")
        print("\n ---- Creating DF from python Tuple List ---- \n")
        tupleDF.show()
        return tupleDF


    def createRDDDF(self, rng):
        data = range(rng)
        # Create RDD
        rdd=self.sc.parallelize(data)

        rddDF=self.spark.createDataFrame(rdd,IntegerType()).toDF("intNumber")
        print("\n ---- Creating DF from RDD ---- \n")
        rddDF.show()



if __name__ == '__main__':
    objCreateDF = CreateDF()
    objCreateDF.createDictDF(1)
    objCreateDF.createTupleDF(1)
    objCreateDF.createRDDDF(5)