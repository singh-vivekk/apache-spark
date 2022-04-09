from SparkBasics.DataFrames.DataFrame_create.sparkSessionBuilder import SparkSessionBuilder
from SparkBasics.DataFrames.DataFrame_create.createDataFrame import CreateDF

class SelectDF():

    def __init__(self):
        # pass
        self.spark = SparkSessionBuilder().spark
        self.sc = SparkSessionBuilder().sc
        print("from CreateDF class...")
        print(self.sc)
        print(self.spark)
        print("Spark Application ID in CreateDF : ", self.spark)

    # sc=SparkContext("local","SparkContext-Application")
    # spark=SparkSession.builder.appName("SparkSession-Application").getOrCreate()

    # print("\n############################################################")
    # print("Spark Version is : ", spark.version)
    # print("Spark Application Name : ", spark.sparkContext.appName)
    # print("Spark Application Name : ", spark.conf)
    # print("\n############################################################")

    def selectFromDf(self):
        objCreateDF = CreateDF()
        df=objCreateDF.createTupleDF(1)
        print(" ------------ selectFromDF method ----------- ")
        # print("Spark Application Name : ", spark.sparkContext.appName)
        df.show()


objSelectDF = SelectDF()
objSelectDF.selectFromDf()
