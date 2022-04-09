from pyspark.sql.types import IntegerType
from SparkBasics.DataFrames.DataFrame_create.sparkSessionBuilder import SparkSessionBuilder

''' 
    We can't have two different sparkcontext/session in same application
    Hence, importing the same application ID 
'''

class CreateDF():

    def __init__(self):
        self.spark = SparkSessionBuilder().spark
        self.sc = SparkSessionBuilder().sc
        print("\nfrom CreateDF class...")
        print("Spark Session Object ID : ", self.spark)
        print("Spark Application ID : ", self.spark.sparkContext.applicationId)


    def createDictDF(self,object):
        dictData=[{"name":"Vivek", "age":30},{"name":"Aakash","age":28}]

        dictDF=self.spark.createDataFrame(dictData)
        print("\n ---- Creating DF from python Dict List ---- \n")
        dictDF.show()


    def createTupleDF(self, object):
        tupleData1 = [("Vivek", 30,"Pune"), ("Aakash", 28,"Mumbai")]
        tupleData2 = [("Rahul", 27,"Pune"), ("Aakash", 28,"Mumbai")]

        tupleDF1=self.spark.createDataFrame(tupleData1).toDF("name","age","city")
        tupleDF2=self.spark.createDataFrame(tupleData2).toDF("name","age","city")
        print("\n ---- Creating DF from python Tuple List ---- \n")
        # tupleDF1.show()
        return tupleDF1, tupleDF2


    def createRDDDF(self, rng):
        data = range(rng)
        # Create RDD
        rdd=self.sc.parallelize(data)

        rddDF=self.spark.createDataFrame(rdd,IntegerType()).toDF("intNumber")
        print("\n ---- Creating DF from RDD ---- \n")
        rddDF.show()


    def createDFwithSchema(self):
        from pyspark.sql.types import StructType, StructField
        from pyspark.sql.types import StringType, IntegerType, ArrayType
        data = [
            (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
            (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
            (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
            (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "CA", "M"),
            (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
            (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
        ]

        schema = StructType([
            StructField('name', StructType([
                StructField('firstname', StringType(), True),
                StructField('middlename', StringType(), True),
                StructField('lastname', StringType(), True)
            ])),
            StructField('languages', ArrayType(StringType()), True),
            StructField('state', StringType(), True),
            StructField('gender', StringType(), True)
        ])

        dfWithSchema = self.spark.createDataFrame(data=data, schema=schema)
        print("\n ---- Creating DF with Schema ---- \n")
        dfWithSchema.printSchema()
        dfWithSchema.show(truncate=False)
        return dfWithSchema


    def dfForLike(self):
        data2 = [(2, "Michael Rose1"), (3, "Robert Williams"),
                 (4, "Rames Rose"), (5, "Rames rose")
                 ]
        dfForLikeRlike = self.spark.createDataFrame(data=data2, schema=["id", "name"])
        print("\n ---- Creating DF for like and rlike ---- \n")
        dfForLikeRlike.show(truncate=False)
        return dfForLikeRlike


if __name__ == '__main__':
    objCreateDF = CreateDF()
    # objCreateDF.createDictDF(1)
    # objCreateDF.createDictDF(1)
    # objCreateDF.createTupleDF(1)
    # objCreateDF.createRDDDF(10)
    objCreateDF.createDFwithSchema()