#Methods for creating Spark DataFrame:
# There are three ways to create a DataFrame in Spark by hand:
# 1. Create a list and parse it as a DataFrame using the toDataFrame() method from the SparkSession.
# 2. Convert an RDD to a DataFrame using the toDF() method.
# 3. Import a file into a SparkSession as a DataFrame directly.

#################METHOD 1#####################
# Create DataFrame from a dict
#  However, UserWarning: inferring schema from dict is deprecated,please use pyspark.sql.Row instead
# warnings.warn("inferring schema from dict is deprecated,"

data = [{"Category": 'A', "ID": 1, "Value": 121.44, "Truth": True},
        {"Category": 'B', "ID": 2, "Value": 300.01, "Truth": False},
        {"Category": 'C', "ID": 3, "Value": 10.99, "Truth": None},
        {"Category": 'E', "ID": 4, "Value": 33.87, "Truth": True}
       ]

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(data)
# type(df)

df.show()

#################METHOD 2#####################
# Create DataFrame from a list object
data1=[(123, 'Vivek', 'Pune'), (234, 'Sumit', 'Patna')]
Columns=["ID", "Name", "Address"]

df1=spark.createDataFrame(data,Columns)
df1.show()
df1.printSchema()

#################METHOD 3#####################
# Using RDD
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("DataFrame preparation").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)

rdd = sc.parallelize(data)
df2 = rdd.toDF()
df2.show()
print(type(rdd))
print(type(df))

print("Execution completed!!!")