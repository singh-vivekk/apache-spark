from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark import SparkContext
import SparkModuleCall

# def print_hi(name):
my_list = [1,2,3]

sc = SparkContext("local", "Vivek Spark app")
spark = SparkSession.builder.appName("my first spark app").getOrCreate()

print("Spark Application Name : ", spark.sparkContext.appName)
print("Spark Application ID : ", spark.sparkContext.applicationId)

print("\n\n ------------------------ \n\n")

df1=spark.read.option('header',True).csv('data.txt')
df1.show()

df2=spark.read.format('csv').option('header',True).load('data.txt')
df2.show()

