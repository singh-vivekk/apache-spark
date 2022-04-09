##############################
# To create a empty dataframe:
##############################
from pyspark import SparkContext
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.session import SparkSession

sc=SparkContext("local","myApp")
spark=SparkSession.builder.appName("mySparkApp").getOrCreate()

schema=StructType([StructField('Act_ID',StringType()),
				  StructField('MES',StringType()),
				  StructField('Data_attr', StringType()),
				  StructField('Data_attr1', StringType()),
				  StructField('Count_Sum',StringType())
				  ])

df1=spark.createDataFrame(sc.emptyRDD(), schema)

df1.show()
#
# rdd1=sc.emptyRDD()
# print(rdd1)
# print(type(rdd1))