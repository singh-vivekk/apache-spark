from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, spark_partition_id
from pyspark.sql.types import StringType,IntegerType,LongType,StructType,StructField
# from pyspark.sql.types import StringType


def testSpark():
    print("Spark setup")
    spark=SparkSession.builder.appName("test").getOrCreate()
    print(spark)

###################################
#### READ operation in spark #####
###################################
    sch=getSchema()    #user defined custom schema. Better performance than option('inferSchmea',true)
# Scenario 1: to read dataframe from csv file format:
    print("*********** Create a DF using CSV file *************")
    df_csv= spark.read.option("header",True).schema(sch).format("csv").load ("../../data/2004.csv")
    df_csv.select("Year","Month","DayofMonth","DayOfWeek").show(5)
    print("\n total records count ")
    print(df_csv.count())
    df_csv.printSchema()
    df_new=df_csv.withColumn("partitionid", spark_partition_id())
    df_new.select("Year","Month","partitionid").show()
    # df_csv.groupBy("Month","DayOfWeek").agg(avg("DayofMonth")).show(50)
                                                          #to show the datatypes of the columns
    # print(df_csv.columns)                               #to see the columns - gives the output in a list
    print("*********** End of create a DF using CSV file **********\n\n")

# Spark SQL:

    #in-memory table

    # df_csv.createOrReplaceTempView("tab1")
    #
    # dfsql = spark.sql("select Year,Month,DayofMonth from tab1 group by")
    # dfsql.show()





 # Scenario 2: to read a dataframe from text file format:

    # print("*********** Create a DF using txt file *************")
    # df_txt= spark.read.text("file:///C:/Users/Administrator/PycharmProjects/pysparkProject/SparkBasics/data/for_RDD.txt")
    # df_txt.show(2)
    # print("*********** End of create a DF using txt file **********\n\n")

# Scenario 3: to read  a dataframe from json file format:

    # print("*********** Create a DF using JSON file *************")
    # df_json= spark.read.option("multiline",True).json("../../data/for_DF.json")
    # df_json.printSchema()
    # df_json.show()
    # print("*********** End of create a DF using JSON file **********\n\n")

    # df=spark.read.load("../../data/csv_output.csv")
    # df.show(4)


###################################
#### WRITE operation in spark #####
###################################

    # df_wr_csv=df_csv.select("Year","Month","DayofMonth")
    # df_wr_csv1=df_wr_csv
    # print(df_wr_csv1.rdd.getNumPartitions())
    #
    # print("*********** Writing DF into CSV format *************\n\n")
    # df_wr_csv1.write.mode("overwrite").option("header",True).save("../../data/csv_output.csv")


#mode : overwrite / append / ignore / error
    # print("*********** Writing DF into Text format *************\n\n")
    # df_wr_txt=df_wr_csv.select("Description")
    # df_wr_txt.write.mode("overwrite").format("text").save("../../data/txt_output")

def getSchema():
    sch = StructType([
        StructField('Year', StringType(), True),
        StructField('Month', StringType(), True),
        StructField('DayofMonth', IntegerType(), True),
        StructField('DayOfWeek', IntegerType(), True),
        StructField('DepTime', IntegerType(), True),
        StructField('CRSDepTime', IntegerType(), True),
        StructField('ArrTime', IntegerType(), True),
        StructField('CRSArrTime', IntegerType(), True),
        StructField('UniqueCarrier', IntegerType(), True),
        StructField('FlightNum', IntegerType(), True),
        StructField('TailNum', LongType(), True),
        StructField('ActualElapsedTime', IntegerType(), True),
        StructField('CRSElapsedTime', IntegerType(), True),
        StructField('AirTime', IntegerType(), True),
        StructField('ArrDelay', IntegerType(), True),
        StructField('DepDelay', IntegerType(), True),
        StructField('Origin', StringType(), True),
        StructField('Dest', StringType(), True),
        StructField('Distance', IntegerType(), True),
        StructField('TaxiIn', IntegerType(), True),
        StructField('TaxiOut', IntegerType(), True),
        StructField('Cancelled', IntegerType(), True),
        StructField('CancellationCode', IntegerType(), True),
        StructField('Diverted', IntegerType(), True),
        StructField('CarrierDelay', IntegerType(), True),
        StructField('WeatherDelay', IntegerType(), True),
        StructField('NASDelay', IntegerType(), True),
        StructField('SecurityDelay', IntegerType(), True),
        StructField('LateAircraftDelay', IntegerType(), True)
    ])

    return sch



if __name__=='__main__':
    testSpark()