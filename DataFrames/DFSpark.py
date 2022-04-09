from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType,StructField,IntegerType
from pyspark.sql.functions import lit,col

def testSpark():
    print("Spark setup")
    spark=SparkSession.builder.appName("test").getOrCreate()
    print(spark)

    print('''\t###################################
    #### READ operation in spark #####
    ###################################''')

# Scenario 1: to read dataframe from csv file format:
    # Default file format : parquet

    # print("*********** Create a DF using CSV file *************")
    # df_csv= spark.read.option("header",True).option("inferSchema",True).format ("csv").load("../data/2004.csv")
    # df_csv= spark.read.option("header",True).option("inferSchema",True).format ("csv").load("C:/Users/Administrator/PycharmProjects/pysparkProject/SparkBasics/data/2004.csv")
    # df_csv = spark.read.option("header",True).csv("../data/2004.csv")
    # df_csv.show(100)           # to print the first 20 records
    # df_csv.printSchema()    # to show the datatypes of the columns

    # print(df_csv.count())
    # print(df_csv.rdd.getNumPartitions())

    # df_csv_subset=df_csv.select("Year","Month","UniqueCarrier").groupBy("month","Year","UniqueCarrier").count()
    # df_csv_subset.show(5)

    # df_csv_subset.withColumn("Flight data" as flight).show(5)
    # df_csv_subset.withColumnRenamed("UniqueCarrier","Airline code")


    # df_csv.createOrReplaceTempView("flightdata")
    #
    # spark.sql("select month,year, 'US' as country from flightdata").show(10)

    # spark.sql("select month, count(*) as div_count from flightdata where diverted !=0  group by month having month in (1,3,5) order by div_count").show()

    # spark.sql("select month, UniqueCarrier, count(*) as ct from flightdata where diverted !=0  group by month, UniqueCarrier order by ct desc").show()
    #
    # print("\n\n\n ****************output of the Union**************** \n\n\n ")
    # spark.sql("""select dayofweek, count(arrdelay) from flightdata where arrdelay !='0' group by dayofweek""").show()

    # df_csv.select('ID','dept').show()
    # print("*********** End of create a DF using CSV file **********\n\n")

 # Scenario 2: to read a dataframe from text file format:

    # print("*********** Create a DF using txt file *************")
    # df_txt= spark.read.text("../data/for_DF.txt")
    # df_txt.show(5,truncate=False)
    # print("*********** End of create a DF using txt file **********\n\n")

# Scenario 3: to read  a dataframe from json file format:

    print("*********** Create a DF using JSON file *************")
    df_json= spark.read.option("multiline","true").format("json").load("../data/for_DF.json")
    df_json.show()
    # df_new=df_json.withColumn("State1",col('Zipcode')*2)        # to add a new column to dataframe
    # df_new.show()
    # df_new1=df_new.withColumnRenamed("State1","Country")
    # df_new1.show()
    df_json.createOrReplaceTempView("jsontab")                    # to create a table from dataframe
    spark.sql("select city, state from jsontab").show()

    print("*********** End of create a DF using JSON file **********\n\n")

#Read JSON file from multiline
#Sometimes you may want to read records from JSON file that scattered multiple lines,
# In order to read such files, use-value true to multiline option, by default multiline option, is set to false.


    # print('''\t###################################
    # #### WRITE operation in spark #####
    # ###################################''')

    # df_wr_csv=df_csv.select("id", "dept")
    # df_wr_csv.show(5)

    # print("*********** Writing DF into CSV format *************\n\n")
    # df_csv_col=df_csv.coalesce(1)
    # df_csv_col.write.mode("overwrite").option("header",True).csv("df_csv_nw_col")

    # print("*********** Writing DF into Text format *************\n\n")
    # df_wr_txt=df_csv.select("name")
    # df_wr_txt.write.mode("overwrite").format("text").save("df_txt_save")


if __name__=='__main__':
    testSpark()



# What is JSON?
# JSON stands for JavaScript Object Notation
# JSON is a lightweight data-interchange format
# JSON is plain text written in JavaScript object notation
# JSON is used to send data between computers
# JSON is language independent
# Code for reading and generating JSON exists in many programming languages