''' This module will get the DF created by CreateDF Class '''
import time
start_time = time.time()
print(" Execution Start time is : ", time.ctime(start_time))

from SparkBasics.DataFrames.DataFrame_create.sparkSessionBuilder import SparkSessionBuilder
from SparkBasics.DataFrames.DataFrame_create.createDataFrame import CreateDF

class FilterDF():

    def __init__(self):
        # pass
        self.spark = SparkSessionBuilder().spark
        self.sc = SparkSessionBuilder().sc
        # self.sc.setLogLevel("INFO")
        print("\nfrom FilterDF class...")
        print("Spark Session Object ID : ", self.spark)
        print("Spark Application ID : ", self.spark.sparkContext.applicationId)


    def filterDF(self,df):
        print(" -------- Filtered data ---------- ")
        # Using equals condition : DataFrame filter() with Column Condition
        df.filter(df.state == "OH").show(truncate=False)

        # Using SQL col() function
        from pyspark.sql.functions import col
        df.filter(col("state") == "OH").show(truncate=False)

        # Using SQL Expression
        df.filter("gender == 'M'").show()
        # For not equal
        # df.filter("gender != 'M'").show()
        # df.filter("gender <> 'M'").show()

        # PySpark Filter with Multiple Conditions
        df.filter((df.state == "OH") & (df.gender == "M")).show(truncate=False)

        # Filter Based on List Values: isin function
        li = ["OH", "CA", "DE"]
        df.filter(df.state.isin(li)).show()

        # Filter NOT IS IN List values
        # These show all records with NY (NY is not part of the list)
        df.filter(~df.state.isin(li)).show()
        df.filter(df.state.isin(li) == False).show()

        # Filter Based on Starts With, Ends With, Contains
        # Using startswith
        df.filter(df.state.startswith("C")).show()

        # using endswith
        df.filter(df.state.endswith("H")).show()

        # using contains
        df.filter(df.state.contains("H")).show()

        # Filter on an Array column
        from pyspark.sql.functions import array_contains
        df.filter(array_contains(df.languages, "Java")) \
                                   .show(truncate=False)

        # Filtering on Nested Struct columns :
        # using . operator within column name
        df.filter(df.name.lastname == "Williams") \
                                .show(truncate=False)



    def filterLikeRlikeDF(self, df):
        # PySpark Filter like and rlike
        # like - SQL LIKE pattern :  can be applied on String type cols
        print(" -------- Filter data using Like and Rlike---------- ")
        df.filter(df.name.like("%rose%")).show()

        # rlike - SQL RLIKE pattern (LIKE with Regex)
        # This check (?i) case insensitive to first character
        df.filter(df.name.rlike("(?i)^*rose$")).show()




################### Data Preparation #########################
                #  Method calls
objCreateDF = CreateDF()
                  # df for filtering data
df=objCreateDF.createDFwithSchema()
# df.cache().count()
                # df for working with Like and Rlike
df2=objCreateDF.dfForLike()

################### Execution #########################

objFilterDF = FilterDF()
# objFilterDF.filterDF(df)
objFilterDF.filterLikeRlikeDF(df2)

print(" Execution End time is : ", time.ctime(time.time()))
print("--- %s Total Execution seconds ---" % (time.time() - start_time))

################### Completed #########################


'''
Notes:
------
PySpark filter() function is used to filter the rows from RDD/DataFrame based on the given condition or SQL expression, 
you can also use where() clause instead of the filter() if you are coming from an SQL background, 
both these functions operate exactly the same.

PySpark DataFrame filter() Syntax:
    filter(condition)

Ref: https://sparkbyexamples.com/pyspark/pyspark-where-filter/
'''