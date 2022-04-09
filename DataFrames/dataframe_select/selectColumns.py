''' This module will get the DF created by CreateDF Class '''
# import time
from SparkBasics.DataFrames.DataFrame_create.sparkSessionBuilder import SparkSessionBuilder
from SparkBasics.DataFrames.DataFrame_create.createDataFrame import CreateDF

class SelectCol():

    def __init__(self):
        # pass
        self.spark = SparkSessionBuilder().spark
        self.sc = SparkSessionBuilder().sc
        # self.sc.setLogLevel("INFO")
        print("\nfrom SelectCol class...")
        print("Spark Session Object ID : ", self.spark)
        print("Spark Application ID : ", self.spark.sparkContext.applicationId)


    def dfSchema(self,df):
        baseDF=df
        print(" ----- From dfSchema method ------- ")
        #To get the columns and its datatype
        baseDF.printSchema()

        #To get the columns list only
        print(baseDF.columns)


    def selectCols(self,df):
        '''
        Syntax: dataframe_name.select( columns_names )
        :param df:
        :return:
        '''
        baseDF=df
        print(" ------- From selectCols method of SelectCol class ---------")
        baseDF.show()

        from pyspark.sql.functions import lit
        #to add new columns to dataframe:
        baseDF = baseDF.withColumn("State", lit("MH")).withColumn("Country", lit("India"))
        baseDF.show()

        newDF = baseDF.withColumnRenamed("Country","CNTRY")

        newDF.show()
        #Way 1: as simple column names
        print("Way 1: as simple column names")
        baseDF.select("name", "city").show()

        #Way 2: as list items
        baseDF.select(baseDF["age"],baseDF["city"]).show()

        #Way 3: as a struct element
        baseDF.select(baseDF.city, baseDF.name).show()

        #Way 4: as a list of cols
        select_columns = ["age", "name"]
        baseDF.select(select_columns).show()

        #Way 5: By using col functions:
        from pyspark.sql.functions import col
        baseDF.select(col("name"),col("city")).show()

        baseDF.select(col("name").alias("emp_name"),\
                      col("city").alias("address")).show()

        # Select All columns
        baseDF.select([col for col in baseDF.columns[::2]]).show()
        # time.sleep(100)

        # Select All columns from List
        # baseDF.select(baseDF.columns).show()


#  Method calls
objCreateDF = CreateDF()
df=objCreateDF.createTupleDF(1)

objSelectCol = SelectCol()
# objSelectCol.dfSchema(df)
objSelectCol.selectCols(df)

print("*************** Col selection completed ***************")

# obj = SelectCol()
# x=obj.selectCols          #change the method name here

                            #to get the help messages:

# if __name__ == '__main__':
#     print("*" * 50)
#     print("Using __doc__:")
#     print(x.__doc__)
#     print("*"*50)
#     print("Using help:")
#     help(x)