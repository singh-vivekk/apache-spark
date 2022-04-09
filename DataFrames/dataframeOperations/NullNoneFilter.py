''' This module will get the DF created by CreateDF Class
    PySpark SQL DataFrame we often need to filter rows with NULL/None values
                on columns, you can do this by checking IS NULL or IS NOT NULL conditions.'''

from SparkBasics.DataFrames.DataFrame_create.sparkSessionBuilder import SparkSessionBuilder
from SparkBasics.DataFrames.DataFrame_create.createDataFrame import CreateDF
from pyspark.sql.functions import col

class NullNoneFilter():

    def __init__(self):
        # pass
        self.spark = SparkSessionBuilder().spark
        self.sc = SparkSessionBuilder().sc
        print("\nfrom NullNoneFilter class...")
        print("Spark Session Object ID : ", self.spark)
        print("Spark Application ID : ", self.spark.sparkContext.applicationId)


    def nullDf(self):
        data = [
            ("James", None, "M"),
            ("Anna", "NY", "F"),
            ("Julia", None, None)
            ]

        columns = ["name", "state", "gender"]
        df = self.spark.createDataFrame(data, columns)
        df.show()
        return df

    #Filter Rows with NULL Values in DataFrame
    def showNullResult(self,df):
        df.filter("state is NULL").show()
        df.filter(df.state.isNull()).show()
        df.filter(col("state").isNull()).show()

    #Filter Rows --> NULL on Multiple Columns
    def showMulColNullResult(self, df):
        df.filter("state IS NULL AND gender IS NULL").show()
        df.filter(df.state.isNull() & df.gender.isNull()).show()

    #Filter Rows with IS NOT NULL or isNotNull
    def showIsNotNULLResult(self,df):
        df.filter("state IS NOT NULL").show()
        df.filter("NOT state IS NULL").show()
        df.filter(df.state.isNotNull()).show()
        df.filter(col("state").isNotNull()).show()
        df.na.drop(subset=["state"]).show()


objSelectDF = NullNoneFilter()
df=objSelectDF.nullDf()

# objSelectDF.showNullResult(df)
# objSelectDF.showMulColNullResult(df)
objSelectDF.showIsNotNULLResult(df)






'''Notes: 
NULL on columns needs to handles before you performing any operations on columns
    as operations on NULL values results in unexpected values.
    
We need to graciously handle null values as the first step before processing. 
Also, While writing DataFrame to the files, it’s a good practice to store files without NULL values 
either by dropping Rows with NULL values on DataFrame or By Replacing NULL values with empty string.    
'''