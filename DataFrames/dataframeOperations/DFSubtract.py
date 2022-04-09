'''
Return a new DataFrame containing rows in First DataFrame but not in Second DataFrame.
'''

from SparkBasics.DataFrames.DataFrame_create.sparkSessionBuilder import SparkSessionBuilder

class DFSubtract():

    def __init__(self):
        # pass
        self.spark = SparkSessionBuilder().spark
        self.sc = SparkSessionBuilder().sc
        # self.sc.setLogLevel("INFO")
        print("\nfrom DF Intersect class...")
        print("Spark Session Object ID : ", self.spark)
        print("Spark Application ID : ", self.spark.sparkContext.applicationId)

    def createDataFrames(self):
        data1 = [('Watermelon','Green','Round'), ('Papaya','Yellow','Oval'), ('Mango','Yellow','Oval'), ('Watermelon', 'Green', 'Round')]
        data2 = [('Watermelon', 'Green', 'Round'), ('Papaya', 'Yellow', 'Oval'),('Plums', 'Red', 'Round'), ('Watermelon', 'Green', 'Round')]

        df1 = self.spark.createDataFrame(data1).toDF('name','color','shape')
        df2 = self.spark.createDataFrame(data2).toDF('name', 'color', 'shape')

        df1.show()
        df2.show()
        return df1, df2


    def dfSubtract(self, df1, df2):
        ########## Subtract two dataframe in pyspark
        print(" \n\n Subtract 1-2 Operations : \n\n ")
        df_Subtract1 = df1.subtract(df2)
        df_Subtract1.show()
        print(" \n\n Subtract 2-1 Operations : \n\n ")
        df_Subtract2 = df2.subtract(df1)
        df_Subtract2.show()


        # print(" \n\n Union Operations : \n\n ")
        # df_inter = df1.union(df2).groupBy("name").count()
        # df_inter.show()

        # df_join=df1.join(df_inter, "name").filter("count == 1")
        # df_join.show()


obj = DFSubtract()
x, y = obj.createDataFrames()
obj.dfSubtract(x,y)