from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

class SparkSessionBuilder(object):
    _instance=None

    sc = SparkContext("local", "SparkContextBuilder-Application")
    spark = SparkSession.builder.appName("SparkSessionBuilder-Application").getOrCreate()

    def __new__(self):
        if not self._instance:
            self._instance=super(SparkSessionBuilder,self).__new__(self)

        return self._instance


# x = SparkSessionBuilder()
# print(x.sc)
# x.y=20
# print(x.spark)
# z=SparkSessionBuilder()
# print(z.sc)
#
# print(id(x))
# print(id(z))
