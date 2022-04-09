# import module
import contextvars

# declaring the variable
# to it's default value
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext("local", "SparkContextBuilder-Application")
spark = SparkSession.builder.appName("SparkSessionBuilder-Application").getOrCreate()

print(spark)
print(type(spark))
cvar = contextvars.ContextVar(str(spark),default = None)

print("value of context variable cvar: ",cvar.get())

