import json

from pyspark import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.types import FloatType, IntegerType, StructType

sc = SparkContext()
# sc = SparkContext("local", "first app")
sqlContext = SQLContext(sc)

workDir = "hdfs://172.29.0.2:9000/test/"

# def function(x):
#      print('-----------------------------------'+str(x))
#
# sc.textFile("hdfs://172.29.0.5:9000/test/test.txt").foreach(function)
# df = sqlContext.createDataFrame([1.0, 2.0, 3.0], FloatType()).show()


schema = StructType.fromJson(json.loads(
    sqlContext.read.text(workDir + '/schemes/test_runs_schema.json', wholetext=True).first()[0]))

runsDF = sqlContext.read.format('csv') \
    .schema(schema).options(header='true') \
    .load(workDir + 'test_runs.csv.gz').show()
