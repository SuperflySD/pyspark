import itertools
import time
from pyspark import SparkContext
from pyspark.rdd import RDD
from pyspark.sql import SQLContext, SparkSession
import json
from pyspark.sql.context import HiveContext
from pyspark.sql.functions import array, lit, when, col, count
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window

sc = SparkContext()
sqlContext = SQLContext(sc)
sparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()

# workDir = "hdfs://172.29.0.2:9000/test/"
workDir = "/home/sergey/PycharmProjects/untitled/test_data/"

schema = StructType.fromJson(json.loads(
    sqlContext.read.text(workDir + '/schemes/test_runs_schema.json', wholetext=True).first()[0]))
runsDF = sqlContext.read.format('csv') \
    .schema(schema).options(header='true') \
    .load(workDir + 'test_runs.csv').cache()

new_schema = StructType.fromJson(json.loads(
    sqlContext.read.text(workDir + '/schemes/test_description_schema.json', wholetext=True).first()[0]))
descDF = sqlContext.read \
    .schema(new_schema) \
    .json(workDir + 'test_description.json', multiLine=True).cache()


def last_test_Report():
    # runsDF.sort(runsDF.TestTimestamp.desc()).show(n=5)
    maxTestTimestamp = runsDF.agg({"TestTimestamp": "max"}).collect()[0]["max(TestTimestamp)"]
    print("Last test report:")
    runsDF.filter(runsDF.TestTimestamp == maxTestTimestamp) \
        .join(descDF, descDF.TestId == runsDF.TestId, how='left') \
        .select("TestRunId", "TestTimestamp", "TestName", "TestResult").show()


def all_critical_tests_dataframe():
    runsDF.createOrReplaceTempView("runsTable")
    descDF.createOrReplaceTempView("descTable")
    sqlContext.sql(
        "select TestId, max(failures) maxFails from (select t.TestId, count(t.diff2) failures from "
        "(select TestRunId, TestId, TestResult, "
        "row_number() over (partition by TestId, TestResult  order by TestResult)  - "
        "row_number() over (partition by TestId order by TestRunId)  as diff2 from runsTable  order by TestId,TestRunId) t"
        " where t.TestResult='FAILURE' group by t.diff2, t.TestId having count(t.diff2) >1 order by t.TestId) t1 group by TestId") \
        .drop_duplicates().createOrReplaceTempView("failuresTable")
    df1= sqlContext.sql("Select * from failuresTable where maxFails>2").cache()

    df2= runsDF\
        .filter(~((col('TestResult')=='FAILURE') & (col('IsCritical')=='false'))) \
        .select('TestId', 'TestResult',
                  (F.row_number().over(Window.partitionBy("TestId", "TestResult").orderBy("TestResult")) -
                   F.row_number().over(Window.partitionBy("TestId").orderBy("TestRunId"))).alias("diff2"))\
        .alias("t") \
        .filter(col("t.TestResult") == 'FAILURE') \
        .groupBy("t.diff2", "t.TestId").agg(count("t.diff2").alias('failures')) \
        .alias("t1") \
        .groupBy('t1.TestId').agg(F.max('t1.failures').alias("maxFailsInTheRow")).where(col('maxFailsInTheRow') > 2).orderBy('TestId')\
        .cache()

    #df1.show()
    df2.show()
    #df1.exceptAll(df2).show()
    #df2.exceptAll(df1).show()




def lastReportSummary():
    print("Last report summary:")
    # runsDF.rdd \
    #     .map(lambda row: (row['TestId'], row)) \
    #     .combineByKey(createCombiner=lambda val: [val],
    #                   mergeValue=lambda c, val: c + [val],
    #                   mergeCombiners=lambda u, u1: u + u1) \
    #     .mapValues(lambda val: {
    #     'TestRunDuration': min(val, key=lambda val: val[1])[1].__str__() + "---" + max(val, key=lambda val: val[1])[
    #         1].__str__(),
    #     'TotalNumberOfTests': len(val),
    #     'NumberOfFailures': len([item for item in val if item[3] == 'FAILURE']),
    #     'NumberOfCriticalTests': len([item for item in val if item[4] is True]),
    #     'NumberOfCriticalFailures': len([item for item in val if (item[3] == 'FAILURE' and item[4] is True)])}) \
    #     .sortByKey().foreach(print)

    # just another way
    runsDF.rdd \
        .map(lambda row: (row['TestId'], {
        'TestId': row['TestId'],
        'MaxDuration': row['TestTimestamp'],
        'MinDuration': row['TestTimestamp'],
        'TotalNumberOfTests': 1,
        'NumberOfFailures': 1 if row['TestResult'] == 'FAILURE' else 0,
        'NumberOfCriticalTests': 1 if row['IsCritical'] is True else 0,
        'NumberOfCriticalFailures': 1 if row['IsCritical'] is True and row['TestResult'] == 'FAILURE' else 0})) \
        .combineByKey(createCombiner=lambda val: val,
                      mergeValue=lambda c, val: {
                          'TestId': val['TestId'],
                          'MinDuration': val['MinDuration'] if c['MinDuration'] > val['MinDuration'] else c[
                              'MinDuration'],
                          'MaxDuration': val['MaxDuration'] if c['MaxDuration'] < val['MaxDuration'] else c[
                              'MaxDuration'],
                          'TotalNumberOfTests': c['TotalNumberOfTests'] + val['TotalNumberOfTests'],
                          'NumberOfFailures': c['NumberOfFailures'] + val['NumberOfFailures'],
                          'NumberOfCriticalTests': c['NumberOfCriticalTests'] + val['NumberOfCriticalTests'],
                          'NumberOfCriticalFailures': c['NumberOfCriticalFailures'] + val[
                              'NumberOfCriticalFailures']},
                      mergeCombiners=lambda u, u1: u + u1) \
        .sortByKey().map(lambda x: (x[1])).toDF().show()


def saveFullJoin():
    df = runsDF.join(descDF, on='TestId', how='full')
    df.write.parquet("./output.parquet", partitionBy='TestRunId', mode='overwrite')
    df.write.saveAsTable("default.tableName", partitionBy='TestRunId', mode='overwrite')


def queryFullJoin():
    sparkSession.read.option("basePath", "./output.parquet/").parquet("./output.parquet").show()
    sparkSession.sql('show tables').show()
    sparkSession.sql("select * from default.tableName").show()


def main():
    # last_test_Report()
    all_critical_tests_dataframe()
    # lastReportSummary()
    # saveFullJoin()
    # queryFullJoin()


if __name__ == '__main__':
    main()
