import itertools
import time

from pyspark import SparkContext
from pyspark.rdd import RDD
from pyspark.sql import SQLContext, SparkSession
import json

from pyspark.sql.context import HiveContext
from pyspark.sql.functions import array, lit, when, col, count
from pyspark.sql.types import StructType, StructField, StringType

sc = SparkContext("local", "first app")
sqlContext = SQLContext(sc)
sparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()

with open('../test_data/schemas/test_runs_schema.json') as data_file:
    schema = StructType.fromJson(json.load(data_file))
runsDF = sqlContext.read.format('csv') \
    .schema(schema).options(header='true') \
    .load('/home/sergey/PycharmProjects/untitled/test_data/test_runs.csv').cache()

with open('../test_data/schemas/test_description_schema.json') as data_file:
    new_schema = StructType.fromJson(json.load(data_file))
descDF = sqlContext.read \
    .schema(new_schema) \
    .json('../test_data/test_description.json', multiLine=True).cache()


class TestSpark:
    # logFile = "hdfs:///user/input/test.txt"

    def last_test_Report(self):
        runsDF.sort(runsDF.TestTimestamp.desc()).show(n=5)
        maxTestTimestamp = runsDF.agg({"TestTimestamp": "max"}).collect()[0]["max(TestTimestamp)"]
        print("Last test report:")
        runsDF.filter(runsDF.TestTimestamp == maxTestTimestamp) \
            .join(descDF, descDF.TestId == runsDF.TestId, how='left') \
            .select("TestRunId", "TestTimestamp", "TestName", "TestResult").show()

    def all_critical_tests(self):
        def fails_in_the_row(tup: tuple):
            count = 0
            max = 0
            i = 0
            while i < len(tup):
                if (tup[i + 1] == 'FAILURE'):
                    count += 1
                if (tup[i + 1] == 'SUCCESS'):
                    if (count > max):
                        max = count
                    count = 0
                i += 2
            return max

        numDF = runsDF.rdd \
            .filter(lambda row: row['IsCritical'] is True) \
            .map(lambda row: (row['TestId'], [row['TestRunId'], row['TestResult']])) \
            .reduceByKey(lambda acc, val: acc + val) \
            .map(lambda x: [x[0], fails_in_the_row(x[1])]) \
            .filter(lambda x: x[1] > 2) \
            .toDF().withColumnRenamed("_1", "TestId").withColumnRenamed("_2", "NumberOfConsequentFails")
        print("All critical tests that fail more than 2 times in a row:")
        numDF.join(runsDF, runsDF.TestId == numDF.TestId, how='left') \
            .join(descDF, descDF.TestId == runsDF.TestId, how='left') \
            .select(runsDF.TestId, "TestName", "NumberOfConsequentFails") \
            .drop_duplicates() \
            .sort(runsDF.TestId) \
            .show(n=1000)

    def lastReportSummary(self):
        print("Last report summary:")
        runsDF.rdd \
            .map(lambda row: (row['TestId'], row)) \
            .combineByKey(createCombiner=lambda val: [val],
                          mergeValue=lambda c, val: c + [val],
                          mergeCombiners=lambda u, u1: u + u1) \
            .mapValues(lambda val: {
            'TestRunDuration': min(val, key=lambda val: val[1])[1].__str__() + "---" + max(val, key=lambda val: val[1])[
                1].__str__(),
            'TotalNumberOfTests': len(val),
            'NumberOfFailures': len([item for item in val if item[3] == 'FAILURE']),
            'NumberOfCriticalTests': len([item for item in val if item[4] is True]),
            'NumberOfCriticalFailures': len([item for item in val if (item[3] == 'FAILURE' and item[4] is True)])}) \
            .sortByKey().foreach(print)

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

    def saveFullJoin(self):
        df = runsDF.join(descDF, on='TestId', how='full')
        df.write.parquet("./output.parquet", partitionBy='TestRunId', mode='overwrite')
        df.write.mode("overwrite").saveAsTable("default.tableName")

    def queryFullJoin(self):
        sparkSession.read.option("basePath", "./output.parquet/").parquet("./output.parquet").show()
        sparkSession.sql('show tables').show()
        sparkSession.sql("select * from default.tableName").show()
