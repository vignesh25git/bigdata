from pyspark.sql import SparkSession

def getSparkSession(process,appname,jdbc_lib,hive_flag):
    if hive_flag == 'Y':
         spark = SparkSession.builder.master(process).appName(appname)\
             .config("spark.jars", jdbc_lib)\
             .enableHiveSupport().getOrCreate()
    else:
        spark = SparkSession.builder.master(process).appName(appname)\
            .config("spark.jars", jdbc_lib)\
            .getOrCreate()

    return spark

def removeHeaderInRdd(rdd):
    header = rdd.first()
    finalrdd = rdd.filter(lambda x:x!=header)
    return finalrdd

def removeTrailer(rdd):
    total_records=rdd.count()-1
    rdd1 = rdd.zipWithIndex()
    rdd2 = rdd1.filter(lambda x:x[1]!=total_records)
    return rdd2

