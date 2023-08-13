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

def writeToFile(df,location,filetype="csv",delim=",",mode="overwrite",headerflag=True):
    if filetype=='csv':
     df.write.mode(mode).csv(location, header=headerflag, sep=delim)
     print("csv saved")
    elif filetype=='json':
     df.write.mode(mode).option("multiline", "true").json(location)
     print("json saved")
    elif filetype=='parquet':
     df.write.parquet(location,mode=mode)
     print("parquet saved")


def writeHiveTable(df, tblname, mode="overwrite"):
    df.write.mode(mode).saveAsTable(tblname)
    print("hive table saved")

def writeToSqlTable(df,url,table,mode,driver):
    print("Starting to save to MYSQL ")
    df.write.jdbc(url=url,table=table,mode=mode,properties={"driver":driver})
    print("completed - save to MYSQL ")






